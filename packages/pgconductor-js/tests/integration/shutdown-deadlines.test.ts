import postgres from "postgres";
import { afterAll, afterEach, beforeAll, expect, test, vi } from "vitest";
import { Conductor } from "../../src/conductor";
import { DatabaseClient } from "../../src/database-client";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition, within } from "../test-utils";

let pool: TestDatabasePool;
const databases: TestDatabase[] = [];

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterEach(async () => {
	vi.restoreAllMocks();
	await Promise.all(databases.map((db) => db.destroy()));
	databases.length = 0;
});

afterAll(async () => {
	await pool?.destroy();
});

async function child(): Promise<TestDatabase> {
	const db = await pool.child();
	databases.push(db);
	return db;
}

// Holds a row lock in a transaction of its own until released, or until cleanup removes the
// orchestrator, so the statements under test wait on it
async function lockRow(db: TestDatabase, table: string, id: string) {
	const locker = postgres(db.url, { max: 1 });
	const release = new Deferred<void>();
	const locked = new Deferred<void>();
	const holding = locker.begin(async (tx) => {
		await tx.unsafe(`select 1 from pgconductor.${table} where id = $1::uuid for no key update`, [
			id,
		]);
		locked.resolve();
		await release.promise;
	});
	await locked.promise;
	const shutdown = DatabaseClient.prototype.orchestratorShutdown;
	vi.spyOn(DatabaseClient.prototype, "orchestratorShutdown").mockImplementation(
		async function (this: DatabaseClient, args, opts) {
			release.resolve();
			return shutdown.call(this, args, opts);
		},
	);
	return async () => {
		release.resolve();
		await holding;
		await locker.end();
	};
}

test("a heartbeat waiting on a lock is cancelled after the stale age and by stop()", async () => {
	const db = await child();
	const definition = defineTask({ name: "idle" });
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([definition]),
		context: {},
	});
	await conductor.ensureInstalled();
	// An orchestrator without workers stops at once
	const task = conductor.createTask(definition, { invocable: true }, async () => {});

	let heartbeats = 0;
	const heartbeat = DatabaseClient.prototype.orchestratorHeartbeat;
	vi.spyOn(DatabaseClient.prototype, "orchestratorHeartbeat").mockImplementation(
		async function (this: DatabaseClient, args, opts) {
			heartbeats++;
			return heartbeat.call(this, args, opts);
		},
	);

	const orch = Orchestrator.create({
		conductor,
		tasks: [task],
		heartbeatIntervalMs: 50,
		staleAfterMs: 200,
	});
	await orch.start();
	const unlock = await lockRow(db, "_private_orchestrators", orch.info.id);
	try {
		// Each heartbeat waits on the lock until the stale age passed, then the next one runs
		const before = heartbeats;
		await waitForCondition(() => heartbeats >= before + 3, 5000);
		// The lock is released only once cleanup reaches the shutdown, after the heartbeats stopped
		expect(await within(orch.stop(), 5000)).toBe("done");
	} finally {
		await unlock();
		await orch.stop();
	}
});

test("a final flush waiting on a lock does not hold up stop()", async () => {
	const db = await child();
	const definition = defineTask({ name: "locked-result" });
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([definition]),
		context: {},
	});
	await conductor.ensureInstalled();
	const started = new Deferred<void>();
	const finish = new Deferred<void>();
	const finished = new Deferred<void>();
	const task = conductor.createTask(definition, { invocable: true }, async () => {
		started.resolve();
		await finish.promise;
		finished.resolve();
	});
	const orch = Orchestrator.create({
		conductor,
		tasks: [task],
		// The result waits in the buffer for the final flush
		defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 60_000, flushBatchSize: 100 },
	});
	await orch.start();
	const id = await conductor.invoke(definition, {});
	await started.promise;
	const unlock = await lockRow(db, "_private_executions", id);
	try {
		finish.resolve();
		await finished.promise;
		// The final flush is cancelled after 10 seconds, then cleanup releases the lock
		expect(await within(orch.stop(), 15_000)).toBe("done");
	} finally {
		await unlock();
		await orch.stop();
	}
	// Whether the cancelled settlement or the release by cleanup won, nothing holds the execution
	const [row] = await db.sql<{ locked: boolean }[]>`
		select locked_by is not null as locked from pgconductor._private_executions
		where id = ${id}::uuid
	`;
	expect(row?.locked).toBe(false);
});
