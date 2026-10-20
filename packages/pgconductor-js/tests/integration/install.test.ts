import postgres, { type Sql } from "postgres";
import { afterAll, afterEach, beforeAll, expect, test, vi } from "vitest";
import { Conductor } from "../../src/conductor";
import { DatabaseClient } from "../../src/database-client";
import { Deferred } from "../../src/lib/deferred";
import { DefaultLogger } from "../../src/lib/logger";
import { waitFor } from "../../src/lib/wait-for";
import { MigrationStore } from "../../src/migration-store";
import { Orchestrator } from "../../src/orchestrator";
import { SchemaManager } from "../../src/schema-manager";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";

const latest = new MigrationStore().getLatestMigrationNumber();

let pool: TestDatabasePool;
const databases: TestDatabase[] = [];
const connections: Sql[] = [];

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterEach(async () => {
	vi.restoreAllMocks();
	await Promise.all(connections.map((sql) => sql.end({ timeout: 1 })));
	connections.length = 0;
	await Promise.all(databases.map((db) => db.destroy()));
	databases.length = 0;
});

afterAll(async () => {
	await pool?.destroy();
});

async function child() {
	const db = await pool.child();
	databases.push(db);
	return db;
}

function connect(db: TestDatabase) {
	const sql = postgres(db.url, { max: 1, onnotice: () => {} });
	connections.push(sql);
	return sql;
}

function client(sql: Sql) {
	return new DatabaseClient({ sql, logger: new DefaultLogger() });
}

async function until(predicate: () => Promise<boolean>) {
	while (!(await predicate())) {
		await waitFor(20);
	}
}

function breakingMigration(sql: string) {
	const getMigration = MigrationStore.prototype.getMigration;
	vi.spyOn(MigrationStore.prototype, "getMigration").mockImplementation(function (
		this: MigrationStore,
		version: number,
	) {
		if (version === latest + 1) {
			return { version, name: "breaking", sql, breaking: true };
		}
		return getMigration.call(this, version);
	});
}

test("ensureInstalled completes a database that has only migration 0", async () => {
	const db = await child();
	const conductor = Conductor.create({ sql: connect(db), context: {} });
	const bootstrap = new MigrationStore().getMigration(0);
	if (!bootstrap) throw new Error("missing bootstrap migration");
	await conductor.db.applyMigration(bootstrap);
	expect(await conductor.db.getInstalledMigrationNumber()).toBe(0);

	await conductor.ensureInstalled();

	expect(await conductor.db.getInstalledMigrationNumber()).toBe(latest);
});

test("concurrent fresh installs both succeed", async () => {
	const db = await child();
	const a = Conductor.create({ sql: connect(db), context: {} });
	const b = Conductor.create({ sql: connect(db), context: {} });

	// Both read "not installed"; B continues once A has finished
	const bothRead = new Deferred<void>();
	const aDone = new Deferred<void>();
	let reads = 0;
	for (const [conductor, gate] of [
		[a, null],
		[b, aDone],
	] as const) {
		const original = conductor.db.getInstalledMigrationNumber.bind(conductor.db);
		vi.spyOn(conductor.db, "getInstalledMigrationNumber").mockImplementationOnce(async (opts) => {
			const version = await original(opts);
			reads += 1;
			if (reads === 2) bothRead.resolve();
			await bothRead.promise;
			if (gate) await gate.promise;
			return version;
		});
	}

	const installA = a.ensureInstalled().finally(() => aDone.resolve());
	await Promise.all([installA, b.ensureInstalled()]);

	expect(await a.db.getInstalledMigrationNumber()).toBe(latest);
});

test("ensureInstalled survives a lost bootstrap commit response", async () => {
	const db = await child();
	const sql = connect(db);
	let begins = 0;
	// The first transaction commits, then fails like a dropped connection
	const lossy = new Proxy(sql, {
		get(target, prop) {
			if (prop !== "begin") return Reflect.get(target, prop);
			return async (...args: Parameters<Sql["begin"]>) => {
				const result = await target.begin(...args);
				begins += 1;
				if (begins === 1) {
					throw Object.assign(new Error("injected ECONNRESET"), { code: "ECONNRESET" });
				}
				return result;
			};
		},
	});
	const conductor = Conductor.create({ sql: lossy, context: {} });

	await conductor.ensureInstalled();

	expect(begins).toBeGreaterThan(1);
	expect(await conductor.db.getInstalledMigrationNumber()).toBe(latest);
});

test("ensureInstalled rejects a schema newer than the code", async () => {
	const db = await child();
	const conductor = Conductor.create({ sql: connect(db), context: {} });
	await conductor.ensureInstalled();
	await db.sql`
		insert into pgconductor.schema_migrations (version, name)
		values (${latest + 1}, 'future')
	`;

	await expect(conductor.ensureInstalled()).rejects.toThrow(
		"The database schema is newer than this version of Postgres Conductor",
	);
});

test("breaking migration waits for an orchestrator that registers after the others exited", async () => {
	const db = await child();
	const migrator = client(connect(db));
	const registrar = client(connect(db));
	await new SchemaManager(migrator).ensureLatest(new AbortController().signal);
	breakingMigration("select 1");

	// An older orchestrator registers between the exit check and the migration
	const lateId = crypto.randomUUID();
	const countActiveOrchestratorsBelow = migrator.countActiveOrchestratorsBelow.bind(migrator);
	vi.spyOn(migrator, "countActiveOrchestratorsBelow").mockImplementationOnce(async (args, opts) => {
		const remaining = await countActiveOrchestratorsBelow(args, opts);
		await registrar.registerOrchestrator({
			orchestratorId: lateId,
			version: "old",
			migrationNumber: latest,
		});
		return remaining;
	});

	const migrating = new SchemaManager(migrator).ensureLatest(new AbortController().signal);
	let settled = false;
	migrating.finally(() => (settled = true)).catch(() => {});

	await until(
		async () =>
			settled ||
			(
				await db.sql`
					select 1 from pgconductor._private_orchestrators
					where id = ${lateId}::uuid and shutdown_reason is not null
				`
			).length > 0,
	);
	expect(await registrar.getInstalledMigrationNumber()).toBe(latest);

	const heartbeat = await registrar.orchestratorHeartbeat({ orchestratorId: lateId });
	expect(heartbeat?.shutdown_reason).toBe("breaking_migration");
	await registrar.orchestratorShutdown({ orchestratorId: lateId });

	expect((await migrating).migrated).toBe(true);
	expect(await registrar.getInstalledMigrationNumber()).toBe(latest + 1);
}, 30000);

test("an older orchestrator that registers during a breaking migration does not start", async () => {
	const db = await child();
	const migrator = client(connect(db));
	const blocker = connect(db);
	const conductor = Conductor.create({ sql: connect(db), context: {} });
	await conductor.ensureInstalled();

	// The migration holds its locks until the test releases the advisory lock
	await blocker`select pg_advisory_lock(4242)`;
	const migrating = migrator.applyMigration(
		{
			version: latest + 1,
			name: "breaking",
			sql: "select pg_advisory_xact_lock(4242)",
			breaking: true,
		},
		{ fence: true },
	);
	const waitingOnLock = async (count: number) =>
		(
			await db.sql`
				select 1 from pg_stat_activity
				where datname = current_database() and wait_event_type = 'Lock'
			`
		).length === count;
	await until(() => waitingOnLock(1));

	const orchestrator = Orchestrator.create({ conductor });
	const starting = orchestrator.start();
	starting.catch(() => {});
	await until(() => waitingOnLock(2));
	await blocker`select pg_advisory_unlock(4242)`;

	expect(await migrating).toBe("applied");
	await expect(starting).rejects.toThrow("newer_migration_detected");
	await orchestrator.stopped.catch(() => {});
	expect(await db.sql`select id from pgconductor._private_orchestrators`).toEqual([]);
}, 30000);
