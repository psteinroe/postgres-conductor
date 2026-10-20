import { afterAll, afterEach, beforeAll, expect, test } from "vitest";
import postgres, { type Sql } from "postgres";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { DatabaseClient } from "../../src/database-client";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import { DefaultLogger } from "../../src/lib/logger";
import { MigrationStore } from "../../src/migration-store";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

let pool: TestDatabasePool;
const databases: TestDatabase[] = [];
const connections: Sql[] = [];

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterEach(async () => {
	await Promise.all(connections.map((sql) => sql.end({ timeout: 1 })));
	connections.length = 0;
	await Promise.all(databases.map((db) => db.destroy()));
	databases.length = 0;
});

afterAll(async () => {
	await pool?.destroy();
});

async function setup(name: string) {
	const db = await pool.child();
	databases.push(db);
	const sql = postgres(db.url, { max: 5, onnotice: () => {} });
	connections.push(sql);
	const def = defineTask({ name, payload: z.object({}) });
	const heldDefinition = defineTask({ name: "held", payload: z.object({}) });
	const conductor = Conductor.create({
		sql,
		tasks: TaskSchemas.fromSchema([def, heldDefinition]),
		context: {},
	});
	await conductor.ensureInstalled();

	const lockedBy = async (id: string) => {
		const [row] = await sql<{ locked_by: string | null }[]>`
			select locked_by from pgconductor._private_executions where id = ${id}::uuid
		`;
		return row?.locked_by;
	};
	// Another orchestrator polling the queue
	const claimElsewhere = async () =>
		(
			await conductor.db.getExecutions({
				orchestratorId: await db.registerOrchestrator(),
				queueName: "default",
				batchSize: 10,
				taskKeys: [name],
			})
		).executions.map((execution) => execution.id);

	return { db, sql, def, heldDefinition, conductor, lockedBy, claimElsewhere };
}

test("an old settlement ack does not drop a newer claim of the same execution", async () => {
	const { def, conductor, lockedBy, claimElsewhere } = await setup("old-ack");

	const secondAttemptStarted = new Deferred<void>();
	const finishSecondAttempt = new Deferred<void>();
	let attempts = 0;
	const task = conductor.createTask(def, { invocable: true }, async (_event, ctx) => {
		attempts++;
		// The first attempt releases with zero delay, so the execution is claimable once it settles
		await ctx.sleep("s", 0);
		secondAttemptStarted.resolve();
		await finishSecondAttempt.promise;
	});

	// The first settlement commits, but its response is held until the second attempt started
	const deliverFirstAck = new Deferred<void>();
	let settlements = 0;
	const returnExecutions = conductor.db.returnExecutions.bind(conductor.db);
	conductor.db.returnExecutions = async (args, opts) => {
		await returnExecutions(args, opts);
		if (++settlements === 1) await deliverFirstAck.promise;
	};
	let claims = 0;
	const getExecutions = conductor.db.getExecutions.bind(conductor.db);
	conductor.db.getExecutions = async (args) => {
		const result = await getExecutions(args);
		claims++;
		return result;
	};

	const orch = Orchestrator.create({
		conductor,
		tasks: [task],
		defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 10, flushBatchSize: 100 },
	});
	await orch.start();
	const id = (await conductor.invoke(def, {})) as string;
	await secondAttemptStarted.promise;

	deliverFirstAck.resolve();
	// Every claim releases the claims the worker does not hold
	const claimsAfterAck = claims;
	await waitForCondition(() => claims >= claimsAfterAck + 3);

	try {
		expect({ lockedBy: await lockedBy(id), claimedElsewhere: await claimElsewhere() }).toEqual({
			lockedBy: orch.info.id,
			claimedElsewhere: [],
		});
	} finally {
		finishSecondAttempt.resolve();
		await orch.stop();
	}
	expect(attempts).toBe(2);
});

test("a claim whose response is still in flight is not released", async () => {
	const { sql, def, conductor, lockedBy, claimElsewhere } = await setup("claim-in-flight");

	const runs: string[] = [];
	const task = conductor.createTask(def, { invocable: true }, async (_event, ctx) => {
		runs.push(ctx.executionId);
	});

	const claimCommitted = new Deferred<string>();
	const deliverClaim = new Deferred<void>();
	const getExecutions = conductor.db.getExecutions.bind(conductor.db);
	conductor.db.getExecutions = async (args) => {
		const result = await getExecutions(args);
		const [claimed] = result.executions;
		if (claimed && !claimCommitted.isSettled) {
			claimCommitted.resolve(claimed.id);
			await deliverClaim.promise;
		}
		return result;
	};

	const orch = Orchestrator.create({
		conductor,
		tasks: [task],
		defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 10 },
	});
	await orch.start();
	await conductor.invoke(def, {});
	const id = await claimCommitted.promise;

	// The response is delayed for longer than any lease; heartbeats and recovery keep running
	await sql`update pgconductor._private_executions set locked_at = locked_at - interval '1 hour' where id = ${id}::uuid`;
	expect(await conductor.db.orchestratorHeartbeat({ orchestratorId: orch.info.id })).toEqual({
		shutdown_reason: null,
	});
	await conductor.db.recoverStaleOrchestrators({ maxAge: "300000 milliseconds" });
	const lockedWhileInFlight = await lockedBy(id);
	const claimedElsewhere = await claimElsewhere();

	deliverClaim.resolve();
	await waitForCondition(() => runs.length === 1);
	await orch.stop();

	expect({ lockedWhileInFlight, claimedElsewhere, runs }).toEqual({
		lockedWhileInFlight: orch.info.id,
		claimedElsewhere: [],
		runs: [id],
	});
});

test("a claim releases a claim the worker does not hold", async () => {
	const { db, def, heldDefinition, conductor, lockedBy } = await setup("orphaned");

	const heldStarted = new Deferred<void>();
	const finishHeld = new Deferred<void>();
	const heldTask = conductor.createTask(heldDefinition, { invocable: true }, async () => {
		heldStarted.resolve();
		await finishHeld.promise;
	});
	let orphanedRuns = 0;
	const orphanedTask = conductor.createTask(def, { invocable: true }, async () => {
		orphanedRuns++;
	});

	// The worker pauses before its next claim, so it cannot claim the execution of the lost claim
	let paused = false;
	const reachedPause = new Deferred<void>();
	const resume = new Deferred<void>();
	const getExecutions = conductor.db.getExecutions.bind(conductor.db);
	conductor.db.getExecutions = async (args) => {
		if (paused) {
			reachedPause.resolve();
			await resume.promise;
		}
		return getExecutions(args);
	};

	const orch = Orchestrator.create({
		conductor,
		tasks: [heldTask, orphanedTask],
		defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 10, concurrency: 2 },
	});
	await orch.start();
	const heldId = (await conductor.invoke(heldDefinition, {})) as string;
	await heldStarted.promise;

	// A claim whose response was lost: it committed, but the worker never received it
	paused = true;
	await reachedPause.promise;
	const orphanedId = (await conductor.invoke(def, {})) as string;
	const [orphaned] = (
		await db.client.getExecutions({
			orchestratorId: orch.info.id,
			queueName: "default",
			batchSize: 1,
			taskKeys: ["orphaned"],
		})
	).executions;
	expect(orphaned?.id).toBe(orphanedId);

	resume.resolve();
	await waitForCondition(() => orphanedRuns === 1);
	expect(await lockedBy(heldId)).toBe(orch.info.id);

	finishHeld.resolve();
	await orch.stop();
});

test("stale recovery racing a claim leaves no execution locked to a deleted owner", async () => {
	const { db, sql, def, conductor, lockedBy } = await setup("recovery-race");
	await db.client.registerWorker({
		queueName: "default",
		taskSpecs: [{ key: "recovery-race", queue: "default", maxAttempts: 3 }],
		cronSchedules: [],
		eventSubscriptions: [],
	});
	const id = (await conductor.invoke(def, {})) as string;

	// Orchestrator A has missed its heartbeats
	const a = await db.registerOrchestrator();
	await sql`update pgconductor._private_orchestrators set last_heartbeat_at = now() - interval '1 hour' where id = ${a}::uuid`;

	// A's claim commits only after recovery started its delete
	const claimSql = postgres(db.url, { max: 1, onnotice: () => {} });
	connections.push(claimSql);
	const tx = await claimSql.reserve();
	await tx`begin`;
	const claim = await new DatabaseClient({ sql: tx, logger: new DefaultLogger() }).getExecutions({
		orchestratorId: a,
		queueName: "default",
		batchSize: 1,
		taskKeys: ["recovery-race"],
	});
	expect(claim.executions.map((execution) => execution.id)).toEqual([id]);

	const recovery = conductor.db.recoverStaleOrchestrators({ maxAge: "300000 milliseconds" });
	await waitForCondition(async () => {
		const [row] = await sql<{ waiting: number }[]>`
			select count(*)::int as waiting from pg_stat_activity
			where datname = current_database() and wait_event_type = 'Lock'
				and query like '%_private_remove_orchestrators%'
		`;
		return row?.waiting === 1;
	});
	await tx`commit`;
	tx.release();
	await recovery;

	const [owner] =
		await sql`select id from pgconductor._private_orchestrators where id = ${a}::uuid`;
	expect({ ownerDeleted: owner === undefined, lockedBy: await lockedBy(id) }).toEqual({
		ownerDeleted: true,
		lockedBy: null,
	});
});

test("a claim by an owner that recovery already deleted claims nothing", async () => {
	const { db, sql, def, conductor } = await setup("deleted-owner");
	await db.client.registerWorker({
		queueName: "default",
		taskSpecs: [{ key: "deleted-owner", queue: "default", maxAttempts: 3 }],
		cronSchedules: [],
		eventSubscriptions: [],
	});
	await conductor.invoke(def, {});

	const a = await db.registerOrchestrator();
	await sql`update pgconductor._private_orchestrators set last_heartbeat_at = now() - interval '1 hour' where id = ${a}::uuid`;
	await conductor.db.recoverStaleOrchestrators({ maxAge: "300000 milliseconds" });

	const claim = await conductor.db.getExecutions({
		orchestratorId: a,
		queueName: "default",
		batchSize: 1,
		taskKeys: ["deleted-owner"],
	});
	expect(claim.executions).toEqual([]);
});

test("an orchestrator recovered as stale still learns it after one lost heartbeat response", async () => {
	const { db, sql, conductor } = await setup("recovered");
	const a = await db.registerOrchestrator();
	await sql`update pgconductor._private_orchestrators set last_heartbeat_at = now() - interval '1 hour' where id = ${a}::uuid`;
	await conductor.db.recoverStaleOrchestrators({ maxAge: "300000 milliseconds" });

	// The response of this heartbeat is lost
	await conductor.db.orchestratorHeartbeat({ orchestratorId: a });
	expect(await conductor.db.orchestratorHeartbeat({ orchestratorId: a })).toBeUndefined();
});

test("a shutdown survives one lost heartbeat response", async () => {
	const { db, conductor } = await setup("shutdown");
	const migrationNumber = new MigrationStore().getLatestMigrationNumber();
	const a = await db.registerOrchestrator();
	// A breaking migration sweeps the orchestrators below it
	await conductor.db.sweepOrchestrators({ migrationNumber: migrationNumber + 1 });

	// The response of this heartbeat is lost
	await conductor.db.orchestratorHeartbeat({ orchestratorId: a });
	expect(await conductor.db.orchestratorHeartbeat({ orchestratorId: a })).toEqual({
		shutdown_reason: "breaking_migration",
	});
});
