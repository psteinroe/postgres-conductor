import { afterAll, afterEach, beforeAll, test, expect, vi } from "vitest";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { Conductor } from "../../src/conductor";
import { DatabaseClient } from "../../src/database-client";
import { Orchestrator } from "../../src/orchestrator";
import { Deferred } from "../../src/lib/deferred";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { z } from "zod";
import postgres from "postgres";

let pool: TestDatabasePool;
const databases: TestDatabase[] = [];

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterEach(async () => {
	vi.useRealTimers();
	vi.restoreAllMocks();
	await Promise.all(databases.map((db) => db.destroy()));
	databases.length = 0;
});

afterAll(async () => {
	await pool?.destroy();
});

test("cleanup removes orchestrator from database", async () => {
	const db = await pool.child();
	databases.push(db);

	const conductor = Conductor.create({
		sql: db.sql,
		context: {},
	});

	await conductor.ensureInstalled();

	const orch = Orchestrator.create({ conductor });

	await orch.start();

	// Wait for orchestrator to fully start
	await orch.started;

	// Verify orchestrator registered in database
	let rows = await db.sql`
		SELECT * FROM pgconductor._private_orchestrators
		WHERE id = ${orch.info.id}
	`;
	expect(rows.length).toBe(1);
	const row = rows[0];
	expect(row).toBeDefined();
	expect(row?.id).toBe(orch.info.id);

	// Stop orchestrator (should run cleanup)
	await orch.stop();

	// Verify orchestrator removed from database
	rows = await db.sql`
		SELECT * FROM pgconductor._private_orchestrators
		WHERE id = ${orch.info.id}
	`;
	expect(rows.length).toBe(0);
});

test("cleanup releases locked executions", async () => {
	const db = await pool.child();
	databases.push(db);

	const conductorSql = postgres(db.url, { max: 5 }); // Allow concurrent queries

	// Define task
	const taskDefinition = defineTask({
		name: "blocking-task",
		payload: z.object({}),
	});

	const conductor = Conductor.create({
		sql: conductorSql,
		tasks: TaskSchemas.fromSchema([taskDefinition]),
		context: {},
	});

	await conductor.ensureInstalled();

	// Create task handler that blocks until shutdown
	const task = conductor.createTask(taskDefinition, { invocable: true }, async (_event, ctx) => {
		await new Promise((_resolve, reject) => {
			ctx.signal.addEventListener("abort", () => reject(new Error("Task was stopped")));
		});
	});

	const orch = Orchestrator.create({
		conductor,
		tasks: [task],
		defaultWorker: { pollIntervalMs: 100 },
	});

	await orch.start();

	// Invoke task (will be picked up by worker and block)
	await conductor.invoke(taskDefinition, {});

	// Wait for worker to claim the execution
	await new Promise((r) => setTimeout(r, 2000));

	// Verify execution is locked by our orchestrator
	let locked = await db.sql`
		SELECT * FROM pgconductor._private_executions
		WHERE locked_by = ${orch.info.id}
	`;
	expect(locked.length).toBe(1);

	// Stop orchestrator (should release locks via cleanup)
	await orch.stop();

	// Verify locks released (locked_by should be NULL)
	locked = await db.sql`
		SELECT * FROM pgconductor._private_executions
		WHERE locked_by = ${orch.info.id}
	`;
	expect(locked.length).toBe(0);

	// Verify execution still exists but unlocked
	const executions = await db.sql`
		SELECT * FROM pgconductor._private_executions
		WHERE task_key = 'blocking-task'
	`;
	expect(executions.length).toBe(1);
	const execution = executions[0];
	expect(execution).toBeDefined();
	expect(execution?.locked_by).toBe(null);

	// Cleanup
	await conductorSql.end();
});

test("cleanup runs when worker crashes during running phase", async () => {
	const db = await pool.child();
	databases.push(db);

	// Define task that throws error
	const taskDefinition = defineTask({
		name: "crash-task",
		payload: z.object({}),
	});

	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([taskDefinition]),
		context: {},
	});

	await conductor.ensureInstalled();

	// Create task handler
	conductor.createTask(taskDefinition, { invocable: true }, async (event, _ctx) => {
		if (event.name === "pgconductor.invoke") {
			throw new Error("Task failed");
		}
	});

	const orch = Orchestrator.create({ conductor });

	await orch.start();

	// Verify orchestrator registered
	let rows = await db.sql`
		SELECT * FROM pgconductor._private_orchestrators
		WHERE id = ${orch.info.id}
	`;
	expect(rows.length).toBe(1);

	// Invoke task (will cause worker to crash)
	await conductor.invoke(taskDefinition, {});

	// Wait for task to fail and be retried
	await new Promise((r) => setTimeout(r, 2000));

	// Stop orchestrator
	await orch.stop();

	// Cleanup should still run - orchestrator removed
	rows = await db.sql`
		SELECT * FROM pgconductor._private_orchestrators
		WHERE id = ${orch.info.id}
	`;
	expect(rows.length).toBe(0);

	expect(orch.isStopped).toBe(true);
});

test("multiple orchestrators cleanup independently", async () => {
	const db = await pool.child();
	databases.push(db);

	// Create separate connections for each conductor with higher pool size
	const sql1 = postgres(db.url, { max: 5 });
	const sql2 = postgres(db.url, { max: 5 });

	// Define a simple task for the orchestrators to handle
	const taskDefinition = defineTask({
		name: "idle-task",
		payload: z.object({}),
	});

	const conductor1 = Conductor.create({
		sql: sql1,
		tasks: TaskSchemas.fromSchema([taskDefinition]),
		context: {},
	});

	const conductor2 = Conductor.create({
		sql: sql2,
		tasks: TaskSchemas.fromSchema([taskDefinition]),
		context: {},
	});

	await conductor1.ensureInstalled();

	// Create task handlers
	const task1 = conductor1.createTask(taskDefinition, { invocable: true }, async (event, _ctx) => {
		// No-op task
	});

	const task2 = conductor2.createTask(taskDefinition, { invocable: true }, async (event, _ctx) => {
		// No-op task
	});

	const orch1 = Orchestrator.create({ conductor: conductor1, tasks: [task1] });
	const orch2 = Orchestrator.create({ conductor: conductor2, tasks: [task2] });

	await orch1.start();
	await orch2.start();

	// Verify both registered
	let rows = await db.sql`
		SELECT * FROM pgconductor._private_orchestrators
		ORDER BY id
	`;
	expect(rows.length).toBe(2);

	// Stop first orchestrator
	await orch1.stop();

	// Verify only first orchestrator removed
	rows = await db.sql`
		SELECT * FROM pgconductor._private_orchestrators
		WHERE id = ${orch1.info.id}
	`;
	expect(rows.length).toBe(0);

	rows = await db.sql`
		SELECT * FROM pgconductor._private_orchestrators
		WHERE id = ${orch2.info.id}
	`;
	expect(rows.length).toBe(1);

	// Stop second orchestrator
	await orch2.stop();

	// Verify second orchestrator also removed
	rows = await db.sql`
		SELECT * FROM pgconductor._private_orchestrators
		WHERE id = ${orch2.info.id}
	`;
	expect(rows.length).toBe(0);

	// Cleanup connections
	await sql1.end();
	await sql2.end();
});

// The heartbeat connection opens on a timer
async function startWithFakeTimers(orch: Orchestrator): Promise<void> {
	let settled = false;
	const starting = orch.start();
	const done = () => {
		settled = true;
	};
	starting.then(done, done);
	while (!settled) {
		vi.advanceTimersByTime(0);
		await new Promise((resolve) => setImmediate(resolve));
	}
	await starting;
}

// Small steps keep the fake time a query takes close to the real time
async function advance(db: TestDatabase, ms: number, until = () => false): Promise<void> {
	for (let elapsed = 0; elapsed < ms && !until(); elapsed += 100) {
		vi.advanceTimersByTime(100);
		await db.sql`select 1`;
	}
}

async function claimAsCrashedOrchestrator(db: TestDatabase, taskKey: string): Promise<string> {
	const crashedId = await db.registerOrchestrator();
	const { executions } = await db.client.getExecutions({
		orchestratorId: crashedId,
		queueName: "default",
		batchSize: 1,
		taskKeys: [taskKey],
	});
	expect(executions.length).toBe(1);
	return crashedId;
}

async function missHeartbeats(db: TestDatabase, orchestratorId: string, seconds: number) {
	await db.sql`
		update pgconductor._private_orchestrators
		set last_heartbeat_at = now() - ${seconds}::float8 * interval '1 second'
		where id = ${orchestratorId}::uuid
	`;
}

test("an orchestrator recovers executions of one that crashed before it started", async () => {
	const db = await pool.child();
	databases.push(db);

	const taskDefinition = defineTask({ name: "orphaned-task" });
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([taskDefinition]),
		context: {},
	});
	await conductor.ensureInstalled();

	let runs = 0;
	const task = conductor.createTask(taskDefinition, { invocable: true }, async () => {
		runs++;
	});

	await db.client.registerWorker({
		queueName: "default",
		taskSpecs: [{ key: "orphaned-task", queue: "default", maxAttempts: 3 }],
		cronSchedules: [],
		eventSubscriptions: [],
	});
	await conductor.invoke(taskDefinition, {});
	const crashedId = await claimAsCrashedOrchestrator(db, "orphaned-task");
	await missHeartbeats(db, crashedId, 31);

	await Orchestrator.create({ conductor, tasks: [task] }).drain();

	expect(runs).toBe(1);
});

test("a live orchestrator recovers a crashed one within a heartbeat of the stale age", async () => {
	const db = await pool.child();
	databases.push(db);

	const taskDefinition = defineTask({ name: "orphaned-task" });
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([taskDefinition]),
		context: {},
	});
	await conductor.ensureInstalled();

	let runs = 0;
	const task = conductor.createTask(taskDefinition, { invocable: true }, async () => {
		runs++;
	});

	vi.useFakeTimers();
	const orch = Orchestrator.create({ conductor, tasks: [task] });
	await startWithFakeTimers(orch);
	for (let seconds = 0; seconds < 30; seconds++) {
		vi.advanceTimersByTime(1000);
		await db.sql`select 1`;
	}

	// Registered after the live orchestrator started, so startup recovery does not see it
	await conductor.invoke(taskDefinition, {});
	const crashedId = await claimAsCrashedOrchestrator(db, "orphaned-task");
	for (let seconds = 0; seconds < 10; seconds++) {
		vi.advanceTimersByTime(1000);
		await db.sql`select 1`;
	}
	expect(runs).toBe(0);

	await missHeartbeats(db, crashedId, 30);
	// the next heartbeat recovers it, and the next poll claims it
	await advance(db, 7000, () => runs > 0);
	expect(runs).toBe(1);

	await orch.stop();
});

test("after missing heartbeats an orchestrator waits the stale age before recovering others", async () => {
	const db = await pool.child();
	databases.push(db);

	const taskDefinition = defineTask({ name: "orphaned-task" });
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([taskDefinition]),
		context: {},
	});
	await conductor.ensureInstalled();

	let runs = 0;
	const task = conductor.createTask(taskDefinition, { invocable: true }, async () => {
		runs++;
	});

	// An outage of the database for the orchestrator's heartbeat connection
	let unreachable = false;
	const heartbeat = DatabaseClient.prototype.orchestratorHeartbeat;
	vi.spyOn(DatabaseClient.prototype, "orchestratorHeartbeat").mockImplementation(
		async function (this: DatabaseClient, args, opts) {
			if (unreachable) throw new Error("connection refused");
			return heartbeat.call(this, args, opts);
		},
	);

	vi.useFakeTimers();
	const orch = Orchestrator.create({ conductor, tasks: [task] });
	await startWithFakeTimers(orch);
	await advance(db, 30_000);

	await conductor.invoke(taskDefinition, {});
	const crashedId = await claimAsCrashedOrchestrator(db, "orphaned-task");
	unreachable = true;
	await advance(db, 20_000);
	unreachable = false;
	// Every other orchestrator looks stale, but may still be reconnecting
	await missHeartbeats(db, crashedId, 60);
	await advance(db, 25_000);
	expect(runs).toBe(0);

	await advance(db, 20_000, () => runs > 0);
	expect(runs).toBe(1);

	await orch.stop();
});

test("an orchestrator recovered as stale aborts its running handlers and stops", async () => {
	const db = await pool.child();
	databases.push(db);

	const taskDefinition = defineTask({ name: "long-task" });
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([taskDefinition]),
		context: {},
	});
	await conductor.ensureInstalled();

	const handlerStarted = new Deferred<void>();
	const handlerAborted = new Deferred<void>();
	const task = conductor.createTask(taskDefinition, { invocable: true }, async (_event, ctx) => {
		handlerStarted.resolve();
		if (!ctx.signal.aborted) {
			await new Promise((resolve) => ctx.signal.addEventListener("abort", resolve));
		}
		handlerAborted.resolve();
	});

	vi.useFakeTimers();
	const orch = Orchestrator.create({ conductor, tasks: [task] });
	await startWithFakeTimers(orch);
	await conductor.invoke(taskDefinition, {});

	while (!handlerStarted.isSettled) {
		vi.advanceTimersByTime(1000);
		await db.sql`select 1`;
	}

	await missHeartbeats(db, orch.info.id, 3600);
	await db.client.recoverStaleOrchestrators({ maxAge: "30 seconds" });

	for (let seconds = 0; !orch.isStopped && seconds < 60; seconds++) {
		vi.advanceTimersByTime(1000);
		await db.sql`select 1`;
	}
	expect(handlerAborted.isSettled).toBe(true);
	expect(orch.isStopped).toBe(true);
});

test.each(["stop()", "a shutdown signal"])(
	"an orchestrator stopped by %s keeps heartbeating until its handlers finish",
	async (trigger) => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinition = defineTask({ name: "slow-task" });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});
		await conductor.ensureInstalled();

		const handlerStarted = new Deferred<void>();
		const finishHandler = new Deferred<void>();
		const task = conductor.createTask(taskDefinition, { invocable: true }, async () => {
			handlerStarted.resolve();
			await finishHandler.promise;
		});

		vi.useFakeTimers();
		const orch = Orchestrator.create({ conductor, tasks: [task] });
		await startWithFakeTimers(orch);
		await conductor.invoke(taskDefinition, {});

		while (!handlerStarted.isSettled) {
			vi.advanceTimersByTime(1000);
			await db.sql`select 1`;
		}

		if (trigger === "stop()") {
			orch.stop();
		} else {
			await db.sql`
				update pgconductor._private_orchestrators
				set shutdown_reason = 'breaking_migration'
				where id = ${orch.info.id}::uuid
			`;
		}
		for (let seconds = 0; seconds < 10; seconds++) {
			vi.advanceTimersByTime(1000);
			await db.sql`select 1`;
		}
		expect(orch.isShuttingDown).toBe(true);

		const lastHeartbeat = async () => {
			const [row] = await db.sql<{ last_heartbeat_at: Date }[]>`
				select last_heartbeat_at from pgconductor._private_orchestrators where id = ${orch.info.id}::uuid
			`;
			return row?.last_heartbeat_at.getTime() || 0;
		};
		const heartbeatAtShutdown = await lastHeartbeat();
		// longer than the stale age
		for (let seconds = 0; seconds < 60; seconds++) {
			vi.advanceTimersByTime(1000);
			await db.sql`select 1`;
		}
		expect(await lastHeartbeat()).toBeGreaterThan(heartbeatAtShutdown);
		const locked = await db.sql`
			select id from pgconductor._private_executions
			where task_key = 'slow-task' and locked_by = ${orch.info.id}::uuid
		`;
		expect(locked.length).toBe(1);

		finishHandler.resolve();
		await orch.stopped;

		const rows = await db.sql`
			select id from pgconductor._private_orchestrators where id = ${orch.info.id}::uuid
		`;
		expect(rows.length).toBe(0);
		const [execution] = await db.sql<{ completed: boolean }[]>`
			select completed_at is not null as completed from pgconductor._private_executions
			where task_key = 'slow-task'
		`;
		expect(execution?.completed).toBe(true);
	},
);

test("heartbeats keep running while handlers hold every pool connection", async () => {
	const db = await pool.child();
	databases.push(db);

	const taskDefinition = defineTask({ name: "holding-task" });
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([taskDefinition]),
		context: {},
	});
	await conductor.ensureInstalled();

	const handlerStarted = new Deferred<void>();
	const finishHandler = new Deferred<void>();
	const task = conductor.createTask(taskDefinition, { invocable: true }, async () => {
		// the pool has one connection
		await db.sql.begin(async () => {
			handlerStarted.resolve();
			await finishHandler.promise;
		});
	});

	// connects before timers are faked
	const observer = postgres(db.url, { max: 1 });
	await observer`select 1`;

	vi.useFakeTimers();
	const orch = Orchestrator.create({ conductor, tasks: [task] });
	await startWithFakeTimers(orch);
	await conductor.invoke(taskDefinition, {});

	while (!handlerStarted.isSettled) {
		vi.advanceTimersByTime(1000);
		await observer`select 1`;
	}

	const lastHeartbeat = async () => {
		const [row] = await observer<{ last_heartbeat_at: Date }[]>`
			select last_heartbeat_at from pgconductor._private_orchestrators where id = ${orch.info.id}::uuid
		`;
		return row?.last_heartbeat_at.getTime() || 0;
	};
	const before = await lastHeartbeat();
	// longer than the stale age
	for (let seconds = 0; seconds < 60; seconds++) {
		vi.advanceTimersByTime(1000);
		await observer`select 1`;
	}
	const after = await lastHeartbeat();
	await observer.end();

	finishHandler.resolve();
	await orch.stop();
	expect(after).toBeGreaterThan(before);
});
