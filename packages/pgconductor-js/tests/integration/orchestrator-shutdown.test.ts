import { afterAll, afterEach, beforeAll, test, expect, jest } from "bun:test";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { Deferred } from "../../src/lib/deferred";

let pool: TestDatabasePool;
const databases: TestDatabase[] = [];

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterEach(async () => {
	jest.useRealTimers();
	await Promise.all(databases.map((db) => db.destroy()));
	databases.length = 0;
});

afterAll(async () => {
	await pool?.destroy();
});

test("gracefully shuts down on stop()", async () => {
	const db = await pool.child();
	databases.push(db);

	const conductor = Conductor.create({
		sql: db.sql,
		context: {},
	});

	await conductor.ensureInstalled();

	const orch = Orchestrator.create({ conductor });

	await orch.start();
	expect(orch.isStarted).toBe(true);
	expect(orch.isStopped).toBe(false);

	// Call stop() (same logic as signal handler, but without process.kill())
	await orch.stop();

	expect(orch.isStopped).toBe(true);
});

test("stop() can be called multiple times safely", async () => {
	const db = await pool.child();
	databases.push(db);

	const conductor = Conductor.create({
		sql: db.sql,
		context: {},
	});

	await conductor.ensureInstalled();

	const orch = Orchestrator.create({ conductor });

	await orch.start();

	// Call stop() multiple times (simulates duplicate signals)
	await Promise.all([orch.stop(), orch.stop(), orch.stop()]);

	expect(orch.isStopped).toBe(true);
});

test("abortController triggers shutdown", async () => {
	const db = await pool.child();
	databases.push(db);

	const conductor = Conductor.create({
		sql: db.sql,
		context: {},
	});

	await conductor.ensureInstalled();

	const orch = Orchestrator.create({ conductor });

	await orch.start();
	expect(orch.isShuttingDown).toBe(false);

	// Manually abort (simulates what stop() does internally)
	(orch as any).abortController.abort();

	expect(orch.isShuttingDown).toBe(true);

	// Wait for shutdown to complete
	await orch.stopped;

	expect(orch.isStopped).toBe(true);
});

test("stop() waits for cleanup to complete", async () => {
	const db = await pool.child();
	databases.push(db);

	const conductor = Conductor.create({
		sql: db.sql,
		context: {},
	});

	await conductor.ensureInstalled();

	const orch = Orchestrator.create({ conductor });

	await orch.start();
	expect(orch.isStarted).toBe(true);

	// Track when cleanup starts and completes
	let cleanupStarted = false;
	let cleanupCompleted = false;

	const originalCleanup = (orch as any).cleanup.bind(orch);
	(orch as any).cleanup = async () => {
		cleanupStarted = true;
		await originalCleanup();
		cleanupCompleted = true;
	};

	// Call stop() - should wait for cleanup
	const stopPromise = orch.stop();

	// Stop should not return until cleanup completes
	expect(cleanupCompleted).toBe(false);

	await stopPromise;

	// After stop() returns, cleanup MUST be complete
	expect(cleanupStarted).toBe(true);
	expect(cleanupCompleted).toBe(true);
	expect(orch.isStopped).toBe(true);
});

test("stop() waits for an in-flight heartbeat and nothing runs after cleanup", async () => {
	const db = await pool.child();
	databases.push(db);

	const conductor = Conductor.create({
		sql: db.sql,
		context: {},
	});

	await conductor.ensureInstalled();

	const unhandled: unknown[] = [];
	const onUnhandled = (err: unknown) => unhandled.push(err);
	process.on("unhandledRejection", onUnhandled);

	const heartbeat = conductor.db.orchestratorHeartbeat.bind(conductor.db);
	const heartbeatStarted = new Deferred<void>();
	const releaseHeartbeat = new Deferred<void>();
	let heartbeats = 0;
	let heartbeatSettled = false;
	conductor.db.orchestratorHeartbeat = async (args, opts) => {
		if (++heartbeats === 2) {
			heartbeatStarted.resolve();
			await releaseHeartbeat.promise;
			const signals = await heartbeat(args, opts);
			heartbeatSettled = true;
			return signals;
		}
		return heartbeat(args, opts);
	};

	jest.useFakeTimers();
	const orch = Orchestrator.create({ conductor });
	await orch.start();

	jest.advanceTimersByTime(30_000);
	await heartbeatStarted.promise;

	const stopping = orch.stop();
	releaseHeartbeat.resolve();
	await stopping;

	expect(heartbeatSettled).toBe(true);
	const rows = await db.sql`
		select id from pgconductor._private_orchestrators where id = ${orch.info.id}
	`;
	expect(rows.length).toBe(0);

	jest.advanceTimersByTime(120_000);
	await db.sql`select 1`;
	expect(heartbeats).toBe(2);
	process.off("unhandledRejection", onUnhandled);
	expect(unhandled).toEqual([]);
});

test("stop() waits for the running step and releases at the next step", async () => {
	const db = await pool.child();
	databases.push(db);

	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([defineTask({ name: "charge-and-ship" })]),
		context: {},
	});

	const ran: string[] = [];
	const stepStarted = new Deferred<void>();
	const finishStep = new Deferred<void>();

	const task = conductor.createTask(
		{ name: "charge-and-ship" },
		{ invocable: true },
		async (_event, ctx) => {
			await ctx.step("charge", async () => {
				ran.push("charge");
				stepStarted.resolve();
				await finishStep.promise;
			});
			await ctx.step("ship", async () => {
				ran.push("ship");
			});
		},
	);

	const orch = Orchestrator.create({
		conductor,
		tasks: [task],
		defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
	});

	await conductor.ensureInstalled();
	await orch.start();
	await conductor.invoke({ name: "charge-and-ship" }, {});
	await stepStarted.promise;

	let stopped = false;
	const stopping = orch.stop().then(() => {
		stopped = true;
	});
	await Bun.sleep(200);
	expect(stopped).toBe(false);

	finishStep.resolve();
	await stopping;

	expect(ran).toEqual(["charge"]);
	const steps = await db.sql<{ key: string }[]>`select key from pgconductor._private_steps`;
	expect(steps.map((step) => step.key)).toEqual(["charge"]);
	const [execution] = await db.sql<{ attempts: number; last_error: string | null }[]>`
		select attempts, last_error from pgconductor._private_executions where task_key = 'charge-and-ship'
	`;
	expect(execution).toEqual({ attempts: 0, last_error: null });

	await Orchestrator.create({ conductor, tasks: [task] }).drain();
	expect(ran).toEqual(["charge", "ship"]);
});

test("stop() releases a handler that throws on ctx.signal", async () => {
	const db = await pool.child();
	databases.push(db);

	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([defineTask({ name: "watch-signal" })]),
		context: {},
	});

	const started = new Deferred<void>();

	const task = conductor.createTask(
		{ name: "watch-signal" },
		{ invocable: true },
		async (_event, ctx) => {
			started.resolve();
			while (!ctx.signal.aborted) {
				await Bun.sleep(10);
			}
			throw new Error("Task was cancelled");
		},
	);

	const orch = Orchestrator.create({
		conductor,
		tasks: [task],
		defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
	});

	await conductor.ensureInstalled();
	await orch.start();
	await conductor.invoke({ name: "watch-signal" }, {});
	await started.promise;
	await orch.stop();

	const [execution] = await db.sql<
		{ attempts: number; last_error: string | null; failed_at: Date | null }[]
	>`
		select attempts, last_error, failed_at from pgconductor._private_executions where task_key = 'watch-signal'
	`;
	expect(execution).toEqual({ attempts: 0, last_error: null, failed_at: null });
});

test("stop() releases claimed executions that have not started", async () => {
	const db = await pool.child();
	databases.push(db);

	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([
			defineTask({ name: "one-at-a-time" }),
			defineTask({ name: "batch-sibling" }),
		]),
		context: {},
	});

	let runs = 0;
	const started = new Deferred<void>();
	const finish = new Deferred<void>();

	const task = conductor.createTask({ name: "one-at-a-time" }, { invocable: true }, async () => {
		runs++;
		started.resolve();
		await finish.promise;
	});
	// A batched task in the queue lets one slot claim a batch worth of executions
	const batchSibling = conductor.createTask(
		{ name: "batch-sibling", batch: { size: 2, timeoutMs: 50 } },
		{ invocable: true },
		async () => {},
	);

	const orch = Orchestrator.create({
		conductor,
		tasks: [task, batchSibling],
		defaultWorker: { concurrency: 1, fetchBatchSize: 2, pollIntervalMs: 50, flushIntervalMs: 50 },
	});

	await conductor.ensureInstalled();
	await conductor.invoke({ name: "one-at-a-time" }, [{ payload: {} }, { payload: {} }]);
	await orch.start();
	await started.promise;

	const stopping = orch.stop();
	finish.resolve();
	await stopping;

	expect(runs).toBe(1);
	const executions = await db.sql<{ completed: boolean; attempts: number }[]>`
		select completed_at is not null as completed, attempts
		from pgconductor._private_executions where task_key = 'one-at-a-time'
		order by completed_at nulls last
	`;
	expect(executions.map(({ completed, attempts }) => [completed, attempts])).toEqual([
		[true, 1],
		[false, 0],
	]);
});

test("stop() waits for a running batch handler and saves its results", async () => {
	const db = await pool.child();
	databases.push(db);

	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([defineTask({ name: "batch-export" })]),
		context: {},
	});

	const started = new Deferred<void>();
	const finish = new Deferred<void>();

	const task = conductor.createTask(
		{ name: "batch-export", batch: { size: 2, timeoutMs: 50 } },
		{ invocable: true },
		async () => {
			started.resolve();
			await finish.promise;
		},
	);

	const orch = Orchestrator.create({
		conductor,
		tasks: [task],
		defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
	});

	await conductor.ensureInstalled();
	await conductor.invoke({ name: "batch-export" }, [{ payload: {} }, { payload: {} }]);
	await orch.start();
	await started.promise;

	let stopped = false;
	const stopping = orch.stop().then(() => {
		stopped = true;
	});
	await Bun.sleep(200);
	expect(stopped).toBe(false);

	finish.resolve();
	await stopping;

	const executions = await db.sql<{ completed: boolean }[]>`
		select completed_at is not null as completed
		from pgconductor._private_executions where task_key = 'batch-export'
	`;
	expect(executions.map((execution) => execution.completed)).toEqual([true, true]);
});
