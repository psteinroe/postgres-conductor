import { afterAll, afterEach, beforeAll, test, expect, jest } from "bun:test";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
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
