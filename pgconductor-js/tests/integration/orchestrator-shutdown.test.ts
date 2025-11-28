import { afterAll, beforeAll, test, expect } from "bun:test";
import { TestDatabasePool } from "../fixtures/test-database";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";

let pool: TestDatabasePool;

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterAll(async () => {
	await pool?.destroy();
});

test("gracefully shuts down on stop()", async () => {
	const db = await pool.child();

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
