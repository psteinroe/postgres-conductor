import { afterAll, afterEach, beforeAll, test, expect, jest } from "bun:test";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { Conductor } from "../../src/conductor";
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
	jest.useRealTimers();
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
	const blockForever = new Deferred<void>();
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

	// Create task handler
	const task = conductor.createTask(taskDefinition, { invocable: true }, async (event, _ctx) => {
		if (event.name === "pgconductor.invoke") {
			await blockForever.promise;
		}
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
	blockForever.resolve();
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

test("a live orchestrator recovers executions locked by a crashed one", async () => {
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

	await db.client.setFakeTime({ date: new Date("2024-01-01T12:00:00Z") });
	await db.client.registerWorker({
		queueName: "default",
		taskSpecs: [{ key: "orphaned-task", queue: "default", maxAttempts: 3 }],
		cronSchedules: [],
		eventSubscriptions: [],
	});
	const crashedId = crypto.randomUUID();
	await db.client.orchestratorHeartbeat({
		orchestratorId: crashedId,
		version: "test",
		migrationNumber: 1,
	});
	await conductor.invoke(taskDefinition, {});
	const claimed = await db.client.getExecutions({
		orchestratorId: crashedId,
		queueName: "default",
		batchSize: 1,
		filterTaskKeys: [],
	});
	expect(claimed.length).toBe(1);

	await db.client.setFakeTime({ date: new Date("2024-01-01T12:10:00Z") });

	jest.useFakeTimers();
	const orch = Orchestrator.create({ conductor, tasks: [task] });
	await orch.start();

	// recovery runs every 8th heartbeat, one heartbeat per 30s
	for (let seconds = 0; runs === 0 && seconds < 600; seconds++) {
		jest.advanceTimersByTime(1000);
		await db.sql`select 1`;
	}
	expect(runs).toBe(1);

	const crashed = await db.sql`
		select id from pgconductor._private_orchestrators where id = ${crashedId}::uuid
	`;
	expect(crashed.length).toBe(0);

	await orch.stop();
	await db.client.clearFakeTime();
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

	jest.useFakeTimers();
	const orch = Orchestrator.create({ conductor, tasks: [task] });
	await orch.start();
	await conductor.invoke(taskDefinition, {});

	while (!handlerStarted.isSettled) {
		jest.advanceTimersByTime(1000);
		await db.sql`select 1`;
	}

	await db.sql`
		update pgconductor._private_orchestrators
		set last_heartbeat_at = now() - interval '1 hour'
		where id = ${orch.info.id}::uuid
	`;
	await db.client.recoverStaleOrchestrators({ maxAge: "5 minutes" });

	for (let seconds = 0; !orch.isStopped && seconds < 60; seconds++) {
		jest.advanceTimersByTime(1000);
		await db.sql`select 1`;
	}
	expect(handlerAborted.isSettled).toBe(true);
	expect(orch.isStopped).toBe(true);
});
