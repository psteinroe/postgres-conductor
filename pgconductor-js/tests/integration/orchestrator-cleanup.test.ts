import { afterAll, beforeAll, test, expect } from "bun:test";
import { TestDatabasePool } from "../fixtures/test-database";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { Deferred } from "../../src/lib/deferred";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { z } from "zod";
import postgres from "postgres";

let pool: TestDatabasePool;

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterAll(async () => {
	await pool?.destroy();
});

test("cleanup removes orchestrator from database", async () => {
	const db = await pool.child();

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
		if (event.event === "pgconductor.invoke") {
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
		if (event.event === "pgconductor.invoke") {
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
