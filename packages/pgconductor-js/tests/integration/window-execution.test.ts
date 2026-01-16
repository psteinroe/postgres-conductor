import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";

describe("Window Execution", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		await Promise.all(databases.map((db) => db.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	test("task releases when hitting step outside window", async () => {
		const db = await pool.child();
		databases.push(db);

		// Start inside window so task gets fetched
		const insideTime = new Date("2024-01-01T10:00:00Z");
		await db.client.setFakeTime({ date: insideTime });

		const taskDefinitions = defineTask({
			name: "window-task",
			payload: z.object({ value: z.number() }),
			returns: z.object({ result: z.number() }),
		});

		const stepFn = mock((n: number) => n * 2);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			// Pass db client so task can advance time
			context: { dbClient: db.client },
		});

		const windowTask = conductor.createTask(
			{ name: "window-task", window: ["09:00", "17:00"] },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name === "pgconductor.invoke") {
					// Advance time to outside window BEFORE calling ctx.step()
					// This simulates the scenario where time passes during execution
					await ctx.dbClient.setFakeTime({ date: new Date("2024-01-01T18:00:00Z") });

					// This step will trigger release (we're now outside window)
					const result = await ctx.step("step1", () => {
						return stepFn(event.payload.value);
					});

					return { result };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [windowTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		const executionId = await conductor.invoke({ name: "window-task" }, { value: 5 });

		// Wait a bit for execution attempt
		await new Promise((r) => setTimeout(r, 300));

		// Step should not execute (released before executing)
		expect(stepFn).not.toHaveBeenCalled();

		// Check execution is pending (released)
		const executions = await db.sql`
			select * from pgconductor._private_executions
			where id = ${executionId}
		`;
		expect(executions.length).toBeGreaterThan(0);
		const execution = executions[0];
		if (!execution) throw new Error("execution not found");
		expect(execution.completed_at).toBeNull();

		// run_at should be scheduled for next day at 09:00
		const nextDay9AM = new Date("2024-01-02T09:00:00Z");
		expect(execution.run_at.getTime()).toBeGreaterThanOrEqual(nextDay9AM.getTime());

		await orchestrator.stop();
	}, 30000);

	test("task releases when hitting checkpoint outside window", async () => {
		const db = await pool.child();
		databases.push(db);

		// Start inside window so task gets fetched
		const insideTime = new Date("2024-01-01T10:00:00Z");
		await db.client.setFakeTime({ date: insideTime });

		const taskDefinitions = defineTask({
			name: "checkpoint-task",
			payload: z.object({}),
			returns: z.object({ completed: z.boolean() }),
		});

		const beforeCheckpoint = mock(() => "before");
		const afterCheckpoint = mock(() => "after");

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			// Pass db client so task can advance time
			context: { dbClient: db.client },
		});

		const checkpointTask = conductor.createTask(
			{ name: "checkpoint-task", window: ["09:00", "17:00"] },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name === "pgconductor.invoke") {
					beforeCheckpoint();

					// Advance time to outside window BEFORE calling ctx.checkpoint()
					await ctx.dbClient.setFakeTime({ date: new Date("2024-01-01T17:30:00Z") });

					// Checkpoint should trigger release (we're now outside window)
					await ctx.checkpoint();

					// This should not execute
					afterCheckpoint();
					return { completed: true };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [checkpointTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		const executionId = await conductor.invoke({ name: "checkpoint-task" }, {});

		// Wait for execution
		await new Promise((r) => setTimeout(r, 300));

		// Task starts executing, reaches checkpoint, then gets released because outside window
		expect(beforeCheckpoint).toHaveBeenCalledTimes(1); // Called before checkpoint
		expect(afterCheckpoint).not.toHaveBeenCalled(); // Not called - released at checkpoint

		// Check execution was released (not completed)
		const executions = await db.sql`
			select * from pgconductor._private_executions
			where id = ${executionId}
		`;
		expect(executions.length).toBeGreaterThan(0);
		const execution2 = executions[0];
		if (!execution2) throw new Error("execution not found");
		expect(execution2.completed_at).toBeNull();

		await orchestrator.stop();
	}, 30000);

	test("task executes fully when inside window", async () => {
		const db = await pool.child();
		databases.push(db);

		// Set time inside window (10:00, window is 09:00-17:00)
		const insideTime = new Date("2024-01-01T10:00:00Z");
		await db.client.setFakeTime({ date: insideTime });

		const taskDefinitions = defineTask({
			name: "inside-window-task",
			payload: z.object({ value: z.number() }),
			returns: z.object({ result: z.number() }),
		});

		const stepFn = mock((n: number) => n * 2);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		const insideTask = conductor.createTask(
			{ name: "inside-window-task", window: ["09:00", "17:00"] },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name === "pgconductor.invoke") {
					const step1 = await ctx.step("step1", () => {
						return stepFn(event.payload.value);
					});

					await ctx.checkpoint();

					const step2 = await ctx.step("step2", () => {
						return stepFn(step1);
					});

					return { result: step2 };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [insideTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		const executionId = await conductor.invoke({ name: "inside-window-task" }, { value: 5 });

		// Wait for completion
		await new Promise((r) => setTimeout(r, 500));

		// Both steps should execute
		expect(stepFn).toHaveBeenCalledTimes(2);
		expect(stepFn).toHaveBeenNthCalledWith(1, 5);
		expect(stepFn).toHaveBeenNthCalledWith(2, 10);

		// Check execution completed
		const executions = await db.sql`
			select * from pgconductor._private_executions
			where id = ${executionId}
		`;
		expect(executions.length).toBeGreaterThan(0);
		const execution3 = executions[0];
		if (!execution3) throw new Error("execution not found");
		expect(execution3.completed_at).not.toBeNull();

		await orchestrator.stop();
	}, 30000);

	test("task resumes when window reopens", async () => {
		const db = await pool.child();
		databases.push(db);

		// Start outside window
		const outsideTime = new Date("2024-01-01T18:00:00Z");
		await db.client.setFakeTime({ date: outsideTime });

		const taskDefinitions = defineTask({
			name: "resume-task",
			payload: z.object({ value: z.number() }),
			returns: z.object({ result: z.number() }),
		});

		const stepFn = mock((n: number) => n * 2);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		const resumeTask = conductor.createTask(
			{ name: "resume-task", window: ["09:00", "17:00"] },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name === "pgconductor.invoke") {
					const result = await ctx.step("step1", () => {
						return stepFn(event.payload.value);
					});

					return { result };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [resumeTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		const executionId = await conductor.invoke({ name: "resume-task" }, { value: 5 });

		// Wait a bit
		await new Promise((r) => setTimeout(r, 200));

		// Task should not execute yet (outside window)
		expect(stepFn).not.toHaveBeenCalled();

		// Move back inside window (next day at 10:00)
		await db.client.setFakeTime({ date: new Date("2024-01-02T10:00:00Z") });

		// Wait for task to resume and complete
		await new Promise((r) => setTimeout(r, 500));

		// Task should now execute
		expect(stepFn).toHaveBeenCalledTimes(1);
		expect(stepFn).toHaveBeenCalledWith(5);

		// Check execution completed
		const executions = await db.sql`
			select * from pgconductor._private_executions
			where id = ${executionId}
		`;
		expect(executions.length).toBeGreaterThan(0);
		const execution3 = executions[0];
		if (!execution3) throw new Error("execution not found");
		expect(execution3.completed_at).not.toBeNull();

		await orchestrator.stop();
	}, 30000);
});
