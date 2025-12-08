import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";

describe("Batch Processing", () => {
	let pool: TestDatabasePool;

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterAll(async () => {
		await pool?.destroy();
	});

	test("executes void batch task - all succeed together", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "batch-void",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		const processed: number[][] = [];

		const batchTask = conductor.createTask(
			{
				name: "batch-void",
				batch: { size: 3, timeoutMs: 1000 },
			},
			{ invocable: true },
			async (events, ctx) => {
				if (events.length > 0 && events[0]?.event === "pgconductor.invoke") {
					const batch = events.map((e) => e.payload.value);
					processed.push(batch);
				}
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [batchTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Invoke 5 tasks - should batch as [3] and [2]
		await Promise.all([
			conductor.invoke({ name: "batch-void" }, { value: 1 }),
			conductor.invoke({ name: "batch-void" }, { value: 2 }),
			conductor.invoke({ name: "batch-void" }, { value: 3 }),
			conductor.invoke({ name: "batch-void" }, { value: 4 }),
			conductor.invoke({ name: "batch-void" }, { value: 5 }),
		]);

		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();

		expect(processed.length).toBe(2);
		expect(processed[0]?.sort()).toEqual([1, 2, 3]);
		expect(processed[1]?.sort()).toEqual([4, 5]);
	}, 30000);

	test("executes batch task with returns - individual results", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "batch-returns",
			payload: z.object({ value: z.number() }),
			returns: z.object({ doubled: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		const processedBatches: Array<{ doubled: number }[]> = [];

		const batchTask = conductor.createTask(
			{
				name: "batch-returns",
				batch: { size: 2, timeoutMs: 1000 },
			},
			{ invocable: true },
			async (events, ctx) => {
				if (events.length > 0 && events[0]?.event === "pgconductor.invoke") {
					const results = events.map((e) => ({
						doubled: e.payload.value * 2,
					}));
					processedBatches.push(results);
					return results;
				}
				return [];
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [batchTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		await Promise.all([
			conductor.invoke({ name: "batch-returns" }, { value: 5 }),
			conductor.invoke({ name: "batch-returns" }, { value: 10 }),
		]);

		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();

		// Verify the batch handler was called with correct data and returned correct results
		expect(processedBatches.length).toBe(1);
		expect(processedBatches[0]).toEqual([{ doubled: 10 }, { doubled: 20 }]);
	}, 30000);

	test("batch task sleep reschedules all executions", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "batch-sleep",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		let executionAttempts = 0;

		const batchTask = conductor.createTask(
			{
				name: "batch-sleep",
				batch: { size: 2, timeoutMs: 1000 },
			},
			{ invocable: true },
			async (events, ctx) => {
				executionAttempts++;
				if (executionAttempts === 1) {
					// First attempt: sleep
					await ctx.sleep("wait", 1000);
				}
				// Second attempt: complete
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [batchTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		await Promise.all([
			conductor.invoke({ name: "batch-sleep" }, { value: 1 }),
			conductor.invoke({ name: "batch-sleep" }, { value: 2 }),
		]);

		await new Promise((r) => setTimeout(r, 4000));

		await orchestrator.stop();

		// Should have executed twice (once for sleep, once for completion)
		expect(executionAttempts).toBe(2);
	}, 30000);

	test("batch task throws - all fail together", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "batch-fail",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		let attemptCount = 0;

		const batchTask = conductor.createTask(
			{
				name: "batch-fail",
				batch: { size: 2, timeoutMs: 1000 },
				maxAttempts: 1,
			},
			{ invocable: true },
			async (events, ctx) => {
				attemptCount++;
				throw new Error("Batch failed");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [batchTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		await Promise.all([
			conductor.invoke({ name: "batch-fail" }, { value: 1 }),
			conductor.invoke({ name: "batch-fail" }, { value: 2 }),
		]);

		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();

		// Should have been attempted once (maxAttempts: 1)
		expect(attemptCount).toBe(1);
	}, 30000);

	test("mixed batch and non-batch tasks work together", async () => {
		const db = await pool.child();

		const batchedTask = defineTask({
			name: "batched",
			payload: z.object({ value: z.number() }),
		});

		const normalTask = defineTask({
			name: "normal",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([batchedTask, normalTask]),
			context: {},
		});

		const batchedProcessed: number[][] = [];
		const normalProcessed: number[] = [];

		const batchedTaskImpl = conductor.createTask(
			{
				name: "batched",
				batch: { size: 2, timeoutMs: 1000 },
			},
			{ invocable: true },
			async (events, ctx) => {
				if (events.length > 0 && events[0]?.event === "pgconductor.invoke") {
					const batch = events.map((e) => e.payload.value);
					batchedProcessed.push(batch);
				}
			},
		);

		const normalTaskImpl = conductor.createTask(
			{
				name: "normal",
			},
			{ invocable: true },
			async (event, ctx) => {
				if (event.event === "pgconductor.invoke") {
					normalProcessed.push(event.payload.value);
				}
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [batchedTaskImpl, normalTaskImpl],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		await Promise.all([
			conductor.invoke({ name: "normal" }, { value: 1 }),
			conductor.invoke({ name: "batched" }, { value: 2 }),
			conductor.invoke({ name: "batched" }, { value: 3 }),
			conductor.invoke({ name: "normal" }, { value: 4 }),
		]);

		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();

		// Normal tasks should be processed individually
		expect(normalProcessed.sort()).toEqual([1, 4]);

		// Batched tasks should be processed together
		expect(batchedProcessed.length).toBe(1);
		expect(batchedProcessed[0]?.sort()).toEqual([2, 3]);
	}, 30000);

	test("single item doesn't wait for batch size", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "batch-single",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		let executed = false;

		const batchTask = conductor.createTask(
			{
				name: "batch-single",
				batch: { size: 10, timeoutMs: 10000 }, // Large batch size and timeout
			},
			{ invocable: true },
			async (events, ctx) => {
				executed = true;
				// Single item in batch
				expect(events.length).toBe(1);
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [batchTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Invoke just one task
		await conductor.invoke({ name: "batch-single" }, { value: 1 });

		// Wait longer to ensure worker has fetched the execution
		await new Promise((r) => setTimeout(r, 500));

		await orchestrator.stop();

		expect(executed).toBe(true);
	}, 30000);
});
