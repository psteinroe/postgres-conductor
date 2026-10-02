import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import { waitForCondition } from "../test-utils";

describe("Batch Processing", () => {
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

	test("executes void batch task - all succeed together", async () => {
		const db = await pool.child();
		databases.push(db);

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
				if (events.length > 0 && events[0]?.name === "pgconductor.invoke") {
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
		databases.push(db);

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
				if (events.length > 0 && events[0]?.name === "pgconductor.invoke") {
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

	test("batch task sleep reschedules all executions and replays past it", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinitions = defineTask({
			name: "batch-sleep",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		let attempts = 0;
		const completed: number[] = [];

		const batchTask = conductor.createTask(
			{
				name: "batch-sleep",
				batch: { size: 2, timeoutMs: 1000 },
			},
			{ invocable: true },
			async (events, ctx) => {
				attempts++;
				await ctx.sleep("wait", 200);
				completed.push(...events.map((e) => e.payload.value));
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

		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();

		expect(attempts).toBe(2);
		expect(completed.sort()).toEqual([1, 2]);
		const pending = await db.sql`
			select id from pgconductor._private_executions
			where task_key = 'batch-sleep' and completed_at is null
		`;
		expect(pending.length).toBe(0);
	}, 30000);

	test("batch task sleep re-sleeps the batch until every execution has slept", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinitions = defineTask({
			name: "batch-sleep-mixed",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		const batches: number[][] = [];
		const completed: number[] = [];

		const batchTask = conductor.createTask(
			{
				name: "batch-sleep-mixed",
				batch: { size: 2, timeoutMs: 10 },
			},
			{ invocable: true },
			async (events, ctx) => {
				batches.push(events.map((e) => e.payload.value).sort());
				await ctx.sleep("wait", 1000);
				completed.push(...events.map((e) => e.payload.value));
			},
		);

		const drain = () =>
			Orchestrator.create({
				conductor,
				tasks: [batchTask],
				defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			}).drain();

		await conductor.ensureInstalled();
		await db.client.setFakeTime({ date: new Date("2024-01-01T12:00:00Z") });

		await conductor.invoke({ name: "batch-sleep-mixed" }, { value: 1 });
		await drain();

		await conductor.invoke({ name: "batch-sleep-mixed" }, { value: 2 });
		await db.client.setFakeTime({ date: new Date("2024-01-01T12:00:01Z") });
		await drain();

		await db.client.setFakeTime({ date: new Date("2024-01-01T12:00:02Z") });
		await drain();

		await db.client.clearFakeTime();

		expect(batches).toEqual([[1], [1, 2], [1, 2]]);
		expect(completed.sort()).toEqual([1, 2]);
		const pending = await db.sql`
			select id from pgconductor._private_executions
			where task_key = 'batch-sleep-mixed' and completed_at is null
		`;
		expect(pending.length).toBe(0);
	}, 30000);

	test("batch task throws - all fail together", async () => {
		const db = await pool.child();
		databases.push(db);

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
		databases.push(db);

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
				if (events.length > 0 && events[0]?.name === "pgconductor.invoke") {
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
				if (event.name === "pgconductor.invoke") {
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
		databases.push(db);

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

		await conductor.ensureInstalled();

		// Invoke just one task
		await conductor.invoke({ name: "batch-single" }, { value: 1 });

		await orchestrator.drain();

		expect(executed).toBe(true);
	}, 30000);

	test("fails cancelled executions claimed alongside active ones", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinitions = defineTask({
			name: "batch-cancel",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		const started = new Deferred();
		const proceed = new Deferred();
		const batches: number[][] = [];

		const batchTask = conductor.createTask(
			{
				name: "batch-cancel",
				batch: { size: 2, timeoutMs: 100 },
			},
			{ invocable: true },
			async (events, ctx) => {
				batches.push(events.map((e) => e.payload.value));
				if (batches.length === 1) {
					started.resolve();
					await proceed.promise;
					await ctx.sleep("wait", 100);
				}
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [batchTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		const [cancelledId, activeId] = await Promise.all([
			conductor.invoke({ name: "batch-cancel" }, { value: 1 }),
			conductor.invoke({ name: "batch-cancel" }, { value: 2 }),
		]);

		await started.promise;
		expect(await conductor.cancel(cancelledId)).toBe(true);
		proceed.resolve();

		const states = () => db.sql<
			{ id: string; completed_at: Date | null; failed_at: Date | null; locked_by: string | null }[]
		>`
			select id, completed_at, failed_at, locked_by
			from pgconductor._private_executions
			where task_key = 'batch-cancel'
		`;

		await waitForCondition(async () => {
			const rows = await states();
			return rows.every((row) => row.completed_at || row.failed_at);
		}, 5000);

		const rows = await states();
		await orchestrator.stop();

		const cancelled = rows.find((row) => row.id === cancelledId);
		const active = rows.find((row) => row.id === activeId);
		expect(cancelled?.failed_at).not.toBeNull();
		expect(cancelled?.locked_by).toBeNull();
		expect(active?.completed_at).not.toBeNull();
		expect(batches).toEqual([[1, 2], [2]]);
	}, 30000);
});
