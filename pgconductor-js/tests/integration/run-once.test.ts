import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";

describe("drain() Mode", () => {
	let pool: TestDatabasePool;

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterAll(async () => {
		await pool?.destroy();
	});

	test("orchestrator.drain() processes all tasks and stops when queue is empty", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "test-task",
			payload: z.object({ value: z.number() }),
		});

		const executionOrder: number[] = [];

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const testTask = conductor.createTask(
			{ name: "test-task" },
			{ invocable: true },
			async (event) => {
				if (event.event === "pgconductor.invoke") {
					executionOrder.push(event.payload.value);
				}
			},
		);

		// Initialize schema
		await conductor.ensureInstalled();

		// Queue tasks
		await conductor.invoke({ name: "test-task" }, { value: 1 });
		await conductor.invoke({ name: "test-task" }, { value: 2 });
		await conductor.invoke({ name: "test-task" }, { value: 3 });
		await conductor.invoke({ name: "test-task" }, { value: 4 });
		await conductor.invoke({ name: "test-task" }, { value: 5 });

		// Process with drain
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [testTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		const startTime = Date.now();
		await orchestrator.drain();
		const duration = Date.now() - startTime;

		// Should have processed all 5 tasks
		expect(executionOrder).toHaveLength(5);
		expect(executionOrder).toContain(1);
		expect(executionOrder).toContain(2);
		expect(executionOrder).toContain(3);
		expect(executionOrder).toContain(4);
		expect(executionOrder).toContain(5);

		// Verify all executions are completed in database
		const completedExecutions = await db.sql`
			SELECT * FROM pgconductor.executions
			WHERE task_key = 'test-task' AND completed_at IS NOT NULL
		`;
		expect(completedExecutions.length).toBe(5);

		// Should stop quickly (not keep polling)
		// With 100ms poll interval, if it kept polling it would take much longer
		expect(duration).toBeLessThan(5000);
	}, 30000);

	test("orchestrator.drain() handles empty queue immediately", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "empty-task",
		});

		const executionCount = mock(() => {});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const emptyTask = conductor.createTask(
			{ name: "empty-task" },
			{ invocable: true },
			async () => {
				executionCount();
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [emptyTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		const startTime = Date.now();
		await orchestrator.drain();
		const duration = Date.now() - startTime;

		// Should not have executed any tasks
		expect(executionCount.mock.calls.length).toBe(0);

		// Should complete very quickly (no polling)
		expect(duration).toBeLessThan(2000);
	}, 30000);

	test("orchestrator.drain() processes tasks added during execution by running multiple times", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "chained-task",
			payload: z.object({ depth: z.number() }),
		});

		const executionOrder: number[] = [];

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const chainedTask = conductor.createTask(
			{ name: "chained-task" },
			{ invocable: true },
			async (event) => {
				if (event.event === "pgconductor.invoke") {
					const depth = event.payload.depth;
					executionOrder.push(depth);

					// Chain another task if depth < 3
					if (depth < 3) {
						await conductor.invoke({ name: "chained-task" }, { depth: depth + 1 });
					}
				}
			},
		);

		// Initialize schema
		await conductor.ensureInstalled();

		// Start with one task
		await conductor.invoke({ name: "chained-task" }, { depth: 1 });

		// Run multiple times to process chained tasks
		// Each run processes one level of the chain
		for (let i = 0; i < 3; i++) {
			const orch = Orchestrator.create({
				conductor,
				tasks: [chainedTask],
				defaultWorker: {
					pollIntervalMs: 100,
					flushIntervalMs: 100,
				},
			});
			await orch.drain();
		}

		// Should have processed all chained tasks (1 → 2 → 3)
		expect(executionOrder).toEqual([1, 2, 3]);
	}, 30000);

	test("orchestrator.drain() stops after processing without needing explicit stop call", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "auto-stop-task",
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const autoStopTask = conductor.createTask(
			{ name: "auto-stop-task" },
			{ invocable: true },
			async () => {
				// Simple task
			},
		);

		// Initialize schema
		await conductor.ensureInstalled();

		await conductor.invoke({ name: "auto-stop-task" }, {});

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [autoStopTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		// drain() should return when work is done
		const drainPromise = orchestrator.drain();

		// Should resolve without needing stop()
		await expect(drainPromise).resolves.toBeUndefined();
	}, 30000);

	test("orchestrator.drain() processes multiple batches", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "batch-task",
			payload: z.object({ id: z.number() }),
		});

		const executedIds: number[] = [];

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const batchTask = conductor.createTask(
			{ name: "batch-task" },
			{ invocable: true },
			async (event) => {
				if (event.event === "pgconductor.invoke") {
					executedIds.push(event.payload.id);
				}
			},
		);

		// Initialize schema
		await conductor.ensureInstalled();

		// Queue up many tasks (more than one batch)
		// Assuming batch size is around 10-20, let's add 50
		for (let i = 1; i <= 50; i++) {
			await conductor.invoke({ name: "batch-task" }, { id: i });
		}

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [batchTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
				fetchBatchSize: 10, // Small batch size to ensure multiple batches
			},
		});

		await orchestrator.drain();

		// All 50 tasks should be processed
		expect(executedIds).toHaveLength(50);
		expect(executedIds.sort((a, b) => a - b)).toEqual(Array.from({ length: 50 }, (_, i) => i + 1));
	}, 30000);

	test("normal orchestrator start continues running until stopped", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "continuous-task",
		});

		const executionCount = mock(() => {});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const continuousTask = conductor.createTask(
			{ name: "continuous-task" },
			{ invocable: true },
			async () => {
				executionCount();
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [continuousTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		// Start without runOnce
		await orchestrator.start();

		// Wait a bit
		await new Promise((r) => setTimeout(r, 500));

		// Orchestrator should still be running
		expect(orchestrator.info.isRunning).toBe(true);

		// Add a task
		await conductor.invoke({ name: "continuous-task" }, {});

		// Wait for execution
		await new Promise((r) => setTimeout(r, 500));

		expect(executionCount.mock.calls.length).toBeGreaterThanOrEqual(1);

		// Must explicitly stop
		await orchestrator.stop();
		await orchestrator.stopped;
	}, 30000);
});
