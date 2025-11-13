import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";

describe("Step Support", () => {
	let pool: TestDatabasePool;

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterAll(async () => {
		await pool?.destroy();
	});

	test("step() caches results across retries", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "step-task",
			payload: z.object({ value: z.number() }),
		});

		const tasks = [taskDefinitions] as const;

		const expensiveFn = mock((n: number) => n * 2);

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const stepTask = conductor.createTask(
			"step-task",
			async (payload, ctx) => {
				const result = await ctx.step("expensive-step", () => {
					return expensiveFn(payload.value);
				});

				return { result };
			},
			{ flushInterval: 100 },
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [stepTask],
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		await conductor.invoke("step-task", { value: 5 });

		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();
		await stoppedPromise;

		// Expensive function should only be called once
		expect(expensiveFn).toHaveBeenCalledTimes(1);
		expect(expensiveFn).toHaveBeenCalledWith(5);
	}, 30000);

	test("sleep() pauses execution and resumes", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "sleep-task",
			payload: z.object({ delay: z.number() }),
		});

		const tasks = [taskDefinitions] as const;

		const executionSteps = mock((step: string) => step);

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const sleepTask = conductor.createTask(
			"sleep-task",
			async (payload, ctx) => {
				executionSteps("before-sleep");
				await ctx.sleep("wait", payload.delay);
				executionSteps("after-sleep");
				return { completed: true };
			},
			{ flushInterval: 100 },
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [sleepTask],
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		await conductor.invoke("sleep-task", { delay: 2000 });

		// Wait for initial execution (should hit sleep and release)
		await new Promise((r) => setTimeout(r, 1500));

		// At this point, should have seen "before-sleep" but not "after-sleep"
		expect(executionSteps).toHaveBeenCalledWith("before-sleep");
		expect(executionSteps).not.toHaveBeenCalledWith("after-sleep");

		// Wait for sleep to complete and task to resume (2s sleep + buffer)
		await new Promise((r) => setTimeout(r, 3000));

		// Now should see "after-sleep"
		expect(executionSteps).toHaveBeenCalledWith("after-sleep");

		await orchestrator.stop();
		await stoppedPromise;
	}, 30000);

	test("checkpoint() works without crashing", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "checkpoint-task",
			payload: z.object({ items: z.number() }),
		});

		const tasks = [taskDefinitions] as const;

		const processedItems = mock((item: number) => item);

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const checkpointTask = conductor.createTask(
			"checkpoint-task",
			async (payload, ctx) => {
				for (let i = 0; i < payload.items; i++) {
					processedItems(i);
					await ctx.checkpoint();
					// Simulate some work
					await new Promise((r) => setTimeout(r, 50));
				}
				return { processed: payload.items };
			},
			{ flushInterval: 100 },
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [checkpointTask],
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		await conductor.invoke("checkpoint-task", { items: 5 });

		// Wait for task to complete
		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();
		await stoppedPromise;

		// Should have processed all items
		expect(processedItems).toHaveBeenCalledTimes(5);
	}, 30000);

	test("step() with multiple steps in sequence", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "multi-step-task",
			payload: z.object({ x: z.number() }),
		});

		const tasks = [taskDefinitions] as const;

		const step1Fn = mock((n: number) => n + 1);
		const step2Fn = mock((n: number) => n * 2);
		const step3Fn = mock((n: number) => n - 3);

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const multiStepTask = conductor.createTask(
			"multi-step-task",
			async (payload, ctx) => {
				const a = await ctx.step("step1", () => step1Fn(payload.x));
				const b = await ctx.step("step2", () => step2Fn(a));
				const c = await ctx.step("step3", () => step3Fn(b));
				return { result: c };
			},
			{ flushInterval: 100 },
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [multiStepTask],
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		await conductor.invoke("multi-step-task", { x: 5 });

		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();
		await stoppedPromise;

		// All steps should execute once
		expect(step1Fn).toHaveBeenCalledTimes(1);
		expect(step1Fn).toHaveBeenCalledWith(5);

		expect(step2Fn).toHaveBeenCalledTimes(1);
		expect(step2Fn).toHaveBeenCalledWith(6);

		expect(step3Fn).toHaveBeenCalledTimes(1);
		expect(step3Fn).toHaveBeenCalledWith(12);
	}, 30000);
});
