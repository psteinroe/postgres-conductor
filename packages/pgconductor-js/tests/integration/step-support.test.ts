import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";

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
			returns: z.object({ result: z.number() }),
		});


		const expensiveFn = mock((n: number) => n * 2);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		const stepTask = conductor.createTask(
			{ name: "step-task" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.event === "pgconductor.invoke") {
					const result = await ctx.step("expensive-step", () => {
						return expensiveFn(event.payload.value);
					});

					return { result };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [stepTask],
		});

		await orchestrator.start();

		await conductor.invoke({ name: "step-task" }, { value: 5 });

		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();

		// Expensive function should only be called once
		expect(expensiveFn).toHaveBeenCalledTimes(1);
		expect(expensiveFn).toHaveBeenCalledWith(5);
	}, 30000);

	test("sleep() pauses execution and resumes", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "sleep-task",
			payload: z.object({ delay: z.number() }),
			returns: z.object({ completed: z.boolean() }),
		});


		const executionSteps = mock((step: string) => step);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		const sleepTask = conductor.createTask(
			{ name: "sleep-task" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.event === "pgconductor.invoke") {
					executionSteps("before-sleep");
					await ctx.sleep("wait", event.payload.delay);
					executionSteps("after-sleep");
					return { completed: true };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [sleepTask],
		});

		await orchestrator.start();

		await conductor.invoke({ name: "sleep-task" }, { delay: 2000 });

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
	}, 30000);

	test("checkpoint() works without crashing", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "checkpoint-task",
			payload: z.object({ items: z.number() }),
			returns: z.object({ processed: z.number() }),
		});


		const processedItems = mock((item: number) => item);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		const checkpointTask = conductor.createTask(
			{ name: "checkpoint-task" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.event === "pgconductor.invoke") {
					for (let i = 0; i < event.payload.items; i++) {
						processedItems(i);
						await ctx.checkpoint();
						// Simulate some work
						await new Promise((r) => setTimeout(r, 50));
					}
					return { processed: event.payload.items };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [checkpointTask],
		});

		await orchestrator.start();

		await conductor.invoke({ name: "checkpoint-task" }, { items: 5 });

		// Wait for task to complete
		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();

		// Should have processed all items
		expect(processedItems).toHaveBeenCalledTimes(5);
	}, 30000);

	test("step() with multiple steps in sequence", async () => {
		const db = await pool.child();

		const taskDefinitions = defineTask({
			name: "multi-step-task",
			payload: z.object({ x: z.number() }),
			returns: z.object({ result: z.number() }),
		});


		const step1Fn = mock((n: number) => n + 1);
		const step2Fn = mock((n: number) => n * 2);
		const step3Fn = mock((n: number) => n - 3);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {},
		});

		const multiStepTask = conductor.createTask(
			{ name: "multi-step-task" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.event === "pgconductor.invoke") {
					const a = await ctx.step("step1", () => step1Fn(event.payload.x));
					const b = await ctx.step("step2", () => step2Fn(a));
					const c = await ctx.step("step3", () => step3Fn(b));
					return { result: c };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [multiStepTask],
		});

		await orchestrator.start();

		await conductor.invoke({ name: "multi-step-task" }, { x: 5 });

		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();

		// All steps should execute once
		expect(step1Fn).toHaveBeenCalledTimes(1);
		expect(step1Fn).toHaveBeenCalledWith(5);

		expect(step2Fn).toHaveBeenCalledTimes(1);
		expect(step2Fn).toHaveBeenCalledWith(6);

		expect(step3Fn).toHaveBeenCalledTimes(1);
		expect(step3Fn).toHaveBeenCalledWith(12);
	}, 30000);
});
