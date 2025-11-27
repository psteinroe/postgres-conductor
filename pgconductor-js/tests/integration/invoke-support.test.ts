import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";

describe("Invoke Support", () => {
	let pool: TestDatabasePool;

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterAll(async () => {
		await pool?.destroy();
	});

	test("invoke() calls child task and returns result", async () => {
		const db = await pool.child();

		const parentDefinition = defineTask({
			name: "parent-task",
			payload: z.object({ value: z.number() }),
			returns: z.object({ result: z.number() }),
		});

		const childDefinition = defineTask({
			name: "child-task",
			payload: z.object({ input: z.number() }),
			returns: z.object({ output: z.number() }),
		});

		const childFn = mock((n: number) => n * 2);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const childTask = conductor.createTask(
			{ name: "child-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.event === "pgconductor.invoke") {
					const result = childFn(event.payload.input);
					return { output: result };
				}
				throw new Error("Unexpected event type");
			},
		);

		const parentTask = conductor.createTask(
			{ name: "parent-task" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.event === "pgconductor.invoke") {
					const childResult = await ctx.invoke(
						"invoke-child",
						{ name: "child-task" },
						{ input: event.payload.value },
					);
					return { result: childResult.output };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			conductor,
			tasks: [parentTask, childTask],
		});

		await orchestrator.start();

		await conductor.invoke({ name: "parent-task" }, { value: 5 });

		await new Promise((r) => setTimeout(r, 300));

		await orchestrator.stop();

		expect(childFn).toHaveBeenCalledTimes(1);
		expect(childFn).toHaveBeenCalledWith(5);
	}, 30000);

	test("invoke() with timeout throws error", async () => {
		const db = await pool.child();

		const parentDefinition = defineTask({
			name: "timeout-parent",
			payload: z.object({}),
			returns: z.object({ success: z.boolean() }),
		});

		const childDefinition = defineTask({
			name: "slow-child",
			payload: z.object({}),
			returns: z.object({ completed: z.boolean() }),
		});

		const parentError = mock((err: Error) => err.message);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const slowChildTask = conductor.createTask(
			{ name: "slow-child" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.sleep("long-sleep", 5000);
				return { completed: true };
			},
		);

		const timeoutParentTask = conductor.createTask(
			{ name: "timeout-parent" },
			{ invocable: true },
			async (_event, ctx) => {
				try {
					await ctx.invoke("invoke-slow", { name: "slow-child" }, {}, 1000);
					return { success: true };
				} catch (err) {
					parentError(err as Error);
					throw err;
				}
			},
		);

		const startTime = new Date("2024-01-01T00:00:00Z");
		await db.client.setFakeTime({ date: startTime });

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [timeoutParentTask, slowChildTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		await conductor.invoke({ name: "timeout-parent" }, {});

		await new Promise((r) => setTimeout(r, 300));

		// Advance time past timeout (1 second) but before sleep completes (5 seconds)
		const afterTimeout = new Date(startTime.getTime() + 1500);
		await db.client.setFakeTime({ date: afterTimeout });

		await new Promise((r) => setTimeout(r, 300));

		await orchestrator.stop();

		await db.client.clearFakeTime();

		expect(parentError).toHaveBeenCalledTimes(1);
		const errorMsg = parentError.mock.results[0]?.value;
		expect(errorMsg).toContain("timed out after 1000ms");
	}, 5000);

	test("invoke() caches child result on retry", async () => {
		const db = await pool.child();

		const parentDefinition = defineTask({
			name: "retry-parent",
			payload: z.object({ value: z.number() }),
			returns: z.object({ result: z.number() }),
		});

		const childDefinition = defineTask({
			name: "once-child",
			payload: z.object({ input: z.number() }),
			returns: z.object({ output: z.number() }),
		});

		const childFn = mock((n: number) => n * 3);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const onceChildTask = conductor.createTask(
			{ name: "once-child" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.event === "pgconductor.invoke") {
					const result = childFn(event.payload.input);
					return { output: result };
				}
				throw new Error("Unexpected event type");
			},
		);

		let parentAttempts = 0;
		const retryParentTask = conductor.createTask(
			{ name: "retry-parent", maxAttempts: 3 },
			{ invocable: true },
			async (event, ctx) => {
				if (event.event === "pgconductor.invoke") {
					parentAttempts++;
					const childResult = await ctx.invoke(
						"invoke-once",
						{ name: "once-child" },
						{ input: event.payload.value },
					);

					// Fail on first attempt, succeed on retry
					if (parentAttempts === 1) {
						throw new Error("First attempt fails");
					}

					return { result: childResult.output };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			conductor,
			tasks: [retryParentTask, onceChildTask],
		});

		await orchestrator.start();

		await conductor.invoke({ name: "retry-parent" }, { value: 7 });

		await new Promise((r) => setTimeout(r, 500));

		await orchestrator.stop();

		expect(childFn).toHaveBeenCalledTimes(1);
		expect(childFn).toHaveBeenCalledWith(7);
	}, 30000);

	test("invoke() without timeout waits indefinitely", async () => {
		const db = await pool.child();

		const parentDefinition = defineTask({
			name: "patient-parent",
			payload: z.object({}),
			returns: z.object({ childResult: z.string() }),
		});

		const childDefinition = defineTask({
			name: "eventual-child",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const childFn = mock(() => "completed");

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const eventualChildTask = conductor.createTask(
			{ name: "eventual-child" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.sleep("moderate-sleep", 2000);
				const result = childFn();
				return { result };
			},
		);

		const patientParentTask = conductor.createTask(
			{ name: "patient-parent" },
			{ invocable: true },
			async (_event, ctx) => {
				const childResult = await ctx.invoke("invoke-eventual", { name: "eventual-child" }, {});
				return { childResult: childResult.result };
			},
		);

		const orchestrator = Orchestrator.create({
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			conductor,
			tasks: [patientParentTask, eventualChildTask],
		});

		await orchestrator.start();

		await conductor.invoke({ name: "patient-parent" }, {});

		await new Promise((r) => setTimeout(r, 2500));

		await orchestrator.stop();

		expect(childFn).toHaveBeenCalledTimes(1);
	}, 40000);

	test("invoke() cascade failure when child fails permanently", async () => {
		const db = await pool.child();

		const parentDefinition = defineTask({
			name: "cascade-parent",
			payload: z.object({}),
			returns: z.object({ success: z.boolean() }),
		});

		const childDefinition = defineTask({
			name: "failing-child",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const childFn = mock(() => {
			throw new Error("Child always fails");
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const failingChildTask = conductor.createTask(
			{ name: "failing-child", maxAttempts: 2 },
			{ invocable: true },
			async (_event, _ctx) => {
				childFn();
				return { result: "never-reached" };
			},
		);

		const cascadeParentTask = conductor.createTask(
			{ name: "cascade-parent" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.invoke("invoke-failing", { name: "failing-child" }, {});
				return { success: true };
			},
		);

		const startTime = new Date("2024-01-01T00:00:00Z");
		await db.client.setFakeTime({ date: startTime });

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [cascadeParentTask, failingChildTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		await conductor.invoke({ name: "cascade-parent" }, {});

		// Wait for first execution cycle: parent runs → invokes child → child fails
		await new Promise((r) => setTimeout(r, 500));
		expect(childFn).toHaveBeenCalledTimes(1);

		// Advance fake time past first backoff (15 seconds)
		const afterFirstBackoff = new Date(startTime.getTime() + 15000);
		await db.client.setFakeTime({ date: afterFirstBackoff });

		// Wait for second execution cycle: child retries and fails permanently
		await new Promise((r) => setTimeout(r, 500));

		await orchestrator.stop();

		await db.client.clearFakeTime();

		expect(childFn).toHaveBeenCalledTimes(2);

		const failedParents = await db.sql<Array<{ last_error: string; task_key: string }>>`
			SELECT last_error, task_key
			FROM pgconductor.executions
			WHERE task_key = 'cascade-parent' AND failed_at IS NOT NULL
		`;

		expect(failedParents.length).toBe(1);
		expect(failedParents[0]?.last_error).toContain("Child execution failed");
	}, 5000);

	test("invoke() works across queues", async () => {
		const db = await pool.child();

		const parentDefinition = defineTask({
			queue: "parent-queue",
			name: "parent-task",
			payload: z.object({ value: z.number() }),
			returns: z.object({ result: z.number() }),
		});

		const childDefinition = defineTask({
			queue: "child-queue",
			name: "child-task",
			payload: z.object({ input: z.number() }),
			returns: z.object({ output: z.number() }),
		});

		const childFn = mock((n: number) => n * 2);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const parentTask = conductor.createTask(
			{ queue: "parent-queue", name: "parent-task" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.event === "pgconductor.invoke") {
					const childResult = await ctx.invoke(
						"invoke-child",
						{ queue: "child-queue", name: "child-task" },
						{ input: event.payload.value },
					);
					return { result: childResult.output };
				}
				throw new Error("Unexpected event type");
			},
		);

		const parentWorker = conductor.createWorker({
			queue: "parent-queue",
			tasks: [parentTask],
		});

		const childTask = conductor.createTask(
			{ queue: "child-queue", name: "child-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.event === "pgconductor.invoke") {
					const result = childFn(event.payload.input);
					return { output: result };
				}
				throw new Error("Unexpected event type");
			},
		);

		const childWorker = conductor.createWorker({
			queue: "child-queue",
			tasks: [childTask],
		});

		const orchestrator = Orchestrator.create({
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			conductor,
			workers: [parentWorker, childWorker],
		});

		await orchestrator.start();

		await conductor.invoke({ queue: "parent-queue", name: "parent-task" }, { value: 5 });

		await new Promise((r) => setTimeout(r, 4000));

		await orchestrator.stop();

		expect(childFn).toHaveBeenCalledTimes(1);
		expect(childFn).toHaveBeenCalledWith(5);
	}, 30000);

	test("invoke() timeout fails pending child immediately", async () => {
		const db = await pool.child();

		const parentDefinition = defineTask({
			name: "timeout-parent-2",
			payload: z.object({}),
			returns: z.object({ success: z.boolean() }),
		});

		const childDefinition = defineTask({
			name: "slow-child-2",
			payload: z.object({}),
			returns: z.object({ completed: z.boolean() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		// Child that takes 5 seconds
		const slowChildTask = conductor.createTask(
			{ name: "slow-child-2" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.sleep("long-sleep", 5000);
				return { completed: true };
			},
		);

		// Parent with 1 second timeout - let error throw
		const timeoutParentTask = conductor.createTask(
			{ name: "timeout-parent-2" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.invoke("invoke-slow", { name: "slow-child-2" }, {}, 1000);
				return { success: true };
			},
		);

		const startTime = new Date("2024-01-01T00:00:00Z");
		await db.client.setFakeTime({ date: startTime });

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [timeoutParentTask, slowChildTask],
			defaultWorker: {
				pollIntervalMs: 50,
				flushIntervalMs: 50,
			},
		});

		await orchestrator.start();

		await conductor.invoke({ name: "timeout-parent-2" }, {});

		// Wait for parent to invoke child
		await new Promise((r) => setTimeout(r, 200));

		// Advance time past timeout (child still pending due to 50ms intervals)
		const afterTimeout = new Date(startTime.getTime() + 1500);
		await db.client.setFakeTime({ date: afterTimeout });

		// Wait for timeout to be processed
		await new Promise((r) => setTimeout(r, 500));

		await orchestrator.stop();
		await db.client.clearFakeTime();

		// Check that child was failed (not cancelled, since it wasn't locked)
		const children = await db.sql<
			{
				cancelled: boolean;
				failed_at: Date | null;
				last_error: string | null;
			}[]
		>`
			select cancelled, failed_at, last_error
			from pgconductor.executions
			where task_key = 'slow-child-2'
		`;

		expect(children.length).toBe(1);
		expect(children[0]?.cancelled).toBe(false); // Not cancelled since it was pending
		expect(children[0]?.failed_at).not.toBeNull();
		expect(children[0]?.last_error).toContain("parent timed out");
	}, 10000);
});
