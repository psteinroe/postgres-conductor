import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";

describe("WaitForEvent Support", () => {
	let pool: TestDatabasePool;

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterAll(async () => {
		await pool?.destroy();
	});

	test("waitForEvent creates subscription in database", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "waiter-task",
			payload: z.object({}),
			returns: z.object({ received: z.boolean() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const waiterTask = conductor.createTask(
			{ name: "waiter-task" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.waitForEvent("wait-step", { event: "user.created" });
				return { received: true };
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [waiterTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		await conductor.invoke({ name: "waiter-task" }, {});

		// Wait for task to execute and create subscription
		await new Promise((r) => setTimeout(r, 500));

		// Verify subscription was created
		const subscriptions = await db.sql`
			SELECT * FROM pgconductor.subscriptions
			WHERE event_key = 'user.created'
		`;

		expect(subscriptions.length).toBe(1);
		expect(subscriptions[0]?.source).toBe("event");
		expect(subscriptions[0]?.step_key).toBe("wait-step");

		// Verify execution is waiting (run_at = infinity)
		const executions = await db.sql`
			SELECT * FROM pgconductor.executions
			WHERE task_key = 'waiter-task'
		`;

		expect(executions.length).toBe(1);
		expect(executions[0]?.waiting_step_key).toBe("wait-step");

		await orchestrator.stop();
	}, 10000);

	test("emitEvent inserts event into database", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "emitter-task",
			payload: z.object({ userId: z.number() }),
			returns: z.object({ emitted: z.boolean() }),
		});

		const emittedFn = mock(() => true);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const emitterTask = conductor.createTask(
			{ name: "emitter-task" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.event === "pgconductor.invoke") {
					await (ctx as any).emit({ event: "user.created" }, { userId: event.payload.userId });
					emittedFn();
					return { emitted: true };
				}
				throw new Error("Unexpected event");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [emitterTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		await conductor.invoke({ name: "emitter-task" }, { userId: 123 });

		await new Promise((r) => setTimeout(r, 500));

		await orchestrator.stop();

		expect(emittedFn).toHaveBeenCalledTimes(1);

		// Verify event was created
		const events = await db.sql`
			SELECT * FROM pgconductor.events
			WHERE event_key = 'user.created'
		`;

		expect(events.length).toBe(1);
		expect(events[0]?.payload).toEqual({ userId: 123 });
	}, 10000);

	test("waitForEvent with timeout throws error when event doesn't arrive", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "timeout-waiter",
			payload: z.object({}),
			returns: z.object({ received: z.boolean() }),
		});

		const errorFn = mock((err: Error) => err.message);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const timeoutWaiterTask = conductor.createTask(
			{ name: "timeout-waiter" },
			{ invocable: true },
			async (_event, ctx) => {
				try {
					await ctx.waitForEvent("wait-step", {
						event: "never.arrives",
						timeout: 1000,
					});
					return { received: true };
				} catch (err) {
					errorFn(err as Error);
					throw err;
				}
			},
		);

		const startTime = new Date("2024-01-01T00:00:00Z");
		await db.client.setFakeTime({ date: startTime });

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [timeoutWaiterTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		await conductor.invoke({ name: "timeout-waiter" }, {});

		// Wait for task to start waiting
		await new Promise((r) => setTimeout(r, 500));

		// Advance time past timeout
		const afterTimeout = new Date(startTime.getTime() + 1500);
		await db.client.setFakeTime({ date: afterTimeout });

		// Wait for timeout to be processed
		await new Promise((r) => setTimeout(r, 500));

		await orchestrator.stop();
		await db.client.clearFakeTime();

		expect(errorFn).toHaveBeenCalledTimes(1);
		const errorMsg = errorFn.mock.results[0]?.value;
		expect(errorMsg).toContain("timed out");
	}, 10000);

	test("wake_execution SQL function saves step and wakes execution", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "wake-test",
			payload: z.object({}),
			returns: z.object({ eventData: z.any() }),
		});

		const resultFn = mock((data: any) => data);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const wakeTestTask = conductor.createTask(
			{ name: "wake-test" },
			{ invocable: true },
			async (_event, ctx) => {
				const eventData = await ctx.waitForEvent("wait-for-wake", {
					event: "test.event",
				});
				resultFn(eventData);
				return { eventData };
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [wakeTestTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		await conductor.invoke({ name: "wake-test" }, {});

		// Wait for task to create subscription
		await new Promise((r) => setTimeout(r, 500));

		// Get the execution ID
		const executions = await db.sql<Array<{ id: string }>>`
			SELECT id FROM pgconductor.executions
			WHERE task_key = 'wake-test'
		`;
		const executionId = executions[0]?.id;
		expect(executionId).toBeDefined();

		// Manually call wake_execution (simulating what event-router does)
		await db.sql`
			SELECT pgconductor.wake_execution(
				${executionId!}::uuid,
                'default',
				'wait-for-wake',
				${db.sql.json({ message: "hello from event" })}
			)
		`;

		// Wait for execution to be woken and complete
		await new Promise((r) => setTimeout(r, 500));

		await orchestrator.stop();

		expect(resultFn).toHaveBeenCalledTimes(1);
		expect(resultFn.mock.results[0]?.value).toEqual({
			message: "hello from event",
		});
	}, 10000);

	test("waitForEvent returns cached result on retry", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "retry-waiter",
			payload: z.object({}),
			returns: z.object({ eventData: z.any() }),
		});

		let attempts = 0;
		const attemptFn = mock(() => ++attempts);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const retryWaiterTask = conductor.createTask(
			{ name: "retry-waiter", maxAttempts: 3 },
			{ invocable: true },
			async (_event, ctx) => {
				attemptFn();
				const eventData = await ctx.waitForEvent("cached-wait", {
					event: "test.event",
				});

				// Fail on first attempt after receiving event
				if (attempts === 1) {
					throw new Error("First attempt fails");
				}

				return { eventData };
			},
		);

		const startTime = new Date("2024-01-01T00:00:00Z");
		await db.client.setFakeTime({ date: startTime });

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [retryWaiterTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		await conductor.invoke({ name: "retry-waiter" }, {});

		// Wait for task to create subscription
		await new Promise((r) => setTimeout(r, 500));

		// Get execution ID and wake it
		const executions = await db.sql<Array<{ id: string }>>`
			SELECT id FROM pgconductor.executions
			WHERE task_key = 'retry-waiter'
		`;
		const executionId = executions[0]?.id;
		expect(executionId).toBeDefined();

		await db.sql`
			SELECT pgconductor.wake_execution(
				${executionId!}::uuid,
                'default',
				'cached-wait',
				${db.sql.json({ value: 42 })}
			)
		`;

		// Wait for first attempt to fail
		await new Promise((r) => setTimeout(r, 500));

		// Advance time past backoff
		const afterBackoff = new Date(startTime.getTime() + 15000);
		await db.client.setFakeTime({ date: afterBackoff });

		// Wait for retry
		await new Promise((r) => setTimeout(r, 500));

		await orchestrator.stop();
		await db.client.clearFakeTime();

		// Should have attempted twice (first fails, second succeeds)
		expect(attemptFn).toHaveBeenCalledTimes(2);

		// Verify execution completed successfully
		const completed = await db.sql`
			SELECT completed_at FROM pgconductor.executions
			WHERE task_key = 'retry-waiter'
		`;
		expect(completed[0]?.completed_at).not.toBeNull();
	}, 10000);
});
