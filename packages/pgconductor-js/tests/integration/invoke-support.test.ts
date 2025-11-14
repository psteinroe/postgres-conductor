import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";

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
		});

		const childDefinition = defineTask({
			name: "child-task",
			payload: z.object({ input: z.number() }),
		});

		const tasks = [parentDefinition, childDefinition] as const;

		const childFn = mock((n: number) => n * 2);

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const childTask = conductor.createTask(
			"child-task",
			async (payload, _ctx) => {
				const result = childFn(payload.input);
				return { output: result };
			},
			{ flushInterval: 100, pollInterval: 100 },
		);

		const parentTask = conductor.createTask(
			"parent-task",
			async (payload, ctx) => {
				const childResult = await ctx.invoke<{ output: number }>(
					"invoke-child",
					"child-task",
					{ input: payload.value },
				);
				return { result: childResult.output };
			},
			{ flushInterval: 100, pollInterval: 100 },
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [parentTask, childTask],
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		await conductor.invoke("parent-task", { value: 5 });

		await new Promise((r) => setTimeout(r, 1500));

		await orchestrator.stop();
		await stoppedPromise;

		// Child function should have been called
		expect(childFn).toHaveBeenCalledTimes(1);
		expect(childFn).toHaveBeenCalledWith(5);
	}, 30000);

	test("invoke() with timeout throws error", async () => {
		const db = await pool.child();

		const parentDefinition = defineTask({
			name: "timeout-parent",
			payload: z.object({}),
		});

		const childDefinition = defineTask({
			name: "slow-child",
			payload: z.object({}),
		});

		const tasks = [parentDefinition, childDefinition] as const;

		const parentError = mock((err: Error) => err.message);

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const slowChildTask = conductor.createTask(
			"slow-child",
			async (_payload, ctx) => {
				// Sleep for 5 seconds
				await ctx.sleep("long-sleep", 5000);
				return { completed: true };
			},
			{ flushInterval: 100, pollInterval: 100 },
		);

		const timeoutParentTask = conductor.createTask(
			"timeout-parent",
			async (_payload, ctx) => {
				try {
					await ctx.invoke(
						"invoke-slow",
						"slow-child",
						{},
						1000, // 1 second timeout
					);
					return { success: true };
				} catch (err) {
					parentError(err as Error);
					throw err;
				}
			},
			{ flushInterval: 100, pollInterval: 100 },
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [timeoutParentTask, slowChildTask],
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		// Set fake time
		const startTime = new Date("2024-01-01T00:00:00Z");
		await db.sql`
			INSERT INTO pgconductor.test_config (key, value)
			VALUES ('fake_now', ${startTime.toISOString()})
			ON CONFLICT (key) DO UPDATE SET value = ${startTime.toISOString()}
		`;

		await conductor.invoke("timeout-parent", {});

		// Wait for parent to invoke child and child to sleep
		await new Promise((r) => setTimeout(r, 500));

		// Advance time past timeout (1 second) but before sleep completes (5 seconds)
		const afterTimeout = new Date(startTime.getTime() + 1500);
		await db.sql`
			UPDATE pgconductor.test_config
			SET value = ${afterTimeout.toISOString()}
			WHERE key = 'fake_now'
		`;

		// Wait for timeout to be detected and parent to resume
		await new Promise((r) => setTimeout(r, 500));

		await orchestrator.stop();
		await stoppedPromise;

		// Clean up
		await db.sql`DELETE FROM pgconductor.test_config WHERE key = 'fake_now'`;

		// Parent should have caught timeout error
		expect(parentError).toHaveBeenCalledTimes(1);
		const errorMsg = parentError.mock.results[0]?.value;
		expect(errorMsg).toContain("timed out after 1000ms");
	}, 5000);

	test("invoke() caches child result on retry", async () => {
		const db = await pool.child();

		const parentDefinition = defineTask({
			name: "retry-parent",
			payload: z.object({ value: z.number() }),
		});

		const childDefinition = defineTask({
			name: "once-child",
			payload: z.object({ input: z.number() }),
		});

		const tasks = [parentDefinition, childDefinition] as const;

		const childFn = mock((n: number) => n * 3);

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const onceChildTask = conductor.createTask(
			"once-child",
			async (payload, _ctx) => {
				const result = childFn(payload.input);
				return { output: result };
			},
			{ flushInterval: 100 },
		);

		let parentAttempts = 0;
		const retryParentTask = conductor.createTask(
			"retry-parent",
			async (payload, ctx) => {
				parentAttempts++;
				const childResult = await ctx.invoke<{ output: number }>(
					"invoke-once",
					"once-child",
					{ input: payload.value },
				);

				// Fail on first attempt, succeed on retry
				if (parentAttempts === 1) {
					throw new Error("First attempt fails");
				}

				return { result: childResult.output };
			},
			{ flushInterval: 100, maxAttempts: 3 },
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [retryParentTask, onceChildTask],
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		await conductor.invoke("retry-parent", { value: 7 });

		// Wait for retry to complete
		await new Promise((r) => setTimeout(r, 5000));

		await orchestrator.stop();
		await stoppedPromise;

		// Child should only be called once (cached on parent retry)
		expect(childFn).toHaveBeenCalledTimes(1);
		expect(childFn).toHaveBeenCalledWith(7);
	}, 30000);

	test("invoke() without timeout waits indefinitely", async () => {
		const db = await pool.child();

		const parentDefinition = defineTask({
			name: "patient-parent",
			payload: z.object({}),
		});

		const childDefinition = defineTask({
			name: "eventual-child",
			payload: z.object({}),
		});

		const tasks = [parentDefinition, childDefinition] as const;

		const childFn = mock(() => "completed");

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const eventualChildTask = conductor.createTask(
			"eventual-child",
			async (_payload, ctx) => {
				// Sleep for 2 seconds
				await ctx.sleep("moderate-sleep", 2000);
				const result = childFn();
				return { result };
			},
			{ flushInterval: 100 },
		);

		const patientParentTask = conductor.createTask(
			"patient-parent",
			async (_payload, ctx) => {
				// No timeout - will wait forever
				const childResult = await ctx.invoke<{ result: string }>(
					"invoke-eventual",
					"eventual-child",
					{},
				);
				return { childResult: childResult.result };
			},
			{ flushInterval: 100 },
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [patientParentTask, eventualChildTask],
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		await conductor.invoke("patient-parent", {});

		// Wait for child to complete (2s sleep + parent resume + buffer)
		await new Promise((r) => setTimeout(r, 6000));

		await orchestrator.stop();
		await stoppedPromise;

		// Child should have completed successfully
		expect(childFn).toHaveBeenCalledTimes(1);
	}, 40000);

	test("invoke() cascade failure when child fails permanently", async () => {
		const db = await pool.child();

		const parentDefinition = defineTask({
			name: "cascade-parent",
			payload: z.object({}),
		});

		const childDefinition = defineTask({
			name: "failing-child",
			payload: z.object({}),
		});

		const tasks = [parentDefinition, childDefinition] as const;

		const childFn = mock(() => {
			throw new Error("Child always fails");
		});

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const failingChildTask = conductor.createTask(
			"failing-child",
			async (_payload, _ctx) => {
				childFn();
				return { result: "never-reached" };
			},
			{ flushInterval: 100, pollInterval: 100, maxAttempts: 2 },
		);

		const cascadeParentTask = conductor.createTask(
			"cascade-parent",
			async (_payload, ctx) => {
				await ctx.invoke("invoke-failing", "failing-child", {});
				return { success: true };
			},
			{ flushInterval: 100, pollInterval: 100 },
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [cascadeParentTask, failingChildTask],
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		// Set initial fake time
		const startTime = new Date("2024-01-01T00:00:00Z");
		await db.sql`
			INSERT INTO pgconductor.test_config (key, value)
			VALUES ('fake_now', ${startTime.toISOString()})
			ON CONFLICT (key) DO UPDATE SET value = ${startTime.toISOString()}
		`;

		await conductor.invoke("cascade-parent", {});

		// Wait for first execution cycle: parent runs → invokes child → child fails
		await new Promise((r) => setTimeout(r, 500));
		expect(childFn).toHaveBeenCalledTimes(1);

		// Advance fake time past first backoff (15 seconds)
		const afterFirstBackoff = new Date(startTime.getTime() + 15000);
		await db.sql`
			UPDATE pgconductor.test_config
			SET value = ${afterFirstBackoff.toISOString()}
			WHERE key = 'fake_now'
		`;

		// Wait for second execution cycle: child retries and fails permanently
		// Cascade should happen immediately in the same flush
		await new Promise((r) => setTimeout(r, 500));

		await orchestrator.stop();
		await stoppedPromise;

		// Clean up
		await db.sql`DELETE FROM pgconductor.test_config WHERE key = 'fake_now'`;

		// Child should have been called maxAttempts times
		expect(childFn).toHaveBeenCalledTimes(2);

		// Verify parent was moved to failed_executions with cascade error
		const failedParents = await db.sql<
			Array<{ last_error: string; task_key: string }>
		>`
			SELECT last_error, task_key
			FROM pgconductor.failed_executions
			WHERE task_key = 'cascade-parent'
		`;

		expect(failedParents.length).toBe(1);
		expect(failedParents[0]?.last_error).toContain("Child execution failed");
	}, 5000);
});
