import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import { waitFor } from "../../src/lib/wait-for";
import { TaskSchemas } from "../../src/schemas";

describe("Cron Scheduling", () => {
	let pool: TestDatabasePool;

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterAll(async () => {
		await pool?.destroy();
	});

	test("cron schedule is created on worker startup", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "daily-report",
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const reportTask = conductor.createTask(
			{ name: "daily-report" },
			[{ invocable: true }, { cron: "0 0 9 * * *", name: "daily-9am" }], // Every day at 9am
			async () => {},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [reportTask],
		});

		await orchestrator.start();
		await waitFor(200);

		// Check that cron schedule was created
		const schedules = await db.sql<Array<{ dedupe_key: string; run_at: Date }>>`
			SELECT dedupe_key, run_at
			FROM pgconductor._private_executions
			WHERE task_key = 'daily-report'
				AND dedupe_key LIKE 'scheduled::%'
		`;

		expect(schedules.length).toBe(1);
		expect(schedules[0]?.dedupe_key).toMatch(/^scheduled::daily-9am::\d+$/);

		// Verify run_at is in the future and at 9 AM UTC
		const runAt = new Date(schedules[0]!.run_at);
		expect(runAt.getTime()).toBeGreaterThan(Date.now());
		expect(runAt.getUTCHours()).toBe(9);
		expect(runAt.getUTCMinutes()).toBe(0);
		expect(runAt.getUTCSeconds()).toBe(0);

		await orchestrator.stop();
		await db.destroy();
	}, 30000);

	test("cron execution schedules next occurrence", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "frequent-sync",
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const executions = mock(() => {});

		const syncTask = conductor.createTask(
			{ name: "frequent-sync" },
			[{ invocable: true }, { cron: "*/3 * * * * *", name: "every-3sec" }], // Every 3 seconds
			async () => {
				executions();
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [syncTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// Wait for first execution
		await waitFor(4000);
		expect(executions.mock.calls.length).toBeGreaterThanOrEqual(1);

		// Check that next execution is scheduled
		const schedules = await db.sql<Array<{ dedupe_key: string; run_at: Date }>>`
			SELECT dedupe_key, run_at
			FROM pgconductor._private_executions
			WHERE task_key = 'frequent-sync'
				AND dedupe_key LIKE 'scheduled::%'
				AND run_at > pgconductor._private_current_time()
			ORDER BY run_at
			LIMIT 1
		`;

		expect(schedules.length).toBe(1);

		// Wait for second execution
		await waitFor(4000);
		expect(executions.mock.calls.length).toBeGreaterThanOrEqual(2);

		await orchestrator.stop();
		await db.destroy();
	}, 30000);

	test("multiple cron triggers for same task", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "multi-schedule",
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const executions = mock(() => {});

		const multiTask = conductor.createTask(
			{ name: "multi-schedule" },
			[
				{ invocable: true },
				{ cron: "0 0 9 * * *", name: "daily-9am" }, // 9 AM daily
				{ cron: "0 0 17 * * *", name: "daily-5pm" }, // 5 PM daily
			],
			async () => {
				executions();
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [multiTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		// Check that both schedules are created
		await orchestrator.start();
		await waitFor(200);

		const schedules = await db.sql<Array<{ dedupe_key: string; run_at: Date }>>`
			SELECT dedupe_key, run_at
			FROM pgconductor._private_executions
			WHERE task_key = 'multi-schedule'
				AND dedupe_key LIKE 'scheduled::%'
			ORDER BY run_at
		`;

		expect(schedules.length).toBe(2);

		// Check both cron expressions are present (order may vary)
		const dedupeKeys = schedules.map((s) => s.dedupe_key);
		const has9am = dedupeKeys.some((key) => key.startsWith("scheduled::daily-9am::"));
		const has5pm = dedupeKeys.some((key) => key.startsWith("scheduled::daily-5pm::"));

		expect(has9am).toBe(true);
		expect(has5pm).toBe(true);

		await orchestrator.stop();
		await db.destroy();
	}, 30000);

	test("removing cron trigger cleans up stale schedules", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "cleanup-test",
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		// First worker with two cron schedules
		const task1 = conductor.createTask(
			{ name: "cleanup-test" },
			[
				{ invocable: true },
				{ cron: "0 0 9 * * *", name: "morning" }, // 9 AM
				{ cron: "0 0 17 * * *", name: "evening" }, // 5 PM
			],
			async () => {},
		);

		let orchestrator = Orchestrator.create({
			conductor,
			tasks: [task1],
		});

		await orchestrator.start();
		await waitFor(200);
		await orchestrator.stop();

		// Verify 2 schedules exist (only check new format with cron_expression)
		let schedules = await db.sql<Array<{ dedupe_key: string; cron_expression: string | null }>>`
			SELECT dedupe_key, cron_expression
			FROM pgconductor._private_executions
			WHERE task_key = 'cleanup-test'
		`;
		const withCronExpression = schedules.filter((s) => s.cron_expression !== null);
		expect(withCronExpression.length).toBe(2);

		// Second worker with only one cron schedule
		const task2 = conductor.createTask(
			{ name: "cleanup-test" },
			[{ invocable: true }, { cron: "0 0 9 * * *", name: "morning" }], // Only 9 AM
			async () => {},
		);

		orchestrator = Orchestrator.create({
			conductor,
			tasks: [task2],
		});

		await orchestrator.start();
		await waitFor(200);
		await orchestrator.stop();

		// Verify only 1 schedule remains (5 PM should be cleaned up)
		const remainingSchedules = await db.sql<Array<{ dedupe_key: string }>>`
			SELECT dedupe_key
			FROM pgconductor._private_executions
			WHERE task_key = 'cleanup-test'
				AND cron_expression IS NOT NULL
		`;
		expect(remainingSchedules.length).toBe(1);
		expect(remainingSchedules[0]?.dedupe_key).toMatch(/^scheduled::morning::\d+$/);

		await db.destroy();
	}, 30000);

	test("cron task with invocable trigger works", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "hybrid-task",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const executions = mock((payload: { value: number }) => payload.value);

		const hybridTask = conductor.createTask(
			{ name: "hybrid-task" },
			[
				{ invocable: true },
				{ cron: "0 0 12 * * *", name: "daily-noon" }, // Noon daily
			],
			async (event) => {
				if (event.name === "pgconductor.invoke") {
					executions(event.payload);
				}
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [hybridTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// Manual invocation should work
		await conductor.invoke({ name: "hybrid-task" }, { value: 42 });
		await waitFor(500);

		expect(executions).toHaveBeenCalledTimes(1);
		expect(executions).toHaveBeenCalledWith({ value: 42 });

		await orchestrator.stop();
		await db.destroy();
	}, 30000);

	test("task context can dynamically schedule cron executions", async () => {
		const db = await pool.child();

		const schedulerDefinition = defineTask({
			name: "dynamic-scheduler",
		});

		const dynamicTargetDefinition = defineTask({
			name: "dynamic-target",
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([schedulerDefinition, dynamicTargetDefinition]),
			context: {},
		});

		const targetExecutions = mock(() => {});

		const dynamicTask = conductor.createTask(
			{ name: "dynamic-target" },
			{ invocable: true },
			async () => {
				targetExecutions();
			},
		);

		const schedulerTask = conductor.createTask(
			{ name: "dynamic-scheduler" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.schedule(
					{ name: "dynamic-target" },
					"reporting",
					{ cron: "*/2 * * * * *" },
					{ source: "dynamic" },
				);
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [dynamicTask, schedulerTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();
		await conductor.invoke({ name: "dynamic-scheduler" }, {});
		await waitFor(4000);
		expect(targetExecutions.mock.calls.length).toBeGreaterThanOrEqual(1);

		const nextSchedules = await db.sql<Array<{ dedupe_key: string }>>`
			SELECT dedupe_key
			FROM pgconductor._private_executions
			WHERE task_key = 'dynamic-target'
				AND cron_expression IS NOT NULL
				AND run_at > pgconductor._private_current_time()
			ORDER BY run_at
			LIMIT 1
		`;

		expect(nextSchedules.length).toBe(1);
		expect(nextSchedules[0]!.dedupe_key).toMatch(/^scheduled::reporting::\d+$/);

		await orchestrator.stop();
		await db.destroy();
	}, 40000);

	test("task context can unschedule dynamic cron executions", async () => {
		const db = await pool.child();

		const schedulerDefinition = defineTask({
			name: "dynamic-scheduler",
		});

		const unschedulerDefinition = defineTask({
			name: "dynamic-unscheduler",
		});

		const dynamicTargetDefinition = defineTask({
			name: "dynamic-target",
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				schedulerDefinition,
				unschedulerDefinition,
				dynamicTargetDefinition,
			]),
			context: {},
		});

		const targetExecutions = mock(() => {});

		const dynamicTask = conductor.createTask(
			{ name: "dynamic-target" },
			{ invocable: true },
			async () => {
				targetExecutions();
			},
		);

		const schedulerTask = conductor.createTask(
			{ name: "dynamic-scheduler" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.schedule({ name: "dynamic-target" }, "reporting", { cron: "*/2 * * * * *" }, {});
			},
		);

		const unschedulerTask = conductor.createTask(
			{ name: "dynamic-unscheduler" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.unschedule({ name: "dynamic-target" }, "reporting");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [dynamicTask, schedulerTask, unschedulerTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();
		await conductor.invoke({ name: "dynamic-scheduler" }, {});
		await waitFor(4000);
		expect(targetExecutions.mock.calls.length).toBeGreaterThanOrEqual(1);

		const runsBeforeUnschedule = targetExecutions.mock.calls.length;
		await conductor.invoke({ name: "dynamic-unscheduler" }, {});
		await waitFor(200);

		const futureSchedules = await db.sql<Array<{ id: string }>>`
			SELECT id
			FROM pgconductor._private_executions
			WHERE task_key = 'dynamic-target'
				AND cron_expression IS NOT NULL
				AND run_at > pgconductor._private_current_time()
		`;

		expect(futureSchedules.length).toBe(0);

		await waitFor(3000);
		expect(targetExecutions.mock.calls.length).toBe(runsBeforeUnschedule);

		await orchestrator.stop();
		await db.destroy();
	}, 40000);

	// Note: This test has inherent timing/race conditions with real DB and workers.
	// A deterministic unit test version exists in tests/unit/in-memory-database-client.test.ts
	// that uses the in-memory client to test the exact same scenario without race conditions.
	test.skip("unschedule cancels running execution and prevents re-insert on retry", async () => {
		const db = await pool.child();

		const schedulerDefinition = defineTask({
			name: "schedule-slow-task",
		});

		const unschedulerDefinition = defineTask({
			name: "unschedule-slow-task",
		});

		const slowTaskDefinition = defineTask({
			name: "slow-task",
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				schedulerDefinition,
				unschedulerDefinition,
				slowTaskDefinition,
			]),
			context: {},
		});

		let executionStarted = false;
		const slowTask = conductor.createTask(
			{ name: "slow-task", maxAttempts: 3 },
			{ invocable: true },
			async (_event, ctx) => {
				executionStarted = true;
				// Sleep for a while to allow unschedule during execution
				await new Promise((r) => setTimeout(r, 2000));
				// Check if we were cancelled
				if (ctx.signal.aborted) {
					throw new Error("Task was cancelled");
				}
			},
		);

		const schedulerTask = conductor.createTask(
			{ name: "schedule-slow-task" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.schedule(
					{ name: "slow-task" },
					"slow-schedule",
					{ cron: "*/2 * * * * *" }, // Every 2 seconds
				);
			},
		);

		const unschedulerTask = conductor.createTask(
			{ name: "unschedule-slow-task" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.unschedule({ name: "slow-task" }, "slow-schedule");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [slowTask, schedulerTask, unschedulerTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// 1. Schedule the slow task
		await conductor.invoke({ name: "schedule-slow-task" }, {});
		await waitFor(500);

		// Wait for execution to start
		const startTime = Date.now();
		while (!executionStarted && Date.now() - startTime < 5000) {
			await waitFor(100);
		}
		expect(executionStarted).toBe(true);

		// 2. Unschedule while task is running
		await conductor.invoke({ name: "unschedule-slow-task" }, {});
		await waitFor(500);

		// 3. Wait for task to fail due to cancellation
		await waitFor(3000);

		// 4. Check that no future executions exist (even after retry)
		const futureSchedules = await db.sql<Array<{ id: string }>>`
			SELECT id
			FROM pgconductor._private_executions
			WHERE task_key = 'slow-task'
				AND cron_expression IS NOT NULL
				AND run_at > pgconductor._private_current_time()
		`;

		expect(futureSchedules.length).toBe(0);

		await orchestrator.stop();
		await db.destroy();
	}, 30000);

	test("cron with second-level precision executes frequently", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "frequent-task",
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const executions = mock(() => {});

		const frequentTask = conductor.createTask(
			{ name: "frequent-task" },
			[{ invocable: true }, { cron: "*/2 * * * * *", name: "every-2sec" }], // Every 2 seconds
			async () => {
				executions();
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [frequentTask],
			defaultWorker: {
				pollIntervalMs: 50,
				flushIntervalMs: 50,
			},
		});

		await orchestrator.start();

		// Wait for at least 2 executions (2 seconds + 2 seconds + extra buffer for processing)
		// First execution at ~2s, second at ~4s, wait 6.5s to be safe
		await waitFor(6500);

		expect(executions.mock.calls.length).toBeGreaterThanOrEqual(2);

		await orchestrator.stop();
		await db.destroy();
	}, 30000);

	test("cron task with failure still reschedules next execution", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "flaky-cron",
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		let attemptCount = 0;
		const executions = mock(() => {
			attemptCount++;
			// Fail first attempt, succeed on subsequent
			if (attemptCount === 1) {
				throw new Error("Task fails");
			}
		});

		const flakyTask = conductor.createTask(
			{ name: "flaky-cron", maxAttempts: 5 },
			[{ invocable: true }, { cron: "*/2 * * * * *", name: "every-2sec" }], // Every 2 seconds
			async () => {
				executions();
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [flakyTask],
			defaultWorker: {
				pollIntervalMs: 50,
				flushIntervalMs: 50,
			},
		});

		await orchestrator.start();

		// Wait for first execution and retry (2s + 15s backoff + buffer)
		await waitFor(18000);

		// Should have at least attempted twice (fail + success)
		expect(attemptCount).toBeGreaterThanOrEqual(2);

		// Verify next cron execution is scheduled
		const nextExecution = await db.sql<Array<{ run_at: Date; dedupe_key: string }>>`
			SELECT run_at, dedupe_key
			FROM pgconductor._private_executions
			WHERE task_key = 'flaky-cron'
				AND dedupe_key LIKE 'scheduled::%'
				AND run_at > pgconductor._private_current_time()
			ORDER BY run_at
			LIMIT 1
		`;

		expect(nextExecution.length).toBe(1);
		expect(nextExecution[0]?.dedupe_key).toMatch(/^scheduled::.*::\d+$/);

		await orchestrator.stop();
		await db.destroy();
	}, 30000);

	test("dedupe prevents duplicate cron executions", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "dedupe-cron",
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const executions = mock(() => {});

		const cronTask = conductor.createTask(
			{ name: "dedupe-cron" },
			[{ invocable: true }, { cron: "0 0 14 * * *", name: "daily-2pm" }], // 2 PM daily
			async () => {
				executions();
			},
		);

		// Start two orchestrators with same cron
		const orchestrator1 = Orchestrator.create({
			conductor,
			tasks: [cronTask],
		});

		const orchestrator2 = Orchestrator.create({
			conductor,
			tasks: [cronTask],
		});

		await orchestrator1.start();
		await orchestrator2.start();

		await waitFor(500);

		// Check database - should only have one scheduled execution
		const schedules = await db.sql<Array<{ dedupe_key: string }>>`
			SELECT dedupe_key
			FROM pgconductor._private_executions
			WHERE task_key = 'dedupe-cron'
				AND dedupe_key LIKE 'scheduled::%'
		`;

		expect(schedules.length).toBe(1);

		await orchestrator1.stop();
		await orchestrator2.stop();

		// Wait a bit for connections to fully close
		await waitFor(100);

		await db.destroy();
	}, 30000);
});
