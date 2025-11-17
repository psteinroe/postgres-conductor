import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import { waitFor } from "../../src/lib/wait-for";

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

		const tasks = [taskDefinition] as const;

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const reportTask = conductor.createTask(
			{ name: "daily-report" },
			[{ invocable: true }, { cron: "0 0 9 * * *" }] as const, // Every day at 9am
			async () => {},
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [reportTask] as const,
		});

		await orchestrator.start();
		await waitFor(200);

		// Check that cron schedule was created
		const schedules = await db.sql<Array<{ dedupe_key: string; run_at: Date }>>`
			SELECT dedupe_key, run_at
			FROM pgconductor.executions
			WHERE task_key = 'daily-report'
				AND dedupe_key LIKE 'repeated::%'
		`;

		expect(schedules.length).toBe(1);
		expect(schedules[0]?.dedupe_key).toMatch(
			/^repeated::0 0 9 \* \* \*::\d+$/,
		);

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

		const tasks = [taskDefinition] as const;

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const executions = mock(() => {});

		const syncTask = conductor.createTask(
			{ name: "frequent-sync" },
			[{ invocable: true }, { cron: "*/3 * * * * *" }] as const, // Every 3 seconds
			async () => {
				executions();
			},
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [syncTask] as const,
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
			FROM pgconductor.executions
			WHERE task_key = 'frequent-sync'
				AND dedupe_key LIKE 'repeated::%'
				AND run_at > pgconductor.current_time()
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

		const tasks = [taskDefinition] as const;

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const executions = mock(() => {});

		const multiTask = conductor.createTask(
			{ name: "multi-schedule" },
			[
				{ invocable: true },
				{ cron: "0 0 9 * * *" }, // 9 AM daily
				{ cron: "0 0 17 * * *" }, // 5 PM daily
			] as const,
			async () => {
				executions();
			},
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [multiTask] as const,
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		// Check that both schedules are created
		await orchestrator.start();
		await waitFor(200);

		const schedules = await db.sql<
			Array<{ dedupe_key: string; run_at: Date }>
		>`
			SELECT dedupe_key, run_at
			FROM pgconductor.executions
			WHERE task_key = 'multi-schedule'
				AND dedupe_key LIKE 'repeated::%'
			ORDER BY run_at
		`;

		expect(schedules.length).toBe(2);

		// Check both cron expressions are present (order may vary)
		const dedupeKeys = schedules.map((s) => s.dedupe_key);
		const has9am = dedupeKeys.some((key) =>
			key.startsWith("repeated::0 0 9 * * *::"),
		);
		const has5pm = dedupeKeys.some((key) =>
			key.startsWith("repeated::0 0 17 * * *::"),
		);

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

		const tasks = [taskDefinition] as const;

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		// First worker with two cron schedules
		const task1 = conductor.createTask(
			{ name: "cleanup-test" },
			[
				{ invocable: true },
				{ cron: "0 0 9 * * *" }, // 9 AM
				{ cron: "0 0 17 * * *" }, // 5 PM
			] as const,
			async () => {},
		);

		let orchestrator = new Orchestrator({
			conductor,
			tasks: [task1] as const,
		});

		await orchestrator.start();
		await waitFor(200);
		await orchestrator.stop();

		// Verify 2 schedules exist (only check new format with cron_expression)
		let schedules = await db.sql<Array<{ dedupe_key: string; cron_expression: string | null }>>`
			SELECT dedupe_key, cron_expression
			FROM pgconductor.executions
			WHERE task_key = 'cleanup-test'
		`;
		const withCronExpression = schedules.filter(s => s.cron_expression !== null);
		expect(withCronExpression.length).toBe(2);

		// Second worker with only one cron schedule
		const task2 = conductor.createTask(
			{ name: "cleanup-test" },
			[{ invocable: true }, { cron: "0 0 9 * * *" }] as const, // Only 9 AM
			async () => {},
		);

		orchestrator = new Orchestrator({
			conductor,
			tasks: [task2] as const,
		});

		await orchestrator.start();
		await waitFor(200);
		await orchestrator.stop();

		// Verify only 1 schedule remains (5 PM should be cleaned up)
		const remainingSchedules = await db.sql<Array<{ dedupe_key: string }>>`
			SELECT dedupe_key
			FROM pgconductor.executions
			WHERE task_key = 'cleanup-test'
				AND cron_expression IS NOT NULL
		`;
		expect(remainingSchedules.length).toBe(1);
		expect(remainingSchedules[0]?.dedupe_key).toMatch(/^repeated::0 0 9 \* \* \*::\d+$/);

		await db.destroy();
	}, 30000);

	test("cron task with invocable trigger works", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "hybrid-task",
			payload: z.object({ value: z.number() }),
		});

		const tasks = [taskDefinition] as const;

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const executions = mock((payload: { value: number }) => payload.value);

		const hybridTask = conductor.createTask(
			{ name: "hybrid-task" },
			[
				{ invocable: true },
				{ cron: "0 0 12 * * *" }, // Noon daily
			] as const,
			async (event) => {
				if (event.event === "pgconductor.invoke") {
					executions(event.payload);
				}
			},
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [hybridTask] as const,
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// Manual invocation should work
		await conductor.invoke("hybrid-task", { value: 42 });
		await waitFor(500);

		expect(executions).toHaveBeenCalledTimes(1);
		expect(executions).toHaveBeenCalledWith({ value: 42 });

		await orchestrator.stop();
		await db.destroy();
	}, 30000);

	test("cron with second-level precision executes frequently", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "frequent-task",
		});

		const tasks = [taskDefinition] as const;

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const executions = mock(() => {});

		const frequentTask = conductor.createTask(
			{ name: "frequent-task" },
			[{ invocable: true }, { cron: "*/2 * * * * *" }] as const, // Every 2 seconds
			async () => {
				executions();
			},
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [frequentTask] as const,
			defaultWorker: {
				pollIntervalMs: 50,
				flushIntervalMs: 50,
			},
		});

		await orchestrator.start();

		// Wait for at least 2 executions (2 seconds + 2 seconds + buffer)
		await waitFor(5000);

		expect(executions.mock.calls.length).toBeGreaterThanOrEqual(2);

		await orchestrator.stop();
		await db.destroy();
	}, 30000);

	test("cron task with failure still reschedules next execution", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "flaky-cron",
		});

		const tasks = [taskDefinition] as const;

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
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
			[{ invocable: true }, { cron: "*/2 * * * * *" }] as const, // Every 2 seconds
			async () => {
				executions();
			},
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [flakyTask] as const,
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
			FROM pgconductor.executions
			WHERE task_key = 'flaky-cron'
				AND dedupe_key LIKE 'repeated::%'
				AND run_at > pgconductor.current_time()
			ORDER BY run_at
			LIMIT 1
		`;

		expect(nextExecution.length).toBe(1);
		expect(nextExecution[0]?.dedupe_key).toMatch(/^repeated::.*::\d+$/);

		await orchestrator.stop();
		await db.destroy();
	}, 30000);

	test("dedupe prevents duplicate cron executions", async () => {
		const db = await pool.child();

		const taskDefinition = defineTask({
			name: "dedupe-cron",
		});

		const tasks = [taskDefinition] as const;

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {},
		});

		const executions = mock(() => {});

		const cronTask = conductor.createTask(
			{ name: "dedupe-cron" },
			[{ invocable: true }, { cron: "0 0 14 * * *" }] as const, // 2 PM daily
			async () => {
				executions();
			},
		);

		// Start two orchestrators with same cron
		const orchestrator1 = new Orchestrator({
			conductor,
			tasks: [cronTask] as const,
		});

		const orchestrator2 = new Orchestrator({
			conductor,
			tasks: [cronTask] as const,
		});

		await orchestrator1.start();
		await orchestrator2.start();

		await waitFor(500);

		// Check database - should only have one scheduled execution
		const schedules = await db.sql<Array<{ dedupe_key: string }>>`
			SELECT dedupe_key
			FROM pgconductor.executions
			WHERE task_key = 'dedupe-cron'
				AND dedupe_key LIKE 'repeated::%'
		`;

		expect(schedules.length).toBe(1);

		await orchestrator1.stop();
		await orchestrator2.stop();

		// Wait a bit for connections to fully close
		await waitFor(100);

		await db.destroy();
	}, 30000);
});
