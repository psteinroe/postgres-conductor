import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach } from "vitest";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";
import { createMaintenanceTask } from "../../src/maintenance-task";
import { nextCronOccurrence } from "../../src/lib/cron";

describe("Maintenance Task", () => {
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

	function maintenanceCron(queue: string): string {
		const [trigger] = createMaintenanceTask(queue).triggers;
		if (!("cron" in trigger)) throw new Error("maintenance task has no cron trigger");
		return trigger.cron;
	}

	test("jitter is consistent and distributed", () => {
		expect(maintenanceCron("default")).toBe(maintenanceCron("default"));

		// queue-53 hashes to 60 modulo 61
		const queues = ["default", "reports", "emails", "analytics", "jobs", "queue-53"];
		const crons = queues.map(maintenanceCron);

		for (const cron of crons) {
			const next = nextCronOccurrence(cron, new Date("2024-01-01T12:00:00Z"));
			expect(next.toISOString()).toMatch(/^2024-01-02T00:[0-5]\d:00\.000Z$/);
		}

		// At least 5 out of 6 should be unique (allowing for small collision chance)
		expect(new Set(crons).size).toBeGreaterThanOrEqual(5);
	});

	test("a worker starts on every queue name", async () => {
		const db = await pool.child();
		databases.push(db);
		const queue = "queue-53";

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "job", queue, payload: z.object({}) })]),
			context: {},
		});
		await conductor.ensureInstalled();
		const job = conductor.createTask({ name: "job", queue }, { invocable: true }, async () => {});
		const orchestrator = () =>
			Orchestrator.create({
				conductor,
				workers: [
					conductor.createWorker({
						queue,
						tasks: [job],
						config: { pollIntervalMs: 50, flushIntervalMs: 50 },
					}),
				],
			});

		await orchestrator().drain();
		const id = await conductor.invoke({ name: "job", queue }, {});
		await orchestrator().drain();

		const [row] = await db.sql<{ completed_at: Date | null }[]>`
			select completed_at from pgconductor._private_executions where id = ${id}
		`;
		expect(row?.completed_at).not.toBeNull();
	}, 30000);

	test("removes old completed executions based on retention policy", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinition = defineTask({
			name: "report-task",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		// Create task with 7-day retention for completed executions
		const reportTask = conductor.createTask(
			{
				name: "report-task",
				removeOnComplete: { days: 7 },
				removeOnFail: false,
			},
			{ invocable: true },
			async () => {},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [reportTask],
		});

		await orchestrator.start();

		// Set fake time to 30 days ago
		const thirtyDaysAgo = new Date();
		thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
		await db.client.setFakeTime({ date: thirtyDaysAgo });

		// Create old completed execution (30 days old)
		const oldExecId = await conductor.invoke({ name: "report-task" }, {});

		// Complete the execution manually
		await db.sql`
			UPDATE pgconductor._private_executions
			SET completed_at = pgconductor._private_current_time(),
				locked_by = null,
				locked_at = null
			WHERE id = ${oldExecId}
		`;

		// Set fake time to 5 days ago
		const fiveDaysAgo = new Date();
		fiveDaysAgo.setDate(fiveDaysAgo.getDate() - 5);
		await db.client.setFakeTime({ date: fiveDaysAgo });

		// Create recent completed execution (5 days old)
		const recentExecId = await conductor.invoke({ name: "report-task" }, {});

		await db.sql`
			UPDATE pgconductor._private_executions
			SET completed_at = pgconductor._private_current_time(),
				locked_by = null,
				locked_at = null
			WHERE id = ${recentExecId}
		`;

		// Return to current time
		await db.client.clearFakeTime();

		// Manually invoke maintenance task (internal task created by Worker)
		// @ts-expect-error - maintenance task is not in conductor's task registry
		await conductor.invoke({ name: "pgconductor.maintenance" }, {});

		// Wait for execution
		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();
		await db.client.clearFakeTime();

		// Check that old execution was deleted
		const oldExec = await db.sql`
			SELECT id FROM pgconductor._private_executions
			WHERE id = ${oldExecId}
		`;
		expect(oldExec.length).toBe(0);

		// Check that recent execution still exists
		const recentExec = await db.sql`
			SELECT id FROM pgconductor._private_executions
			WHERE id = ${recentExecId}
		`;
		expect(recentExec.length).toBe(1);
	}, 30000);

	test("removes old failed executions based on retention policy", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinition = defineTask({
			name: "failing-task",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		// Create task with 14-day retention for failed executions
		const failingTask = conductor.createTask(
			{
				name: "failing-task",
				maxAttempts: 1,
				removeOnComplete: false,
				removeOnFail: { days: 14 },
			},
			{ invocable: true },
			async () => {
				throw new Error("Task always fails");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [failingTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		// Set fake time to 20 days ago
		const twentyDaysAgo = new Date();
		twentyDaysAgo.setDate(twentyDaysAgo.getDate() - 20);
		await db.client.setFakeTime({ date: twentyDaysAgo });

		// Create old failed execution (20 days ago)
		const oldExecId = await conductor.invoke({ name: "failing-task" }, {});

		// Wait for first execution to fail with old timestamp
		await new Promise((r) => setTimeout(r, 1000));

		// Set fake time to 10 days ago
		const tenDaysAgo = new Date();
		tenDaysAgo.setDate(tenDaysAgo.getDate() - 10);
		await db.client.setFakeTime({ date: tenDaysAgo });

		// Create recent failed execution (10 days ago)
		const recentExecId = await conductor.invoke({ name: "failing-task" }, {});

		// Wait for second execution to fail
		await new Promise((r) => setTimeout(r, 1000));

		// Return to current time
		await db.client.clearFakeTime();

		// Verify both executions failed
		const failedExecs = await db.sql`
			SELECT id, failed_at
			FROM pgconductor._private_executions
			WHERE task_key = 'failing-task' AND failed_at IS NOT NULL
			ORDER BY failed_at ASC
		`;
		expect(failedExecs.length).toBe(2);

		// Manually invoke maintenance task (internal task created by Worker)
		// @ts-expect-error - maintenance task is not in conductor's task registry
		await conductor.invoke({ name: "pgconductor.maintenance" }, {});

		// Wait for execution
		await new Promise((r) => setTimeout(r, 1000));

		await orchestrator.stop();

		// Check that old execution was deleted
		const oldExec = await db.sql`
			SELECT id FROM pgconductor._private_executions
			WHERE id = ${oldExecId}
		`;
		expect(oldExec.length).toBe(0);

		// Check that recent execution still exists
		const recentExec = await db.sql`
			SELECT id FROM pgconductor._private_executions
			WHERE id = ${recentExecId}
		`;
		expect(recentExec.length).toBe(1);
	}, 30000);

	test("handles mixed retention policies for multiple tasks", async () => {
		const db = await pool.child();
		databases.push(db);

		const task1Definition = defineTask({
			name: "short-retention",
			payload: z.object({}),
		});

		const task2Definition = defineTask({
			name: "long-retention",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([task1Definition, task2Definition]),
			context: {},
		});

		// Task with 3-day retention
		const shortTask = conductor.createTask(
			{
				name: "short-retention",
				removeOnComplete: { days: 3 },
				removeOnFail: false,
			},
			{ invocable: true },
			async () => {},
		);

		// Task with 30-day retention
		const longTask = conductor.createTask(
			{
				name: "long-retention",
				removeOnComplete: { days: 30 },
				removeOnFail: false,
			},
			{ invocable: true },
			async () => {},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [shortTask, longTask],
		});

		await orchestrator.start();

		// Set fake time to 10 days ago
		const tenDaysAgo = new Date();
		tenDaysAgo.setDate(tenDaysAgo.getDate() - 10);
		await db.client.setFakeTime({ date: tenDaysAgo });

		// Create executions 10 days ago
		const shortExecId = await conductor.invoke({ name: "short-retention" }, {});
		const longExecId = await conductor.invoke({ name: "long-retention" }, {});

		// Complete both
		await db.sql`
			UPDATE pgconductor._private_executions
			SET completed_at = pgconductor._private_current_time(),
				locked_by = null,
				locked_at = null
			WHERE id = ANY(${[shortExecId, longExecId]}::uuid[])
		`;

		// Return to current time
		await db.client.clearFakeTime();

		// Manually invoke maintenance task (internal task created by Worker)
		// @ts-expect-error - maintenance task is not in conductor's task registry
		await conductor.invoke({ name: "pgconductor.maintenance" }, {});

		// Wait for execution
		await new Promise((r) => setTimeout(r, 3000));

		await orchestrator.stop();

		// Short retention task should be deleted (10 days > 3 days)
		const shortExec = await db.sql`
			SELECT id FROM pgconductor._private_executions
			WHERE id = ${shortExecId}
		`;
		expect(shortExec.length).toBe(0);

		// Long retention task should still exist (10 days < 30 days)
		const longExec = await db.sql`
			SELECT id FROM pgconductor._private_executions
			WHERE id = ${longExecId}
		`;
		expect(longExec.length).toBe(1);
	}, 30000);

	test("removes executions in batches (hasMore pattern)", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinition = defineTask({
			name: "batch-task",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const batchTask = conductor.createTask(
			{
				name: "batch-task",
				removeOnComplete: { days: 1 },
				removeOnFail: false,
			},
			{ invocable: true },
			async () => {},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [batchTask],
		});

		await orchestrator.start();

		// Set fake time to 7 days ago
		const sevenDaysAgo = new Date();
		sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
		await db.client.setFakeTime({ date: sevenDaysAgo });

		// Create 50 old completed executions
		const execIds = [];
		for (let i = 0; i < 50; i++) {
			const id = await conductor.invoke({ name: "batch-task" }, {});
			execIds.push(id);
		}

		// Complete all
		await db.sql`
			UPDATE pgconductor._private_executions
			SET completed_at = pgconductor._private_current_time(),
				locked_by = null,
				locked_at = null
			WHERE task_key = 'batch-task'
		`;

		// Return to current time
		await db.client.clearFakeTime();

		// Manually invoke maintenance task (internal task created by Worker)
		// @ts-expect-error - maintenance task is not in conductor's task registry
		await conductor.invoke({ name: "pgconductor.maintenance" }, {});

		// Wait for execution (may need multiple cycles for batching)
		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();

		// All old executions should be deleted
		const remaining = await db.sql`
			SELECT id FROM pgconductor._private_executions
			WHERE task_key = 'batch-task' AND completed_at IS NOT NULL
		`;
		expect(remaining.length).toBe(0);
	}, 30000);

	test("respects retention policy of false (no removal)", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinition = defineTask({
			name: "keep-forever",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const keepTask = conductor.createTask(
			{
				name: "keep-forever",
				removeOnComplete: false, // Keep forever
				removeOnFail: false,
			},
			{ invocable: true },
			async () => {},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [keepTask],
		});

		await orchestrator.start();

		// Set fake time to 365 days ago
		const oneYearAgo = new Date();
		oneYearAgo.setDate(oneYearAgo.getDate() - 365);
		await db.client.setFakeTime({ date: oneYearAgo });

		// Create very old completed execution
		const execId = await conductor.invoke({ name: "keep-forever" }, {});

		await db.sql`
			UPDATE pgconductor._private_executions
			SET completed_at = pgconductor._private_current_time(),
				locked_by = null,
				locked_at = null
			WHERE id = ${execId}
		`;

		// Return to current time
		await db.client.clearFakeTime();

		// Manually invoke maintenance task (internal task created by Worker)
		// @ts-expect-error - maintenance task is not in conductor's task registry
		await conductor.invoke({ name: "pgconductor.maintenance" }, {});

		// Wait for execution
		await new Promise((r) => setTimeout(r, 1000));

		await orchestrator.stop();

		// Execution should still exist (not removed)
		const exec = await db.sql`
			SELECT id FROM pgconductor._private_executions
			WHERE id = ${execId}
		`;
		expect(exec.length).toBe(1);
	}, 30000);
	describe("executions of unregistered tasks", () => {
		const definitions = [
			defineTask({ name: "removed", payload: z.object({}) }),
			defineTask({ name: "kept", payload: z.object({}) }),
			defineTask({ name: "parent", payload: z.object({}) }),
			defineTask({ name: "waiter", payload: z.object({ id: z.string() }) }),
		];

		async function setup() {
			const db = await pool.child();
			databases.push(db);
			const conductor = Conductor.create({
				sql: db.sql,
				tasks: TaskSchemas.fromSchema(definitions),
				context: {},
				clock: db.clock,
			});
			await conductor.ensureInstalled();
			const outcomes: unknown[] = [];
			const kept = conductor.createTask({ name: "kept" }, { invocable: true }, async () => {});
			const tasks = {
				kept,
				removed: conductor.createTask(
					{ name: "removed", deadLetter: { queue: "default", task: kept } },
					{ invocable: true },
					async () => {},
				),
				parent: conductor.createTask({ name: "parent" }, { invocable: true }, async (_, ctx) => {
					await ctx.invokeAndWait("child", { name: "removed" }, {});
				}),
				waiter: conductor.createTask(
					{ name: "waiter" },
					{ invocable: true },
					async (event, ctx) => {
						if (event.name === "pgconductor.invoke") {
							outcomes.push(await ctx.waitForResult("result", event.payload.id));
						}
					},
				),
			};
			const drain = (registered: (keyof typeof tasks)[]) =>
				Orchestrator.create({
					conductor,
					tasks: registered.map((name) => tasks[name]),
					defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
				}).drain();
			const maintain = async (registered: (keyof typeof tasks)[]) => {
				// @ts-expect-error - maintenance task is not in conductor's task registry
				await conductor.invoke({ name: "pgconductor.maintenance" }, {});
				await drain(registered);
			};
			const state = async (id: string) => {
				const [row] = await db.sql<{ failed: boolean; last_error: string | null }[]>`
					select failed_at is not null as failed, last_error
					from pgconductor._private_executions where id = ${id}::uuid
				`;
				return row;
			};
			return { db, conductor, outcomes, drain, maintain, state };
		}

		const later = { run_at: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000) };

		test("fails them a day after a registration left their task out", async () => {
			const { db, conductor, outcomes, drain, maintain, state } = await setup();
			await drain(["removed", "kept", "parent", "waiter"]);
			const removedId = await conductor.invoke({ name: "removed" }, {}, later);
			const parentId = await conductor.invoke({ name: "parent" }, {});
			const waiterId = await conductor.invoke({ name: "waiter" }, { id: removedId });
			await drain(["kept", "parent", "waiter"]);
			const [child] = await db.sql<{ id: string }[]>`
				select id from pgconductor._private_executions
				where task_key = 'removed' and parent_execution_id = ${parentId}::uuid
			`;
			if (!child) throw new Error("expected the parent's child");

			await maintain(["kept", "parent", "waiter"]);
			expect(await state(removedId)).toEqual({ failed: false, last_error: null });
			expect(await state(child.id)).toEqual({ failed: false, last_error: null });

			await db.clock.advance("25h");
			await maintain(["kept", "parent", "waiter"]);

			const error = "task removed is not registered on queue default";
			expect(await state(removedId)).toEqual({ failed: true, last_error: error });
			expect(await state(child.id)).toEqual({ failed: true, last_error: error });
			expect(await state(parentId)).toEqual({
				failed: true,
				last_error: `Child execution failed: ${error}`,
			});
			expect(outcomes).toEqual([{ status: "failed", error }]);
			expect(await state(waiterId)).toMatchObject({ failed: false });
			const deadLetters = await db.sql<{ source: string; error: string }[]>`
				select dead_letter->>'sourceExecutionId' as source, dead_letter->>'error' as error
				from pgconductor._private_executions
				where task_key = 'kept' and dead_letter is not null
				order by source
			`;
			expect(deadLetters).toEqual(
				[removedId, child.id].sort().map((source) => ({ source, error })),
			);
		}, 30000);

		test("keeps them when a later registration includes their task again", async () => {
			const { db, conductor, drain, maintain, state } = await setup();
			await drain(["removed", "kept"]);
			const id = await conductor.invoke({ name: "removed" }, {}, later);
			await drain(["kept"]);
			await drain(["removed", "kept"]);

			await db.clock.advance("25h");
			await maintain(["kept"]);

			expect(await state(id)).toEqual({ failed: false, last_error: null });
		}, 30000);

		test("fails them a day after they were created when their task was never registered", async () => {
			const { db, maintain, state } = await setup();
			const [{ id }] = await db.sql<[{ id: string }]>`
				select id from pgconductor.invoke(
					p_task_key := 'ghost',
					p_run_at := pgconductor._private_current_time() + interval '7 days'
				)
			`;

			await maintain(["kept"]);
			expect(await state(id)).toEqual({ failed: false, last_error: null });

			await db.clock.advance("25h");
			await maintain(["kept"]);

			expect(await state(id)).toEqual({
				failed: true,
				last_error: "task ghost is not registered on queue default",
			});
		}, 30000);
	});
});
