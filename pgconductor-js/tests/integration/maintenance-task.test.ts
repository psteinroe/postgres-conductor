import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import crypto from "crypto";
import { TaskSchemas } from "../../src/schemas";

function hashToJitter(str: string): number {
	const hash = crypto.createHash("sha256").update(str).digest();
	const int = hash.readUInt32BE(0);
	return int % 61;
}

describe("Maintenance Task", () => {
	let pool: TestDatabasePool;

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterAll(async () => {
		await pool?.destroy();
	});

	test("jitter is consistent and distributed", () => {
		// Test that same queue always gets same jitter
		const jitter1 = hashToJitter("default");
		const jitter2 = hashToJitter("default");
		expect(jitter1).toBe(jitter2);

		// Test that different queues get different jitter (high probability)
		const queues = ["default", "reports", "emails", "analytics", "jobs"];
		const jitters = queues.map((q) => hashToJitter(q));

		// All jitters should be in valid range [0, 60]
		for (const j of jitters) {
			expect(j).toBeGreaterThanOrEqual(0);
			expect(j).toBeLessThanOrEqual(60);
		}

		// At least 4 out of 5 should be unique (allowing for small collision chance)
		const uniqueJitters = new Set(jitters);
		expect(uniqueJitters.size).toBeGreaterThanOrEqual(4);
	});

	test("removes old completed executions based on retention policy", async () => {
		const db = await pool.child();

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
		// @ts-expect-error - maintenance task is not in the conductor's task list
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
		// @ts-expect-error - maintenance task is not in the conductor's task list
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
		// @ts-expect-error - maintenance task is not in the conductor's task list
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
		// @ts-expect-error - maintenance task is not in the conductor's task list
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
		// @ts-expect-error - maintenance task is not in the conductor's task list
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
});
