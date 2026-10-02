import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";
import { waitForCondition } from "../test-utils";

describe("Execution Timeout", () => {
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

	test("aborts the handler signal and fails the attempt", async () => {
		const db = await pool.child();
		databases.push(db);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "hanging", payload: z.object({}) })]),
			context: {},
		});

		let signal: AbortSignal | undefined;
		const task = conductor.createTask(
			{ name: "hanging", timeoutMs: 100 },
			{ invocable: true },
			async (_event, ctx) => {
				signal = ctx.signal;
				await new Promise(() => {});
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await conductor.ensureInstalled();
		await conductor.invoke({ name: "hanging" }, {});
		await orchestrator.drain();

		expect(signal?.aborted).toBe(true);

		const [execution] = await db.sql<
			{ attempts: number; last_error: string; locked_by: string | null; failed_at: Date | null }[]
		>`select attempts, last_error, locked_by, failed_at from pgconductor._private_executions where task_key = 'hanging'`;
		expect(execution).toMatchObject({
			attempts: 1,
			last_error: "Task timed out after 100ms",
			locked_by: null,
			failed_at: null,
		});
	}, 30000);

	test("permanently fails after the last attempt times out", async () => {
		const db = await pool.child();
		databases.push(db);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "hanging", payload: z.object({}) })]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "hanging", timeoutMs: 100, maxAttempts: 1 },
			{ invocable: true },
			async () => {
				await new Promise(() => {});
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await conductor.ensureInstalled();
		await conductor.invoke({ name: "hanging" }, {});
		await orchestrator.drain();

		const [execution] = await db.sql<{ last_error: string; failed_at: Date | null }[]>`
			select last_error, failed_at from pgconductor._private_executions where task_key = 'hanging'
		`;
		expect(execution?.last_error).toBe("Task timed out after 100ms");
		expect(execution?.failed_at).not.toBeNull();
	}, 30000);

	test("does not affect handlers that finish in time", async () => {
		const db = await pool.child();
		databases.push(db);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "quick", payload: z.object({}) })]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "quick", timeoutMs: 5000 },
			{ invocable: true },
			async () => {},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await conductor.ensureInstalled();
		await conductor.invoke({ name: "quick" }, {});
		await orchestrator.drain();

		const [execution] = await db.sql<{ completed_at: Date | null }[]>`
			select completed_at from pgconductor._private_executions where task_key = 'quick'
		`;
		expect(execution?.completed_at).not.toBeNull();
	}, 30000);

	test("bounds how long stop() waits for a handler that ignores the signal", async () => {
		const db = await pool.child();
		databases.push(db);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "hanging", payload: z.object({}) })]),
			context: {},
		});

		let started = false;
		const task = conductor.createTask(
			{ name: "hanging", timeoutMs: 500 },
			{ invocable: true },
			async () => {
				started = true;
				await new Promise(() => {});
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await conductor.ensureInstalled();
		await orchestrator.start();
		await conductor.invoke({ name: "hanging" }, {});
		await waitForCondition(() => started);
		await orchestrator.stop();

		const [execution] = await db.sql<{ attempts: number; last_error: string }[]>`
			select attempts, last_error from pgconductor._private_executions where task_key = 'hanging'
		`;
		expect(execution).toMatchObject({ attempts: 1, last_error: "Task timed out after 500ms" });
	}, 30000);

	test("fails every execution of a timed out batch", async () => {
		const db = await pool.child();
		databases.push(db);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "batch", payload: z.object({}) })]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "batch", timeoutMs: 100, batch: { size: 2, timeoutMs: 50 } },
			{ invocable: true },
			async () => {
				await new Promise(() => {});
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await conductor.ensureInstalled();
		await conductor.invoke({ name: "batch" }, {});
		await conductor.invoke({ name: "batch" }, {});
		await orchestrator.drain();

		const executions = await db.sql<{ attempts: number; last_error: string }[]>`
			select attempts, last_error from pgconductor._private_executions where task_key = 'batch'
		`;
		expect([...executions]).toEqual([
			{ attempts: 1, last_error: "Task timed out after 100ms" },
			{ attempts: 1, last_error: "Task timed out after 100ms" },
		]);
	}, 30000);
});
