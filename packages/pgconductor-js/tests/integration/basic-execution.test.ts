import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";
import { waitForCondition } from "../test-utils";

describe("Basic Task Execution", () => {
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

	test("executes a simple task end-to-end", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinitions = defineTask({
			name: "hello-task",
			payload: z.object({ name: z.string() }),
		});

		const contextFn = mock((s: string) => `Hello ${s}`);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinitions]),
			context: {
				contextFn,
			},
		});

		let executionCount = 0;

		const helloTask = conductor.createTask(
			{ name: "hello-task" },
			{ invocable: true },
			async (event, ctx) => {
				executionCount++;
				// This test only uses manual invocation
				if (event.name === "pgconductor.invoke") {
					ctx.contextFn(event.payload.name);
				}
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [helloTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		await conductor.invoke(
			{ name: "hello-task" },
			{
				name: "World",
			},
		);

		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();

		await stoppedPromise;

		expect(executionCount).toBe(1);
		expect(contextFn).toHaveBeenCalledWith("World");
	}, 30000);

	test("flushes a single result after a full flush batch", async () => {
		const db = await pool.child();
		databases.push(db);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "flush-task", payload: z.object({}) })]),
			context: {},
		});
		const task = conductor.createTask({ name: "flush-task" }, { invocable: true }, async () => {});
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50, flushBatchSize: 2 },
		});
		await orchestrator.start();

		const completedCount = async () => {
			const [{ count }] = await db.sql<[{ count: number }]>`
				select count(*)::int as count from pgconductor._private_executions
				where task_key = 'flush-task' and completed_at is not null
			`;
			return count;
		};
		try {
			await Promise.all([
				conductor.invoke({ name: "flush-task" }, {}),
				conductor.invoke({ name: "flush-task" }, {}),
			]);
			await waitForCondition(async () => (await completedCount()) === 2, 5000);
			await new Promise((r) => setTimeout(r, 200));

			await conductor.invoke({ name: "flush-task" }, {});
			await waitForCondition(async () => (await completedCount()) === 3, 5000);
		} finally {
			await orchestrator.stop();
		}
	}, 30000);

	test("retries a failed flush without new results", async () => {
		const db = await pool.child();
		databases.push(db);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "flush-task", payload: z.object({}) })]),
			context: {},
		});
		const returnExecutions = conductor.db.returnExecutions.bind(conductor.db);
		let failures = 0;
		conductor.db.returnExecutions = async (grouped, opts) => {
			if (failures === 0) {
				failures++;
				throw new Error("flush failed");
			}
			return returnExecutions(grouped, opts);
		};
		const task = conductor.createTask({ name: "flush-task" }, { invocable: true }, async () => {});
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 100, flushBatchSize: 10 },
		});
		await orchestrator.start();

		try {
			const id = await conductor.invoke({ name: "flush-task" }, {});
			await waitForCondition(async () => {
				const [execution] = await db.sql<{ completed: boolean }[]>`
					select completed_at is not null as completed from pgconductor._private_executions
					where id = ${id}
				`;
				return execution?.completed === true;
			}, 3000);
			expect(failures).toBe(1);
		} finally {
			await orchestrator.stop();
		}
	}, 30000);

	test("stop() waits for an in-flight flush", async () => {
		const db = await pool.child();
		databases.push(db);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "flush-task", payload: z.object({}) })]),
			context: {},
		});
		const returnExecutions = conductor.db.returnExecutions.bind(conductor.db);
		const flushStarted = Promise.withResolvers<void>();
		conductor.db.returnExecutions = async (grouped, opts) => {
			flushStarted.resolve();
			await Bun.sleep(500);
			return returnExecutions(grouped, opts);
		};
		const task = conductor.createTask({ name: "flush-task" }, { invocable: true }, async () => {});
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 100, flushBatchSize: 10 },
		});
		await orchestrator.start();

		const id = await conductor.invoke({ name: "flush-task" }, {});
		await flushStarted.promise;
		await orchestrator.stop();

		const [execution] = await db.sql<{ completed: boolean; locked: boolean }[]>`
			select completed_at is not null as completed, locked_by is not null as locked
			from pgconductor._private_executions where id = ${id}
		`;
		expect(execution).toEqual({ completed: true, locked: false });
	}, 30000);
});
