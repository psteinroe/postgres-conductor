import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { waitFor } from "../../src/lib/wait-for";
import { TaskSchemas } from "../../src/schemas";
import { waitForCondition } from "../test-utils";

describe("Worker API", () => {
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

	test("createWorker() API works", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefEmail = defineTask({
			name: "send-email",
			queue: "notifications",
			payload: z.object({ to: z.string() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefEmail]),
			context: {},
		});

		const emailResults: string[] = [];

		const emailTask = conductor.createTask(
			{ name: "send-email", queue: "notifications" },
			{ invocable: true },
			async (event) => {
				if (event.name === "pgconductor.invoke") {
					emailResults.push(event.payload.to);
				}
			},
		);

		const notificationWorker = conductor.createWorker({
			queue: "notifications",
			tasks: [emailTask],
			config: { concurrency: 2 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [notificationWorker],
		});
		expect(orchestrator.info.workerCount).toBe(1);

		await orchestrator.start();

		await conductor.invoke(
			{ name: "send-email", queue: "notifications" },
			{ to: "user@example.com" },
		);

		// Wait for execution to complete
		await waitFor(2000);

		expect(emailResults).toHaveLength(1);
		expect(emailResults[0]).toBe("user@example.com");

		await orchestrator.stop();
		await db.destroy();
	}, 60000);

	test("default worker api works", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({
			name: "test-task",
			payload: z.object({ value: z.string() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const results: string[] = [];

		const testTask = conductor.createTask(
			{ name: "test-task" },
			{ invocable: true },
			async (event) => {
				if (event.name === "pgconductor.invoke") {
					results.push(event.payload.value);
				}
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [testTask],
		});

		await orchestrator.start();

		await conductor.invoke({ name: "test-task" }, { value: "test" });

		await waitFor(2000);

		expect(results).toHaveLength(1);
		expect(results[0]).toBe("test");

		await orchestrator.stop();
		await db.destroy();
	}, 60000);

	test.each([
		["without", undefined],
		["with", 1],
	])(
		"claims only tasks the worker registers %s concurrency limits",
		async (_, concurrency) => {
			const db = await pool.child();
			databases.push(db);

			const taskA = defineTask({ name: "a" });
			const taskB = defineTask({ name: "b" });

			const newConductor = Conductor.create({
				sql: db.sql,
				tasks: TaskSchemas.fromSchema([taskA, taskB]),
				context: {},
			});
			const newOrchestrator = Orchestrator.create({
				conductor: newConductor,
				tasks: [
					newConductor.createTask({ name: "a" }, { invocable: true }, async () => {}),
					newConductor.createTask({ name: "b", concurrency }, { invocable: true }, async () => {}),
				],
			});
			await newOrchestrator.start();
			await newOrchestrator.stop();

			await newConductor.invoke({ name: "b" }, {});
			await db.sql`insert into pgconductor._private_executions (task_key, queue) values ('ghost', 'default')`;

			const oldConductor = Conductor.create({
				sql: db.sql,
				tasks: TaskSchemas.fromSchema([taskA]),
				context: {},
			});
			let ranA = false;
			const oldOrchestrator = Orchestrator.create({
				conductor: oldConductor,
				tasks: [
					oldConductor.createTask({ name: "a" }, { invocable: true }, async () => {
						ranA = true;
					}),
				],
				defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			});
			await oldOrchestrator.start();
			await oldConductor.invoke({ name: "a" }, {});
			await waitForCondition(() => ranA, 5000);
			await waitFor(300);

			const rows = await db.sql<{ task_key: string; attempts: number; locked: boolean }[]>`
			select task_key, attempts, locked_by is not null as locked
			from pgconductor._private_executions
			where task_key in ('b', 'ghost')
			order by task_key
		`;
			await oldOrchestrator.stop();

			expect([...rows]).toEqual([
				{ task_key: "b", attempts: 0, locked: false },
				{ task_key: "ghost", attempts: 0, locked: false },
			]);
		},
		30000,
	);
});
