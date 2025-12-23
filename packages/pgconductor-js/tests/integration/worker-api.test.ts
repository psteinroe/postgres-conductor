import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { waitFor } from "../../src/lib/wait-for";
import { TaskSchemas } from "../../src/schemas";

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
});
