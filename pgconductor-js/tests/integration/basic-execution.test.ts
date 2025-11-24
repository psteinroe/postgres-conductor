import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";

describe("Basic Task Execution", () => {
	let pool: TestDatabasePool;

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterAll(async () => {
		await pool?.destroy();
	});

	test("executes a simple task end-to-end", async () => {
		const db = await pool.child();

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
				if (event.event === "pgconductor.invoke") {
					ctx.contextFn(event.payload.name);
				}
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [helloTask],
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		await conductor.invoke({ name: "hello-task" }, {
			name: "World",
		});

		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();

		await stoppedPromise;

		expect(executionCount).toBe(1);
		expect(contextFn).toHaveBeenCalledWith("World");
	}, 30000);
});
