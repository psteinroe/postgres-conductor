import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";

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

		const tasks = [taskDefinitions] as const;

		const contextFn = mock((s: string) => `Hello ${s}`);

		const conductor = new Conductor({
			sql: db.sql,
			tasks,
			context: {
				contextFn,
			},
		});

		let executionCount = 0;

		const helloTask = conductor.createTask(
			"hello-task",
			async (payload, ctx) => {
				executionCount++;
				ctx.contextFn(payload.name);
			},
		);

		const orchestrator = new Orchestrator({
			conductor,
			tasks: [helloTask],
		});

		await orchestrator.start();

		const stoppedPromise = orchestrator.stopped;

		const executionId = await conductor.db.invoke({
			task_key: "hello-task",
			payload: { name: "World" },
		});

		expect(executionId).toBeDefined();

		// Wait for task execution
		await new Promise((r) => setTimeout(r, 2000));

		await orchestrator.stop();

		// this is just for testing - it should resolve at the same time as orchestrator.stop()
		await stoppedPromise;

		expect(executionCount).toBe(1);
		expect(contextFn).toHaveBeenCalledWith("World");
	}, 30000);
});
