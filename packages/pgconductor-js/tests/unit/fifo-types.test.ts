import { describe, expect, test } from "bun:test";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";

const mockSql = {} as any;

describe("FIFO task configuration", () => {
	test("is exposed and rejects conflicting limits at type level", () => {
		const definition = defineTask({ name: "fifo-task" });
		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "fifo-task", fifo: true },
			{ invocable: true },
			async () => {},
		);
		expect(task.fifo).toBe(true);

		if (false)
			conductor.createTask(
				// @ts-expect-error FIFO and task concurrency are contradictory.
				{ name: "fifo-task", fifo: true, concurrency: 2 },
				{ invocable: true },
				async () => {},
			);
		if (false)
			conductor.createTask(
				// @ts-expect-error FIFO and group concurrency are contradictory.
				{ name: "fifo-task", fifo: true, groupConcurrency: 2 },
				{ invocable: true },
				async () => {},
			);
	});

	test("rejects conflicting limits at runtime", () => {
		const definition = defineTask({ name: "fifo-runtime" });
		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		expect(() =>
			conductor.createTask(
				{ name: "fifo-runtime", fifo: true, concurrency: 2 } as any,
				{ invocable: true },
				async () => {},
			),
		).toThrow("fifo cannot be combined");
	});
});
