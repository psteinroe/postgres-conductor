import { test, describe } from "bun:test";
import { expectTypeOf } from "expect-type";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import type { ExecutionInfo } from "../../src/database-client";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";

const mockSql = Object.assign(
	(() => Promise.resolve([{ status: "completed", result: null }])) as any,
	{
		json: (val: any) => val,
		unsafe: () => Promise.resolve([]),
	},
);

describe("execution result types", () => {
	const add = defineTask({
		name: "add",
		payload: z.object({ a: z.number(), b: z.number() }),
		returns: z.object({ sum: z.number() }),
	});
	const report = defineTask({
		name: "report",
		queue: "reports",
		returns: z.object({ url: z.string() }),
	});
	const notify = defineTask({ name: "notify" });

	const conductor = Conductor.create({
		sql: mockSql,
		tasks: TaskSchemas.fromSchema([add, report, notify]),
		context: {},
	});

	test("getExecution types the result by task", () => {
		expectTypeOf(conductor.getExecution("id")).toEqualTypeOf<Promise<ExecutionInfo | null>>();
		expectTypeOf(conductor.getExecution({ name: "add" }, "id")).toEqualTypeOf<
			Promise<ExecutionInfo<{ sum: number }> | null>
		>();
		expectTypeOf(conductor.getExecution({ name: "report", queue: "reports" }, "id")).toEqualTypeOf<
			Promise<ExecutionInfo<{ url: string }> | null>
		>();
		expectTypeOf<ExecutionInfo["status"]>().toEqualTypeOf<
			"pending" | "running" | "completed" | "failed" | "cancelled"
		>();
		expectTypeOf<ExecutionInfo["result"]>().toEqualTypeOf<unknown>();
	});

	test("waitForResult types the result by task", () => {
		expectTypeOf(conductor.waitForResult("id")).toEqualTypeOf<Promise<unknown>>();
		expectTypeOf(conductor.waitForResult({ name: "add" }, "id")).toEqualTypeOf<
			Promise<{ sum: number }>
		>();
		expectTypeOf(
			conductor.waitForResult({ name: "report", queue: "reports" }, "id", { timeout: 1000 }),
		).toEqualTypeOf<Promise<{ url: string }>>();
		expectTypeOf(conductor.waitForResult({ name: "notify" }, "id")).toEqualTypeOf<Promise<void>>();

		if (false) {
			// @ts-expect-error - unknown task
			conductor.waitForResult({ name: "missing" }, "id");

			// @ts-expect-error - unknown option
			conductor.waitForResult("id", { interval: 10 });
		}
	});
});
