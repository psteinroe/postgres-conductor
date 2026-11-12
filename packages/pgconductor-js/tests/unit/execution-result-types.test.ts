import { test, describe } from "vitest";
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
			Promise<ExecutionInfo<{ sum: number }, { a: number; b: number }> | null>
		>();
		expectTypeOf(conductor.getExecution({ name: "report", queue: "reports" }, "id")).toEqualTypeOf<
			Promise<ExecutionInfo<{ url: string }, object> | null>
		>();
		expectTypeOf<ExecutionInfo["status"]>().toEqualTypeOf<
			"pending" | "running" | "completed" | "failed" | "cancelled"
		>();
		expectTypeOf<ExecutionInfo["result"]>().toEqualTypeOf<unknown>();
	});

	test("getExecution by key and dedupe key types the result by task", () => {
		expectTypeOf(conductor.getExecution({ task: { name: "add" }, key: "k" })).toEqualTypeOf<
			Promise<ExecutionInfo<{ sum: number }, { a: number; b: number }> | null>
		>();
		expectTypeOf(
			conductor.getExecution({ task: { name: "report", queue: "reports" }, dedupeKey: "d" }),
		).toEqualTypeOf<Promise<ExecutionInfo<{ url: string }, object> | null>>();

		if (false) {
			// @ts-expect-error - unknown task
			conductor.getExecution({ task: { name: "missing" }, key: "k" });

			// @ts-expect-error - a key or a dedupe key, not both
			conductor.getExecution({ task: { name: "add" }, key: "k", dedupeKey: "d" });

			// @ts-expect-error - a key or a dedupe key is required
			conductor.getExecution({ task: { name: "add" } });
		}
	});

	test("signal addresses an execution by id or by key", () => {
		const signal = { name: "stop", id: "s1" };
		expectTypeOf(conductor.signal("id", signal)).toEqualTypeOf<Promise<boolean>>();
		expectTypeOf(
			conductor.signal({ task: { name: "report", queue: "reports" }, key: "k" }, signal),
		).toEqualTypeOf<Promise<boolean>>();

		if (false) {
			// @ts-expect-error - unknown task
			conductor.signal({ task: { name: "missing" }, key: "k" }, signal);

			// @ts-expect-error - the key is required
			conductor.signal({ task: { name: "add" } }, signal);
		}
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
