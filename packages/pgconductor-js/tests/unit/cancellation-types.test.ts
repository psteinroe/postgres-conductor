import { describe, expect, test } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { CancelledError } from "../../src/index";

describe("cancellation API", () => {
	test("invoke accepts cancelWithParent and CancelledError is exported", () => {
		const parent = defineTask({ name: "api.parent", payload: z.object({}) });
		const child = defineTask({ name: "api.child", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([parent, child]),
			context: {},
		});
		conductor.createTask({ name: "api.parent" }, { invocable: true }, async (_event, ctx) => {
			await ctx.invoke("child", { name: "api.child" }, {}, { cancelWithParent: false });
			// @ts-expect-error cancelWithParent is a boolean
			await ctx.invoke("bad", { name: "api.child" }, {}, { cancelWithParent: "no" });
		});

		const error = new CancelledError("Cancelled by user");
		expect(error).toBeInstanceOf(Error);
		expect(error.name).toBe("CancelledError");
		expect(error.code).toBe("PGCONDUCTOR_CANCELLED");
		expect(error.message).toBe("Cancelled by user");
	});
});
