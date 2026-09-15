import { describe, expect, test } from "bun:test";
import { expectTypeOf } from "expect-type";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineEvent } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { WaitForEventTimeoutError } from "../../src/index";

describe("waitForEvent API", () => {
	test("returns the declared event and payload and constrains filters", () => {
		const event = defineEvent({
			name: "api.order",
			payload: z.object({ status: z.enum(["paid", "pending"]), id: z.string() }),
			filterable: ["status"],
		});
		const task = defineTask({ name: "api.wait", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([task]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		conductor.createTask({ name: "api.wait" }, { invocable: true }, async (_event, ctx) => {
			const result = await ctx.waitForEvent("order", { event, filter: { status: ["paid"] } });
			expectTypeOf(result).toEqualTypeOf<{
				name: "api.order";
				payload: { status: "paid" | "pending"; id: string };
			}>();
			// @ts-expect-error id is not declared filterable
			ctx.waitForEvent("bad", { event, filter: { id: ["x"] } });
		});
		expect(WaitForEventTimeoutError).toBeDefined();
	});
});
