import { describe, expect, test } from "bun:test";
import { expectTypeOf } from "expect-type";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineEvent } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { WaitForEventTimeoutError, type EventSubscription } from "../../src/index";

describe("waitForEvent API", () => {
	test("returns the declared event and payload and constrains filters", () => {
		const event = defineEvent({
			name: "api.order",
			payload: z.object({ status: z.enum(["paid", "pending"]), id: z.string() }),
			filterable: ["status"],
		});
		const reply = defineEvent({
			name: "api.reply",
			payload: z.object({ thread: z.string(), text: z.string() }),
			filterable: ["thread"],
		});
		const task = defineTask({ name: "api.wait", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([task]),
			events: EventSchemas.fromSchema([event, reply]),
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

			const subscription = await ctx.subscribe("approve", {
				event,
				filter: { status: ["paid"] },
			});
			expectTypeOf(subscription).toEqualTypeOf<
				EventSubscription<{
					name: "api.order";
					payload: { status: "paid" | "pending"; id: string };
				}>
			>();
			expectTypeOf(await subscription.wait({ timeout: "24h" })).toEqualTypeOf<{
				name: "api.order";
				payload: { status: "paid" | "pending"; id: string };
			}>();
			// @ts-expect-error subscriptions do not take a timeout
			ctx.subscribe("timeout", { event, timeout: "1h" });
			// @ts-expect-error id is not declared filterable
			ctx.subscribe("bad", { event, filter: { id: ["x"] } });

			const winner = await ctx.waitForAny(
				"approval-or-reply",
				{ decision: subscription, reply: { event: reply, filter: { thread: ["t"] } } },
				{ timeout: "24h" },
			);
			expectTypeOf(winner).toEqualTypeOf<
				| {
						key: "decision";
						event: { name: "api.order"; payload: { status: "paid" | "pending"; id: string } };
				  }
				| { key: "reply"; event: { name: "api.reply"; payload: { thread: string; text: string } } }
				| { key: "timeout" }
			>();
			// @ts-expect-error text is not declared filterable
			ctx.waitForAny("bad", { reply: { event: reply, filter: { text: ["x"] } } });
			// @ts-expect-error timeout is the result key of the timeout
			ctx.waitForAny("bad", { timeout: subscription });
		});
		expect(WaitForEventTimeoutError).toBeDefined();
	});
});
