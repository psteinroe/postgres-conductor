import { describe, expect, test } from "vitest";
import { expectTypeOf } from "expect-type";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineEvent } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import {
	WaitForEventTimeoutError,
	type EventSubscription,
	type ExecutionOutcome,
	type SignalInfo,
} from "../../src/index";

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
		const work = defineTask({ name: "api.work", returns: z.object({ doubled: z.number() }) });
		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([task, work]),
			events: [event, reply],
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

			const replied = await ctx.subscribe("reply", { event: reply, filter: { thread: ["t"] } });
			const winner = await ctx.waitForAny(
				"approval-or-reply",
				{ decision: subscription, reply: replied },
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
			// @ts-expect-error branches are subscriptions
			ctx.waitForAny("bad", { reply: { event: reply } });
			// @ts-expect-error timeout is the result key of the timeout
			ctx.waitForAny("bad", { timeout: subscription });

			const toolOrReply = await ctx.waitForAny("tool-or-reply", {
				tool: { execution: "id" },
				reply: replied,
			});
			expectTypeOf(toolOrReply).toEqualTypeOf<
				| { key: "tool"; outcome: ExecutionOutcome }
				| { key: "reply"; event: { name: "api.reply"; payload: { thread: string; text: string } } }
				| { key: "timeout" }
			>();
			expectTypeOf(
				await ctx.waitForResult<{ doubled: number }>("result", "id", { timeout: "1h" }),
			).toEqualTypeOf<ExecutionOutcome<{ doubled: number }> | { status: "timeout" }>();
			expectTypeOf(
				await ctx.waitForResult("typed", { name: "api.work" }, "id", { timeout: "1h" }),
			).toEqualTypeOf<ExecutionOutcome<{ doubled: number }> | { status: "timeout" }>();
			// @ts-expect-error unknown task
			ctx.waitForResult("bad", { name: "api.missing" }, "id");
			expectTypeOf(
				await ctx.waitForAny("typed-any", {
					tool: { execution: "id", task: { name: "api.work" } },
				}),
			).toEqualTypeOf<
				{ key: "tool"; outcome: ExecutionOutcome<{ doubled: number }> } | { key: "timeout" }
			>();
			expectTypeOf(
				await ctx.waitForAny("idle", { input: { signal: "input" }, tool: { execution: "id" } }),
			).toEqualTypeOf<
				| { key: "input"; signal: SignalInfo }
				| { key: "tool"; outcome: ExecutionOutcome }
				| { key: "timeout" }
			>();
			expectTypeOf(ctx.executionId).toEqualTypeOf<string>();
			expectTypeOf(ctx.queue).toEqualTypeOf<string>();
		});
		expect(WaitForEventTimeoutError).toBeDefined();
	});
});
