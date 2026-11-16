import { describe, expect, test } from "vitest";
import { expectTypeOf } from "expect-type";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";

const mockSql = Object.assign((() => Promise.resolve([])) as any, {
	json: (val: any) => val,
	unsafe: () => Promise.resolve([]),
});

const send = defineTask({
	name: "send",
	payload: z.object({ to: z.string(), retries: z.number().default(1) }),
});
const counter = defineTask({
	name: "counter",
	state: z.object({ count: z.string().transform(Number) }),
});

const conductor = Conductor.create({
	sql: mockSql,
	tasks: TaskSchemas.fromSchema([send, counter]),
	context: {},
});

describe("senders take a schema's input, handlers get its output", () => {
	test("a payload field with a default may be left out when sending", () => {
		if (false) {
			void conductor.invoke({ name: "send" }, { to: "a" });
			conductor.createTask({ name: "send" }, { invocable: true }, async (event, ctx) => {
				expectTypeOf(event.payload.retries).toEqualTypeOf<number>();
				await ctx.invoke("again", { name: "send" }, { to: "b" });
				// @ts-expect-error: the payload is required
				await ctx.invoke("missing", { name: "send" });
				// @ts-expect-error: `to` is required
				await ctx.invokeAndWait("wrong", { name: "send" }, {});
			});
		}
	});

	test("setState takes the state schema's input, ctx.state is its output", () => {
		if (false) {
			conductor.createTask({ name: "counter" }, { invocable: true }, async (_event, ctx) => {
				await ctx.setState({ count: "5" });
				// @ts-expect-error: the schema's input is a string
				await ctx.setState({ count: 5 });
				expectTypeOf(ctx.state?.count).toEqualTypeOf<number | undefined>();
			});
		}
	});

	test("an invoke whose payload does not match the schema throws before anything is stored", async () => {
		await expect(
			// @ts-expect-error: `to` must be a string
			conductor.invoke({ name: "send" }, { to: 1 }),
		).rejects.toThrow('Invalid payload for task "send": to:');
		await expect(
			// @ts-expect-error: `to` must be a string
			conductor.invoke({ name: "send" }, [{ payload: { to: 1 } }]),
		).rejects.toThrow('Invalid payload for task "send"');
	});
});

describe("invoke returns null only when it may throttle", () => {
	test("an invoke without throttle returns an id", () => {
		if (false) {
			expectTypeOf(
				conductor.invoke({ name: "send" }, { to: "a" }),
			).resolves.toEqualTypeOf<string>();
			expectTypeOf(
				conductor.invoke({ name: "send" }, { to: "a" }, { dedupe_key: "k" }),
			).resolves.toEqualTypeOf<string>();
			expectTypeOf(
				conductor.invoke({ name: "send" }, [{ payload: { to: "a" } }]),
			).resolves.toEqualTypeOf<string[]>();
		}
	});

	test("a throttled invoke may return null", () => {
		if (false) {
			expectTypeOf(
				conductor.invoke({ name: "send" }, { to: "a" }, { throttle: { seconds: 1 } }),
			).resolves.toEqualTypeOf<string | null>();
			const opts: { throttle?: { seconds: number } } = {};
			expectTypeOf(conductor.invoke({ name: "send" }, { to: "a" }, opts)).resolves.toEqualTypeOf<
				string | null
			>();
			expectTypeOf(
				conductor.invoke({ name: "send" }, [
					{ payload: { to: "a" } },
					{ payload: { to: "b" }, throttle: { seconds: 1 } },
				]),
			).resolves.toEqualTypeOf<(string | null)[]>();
		}
	});
});
