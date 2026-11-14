import { test, describe } from "vitest";
import { expectTypeOf } from "expect-type";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import type { JsonValue } from "../../src/database-client";
import type { SignalBranch } from "../../src/task-context";
import {
	defineTask,
	type DefineTask,
	type InferSignalInputs,
	type InferSignals,
} from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";

const mockSql = Object.assign((() => Promise.resolve([])) as any, {
	json: (val: any) => val,
	unsafe: () => Promise.resolve([]),
});

const Delivery = z.object({ text: z.string(), from: z.string() });
type Delivery = z.infer<typeof Delivery>;
// A default makes the field optional for senders; handlers receive it
const Withdraw = z.object({ reason: z.string().default("none") });
type Withdraw = z.output<typeof Withdraw>;
// An interface is not assignable to JsonValue, and still types a signal
interface Decision {
	status: "approved" | "rejected";
	by: string;
}

const session = defineTask({
	name: "session",
	payload: z.object({ thread: z.string() }),
	signals: { input: Delivery, withdraw: Withdraw },
});
const report = defineTask({
	name: "report",
	queue: "reports",
	signals: { progress: z.number() },
});
const plain = defineTask({ name: "plain", payload: z.object({ id: z.string() }) });

type Approval = DefineTask<{
	name: "approval";
	payload: { requestId: string };
	signals: { decision: Decision };
}>;
type Untyped = DefineTask<{ name: "untyped"; payload: { id: string } }>;

describe("signal types", () => {
	const conductor = Conductor.create({
		sql: mockSql,
		tasks: TaskSchemas.fromSchema([session, report, plain]).fromUnion<Approval | Untyped>(),
		context: {},
	});

	test("InferSignals reads the signal schemas or types", () => {
		expectTypeOf<InferSignals<typeof session>>().toEqualTypeOf<{
			input: Delivery;
			withdraw: Withdraw;
		}>();
		expectTypeOf<InferSignals<typeof report>>().toEqualTypeOf<{ progress: number }>();
		expectTypeOf<InferSignals<Approval>>().toEqualTypeOf<{ decision: Decision }>();
		// Without signals, nothing is declared
		expectTypeOf<InferSignals<typeof plain>>().toEqualTypeOf<{}>();
		expectTypeOf<InferSignals<Untyped>>().toEqualTypeOf<{}>();
	});

	test("InferSignalInputs reads what senders pass: the schemas' inputs", () => {
		expectTypeOf<InferSignalInputs<typeof session>>().toEqualTypeOf<{
			input: Delivery;
			withdraw: z.input<typeof Withdraw>;
		}>();
		expectTypeOf<z.input<typeof Withdraw>>().toEqualTypeOf<{ reason?: string | undefined }>();
		expectTypeOf<InferSignalInputs<Approval>>().toEqualTypeOf<{ decision: Decision }>();
	});

	test("ctx.onSignal types the payload of a declared signal", () => {
		conductor.createTask({ name: "session" }, { invocable: true }, async (_event, ctx) => {
			ctx.onSignal("input", (payload, info) => {
				expectTypeOf(payload).toEqualTypeOf<Delivery>();
				expectTypeOf(info).toEqualTypeOf<{ id: string; name: string; listens: string[] }>();
			});
			ctx.onSignal("withdraw", (payload) => {
				expectTypeOf(payload).toEqualTypeOf<Withdraw>();
			});
			// A name the task does not declare, such as an event's from ctx.listen(), is untyped
			ctx.onSignal("chat.message", (payload) => {
				expectTypeOf(payload).toEqualTypeOf<JsonValue>();
			});
			// An undeclared name's handler types its payload, unchecked as before
			ctx.onSignal("note", (payload: string) => payload);
			ctx.onSignal<{ id: string }>("ping", (payload) => {
				expectTypeOf(payload).toEqualTypeOf<{ id: string }>();
			});

			if (false) {
				// @ts-expect-error - the payload of input is a Delivery
				ctx.onSignal("input", (payload: string) => payload);
				// @ts-expect-error - text is a string
				ctx.onSignal("input", (payload: { text: number }) => payload);
			}
		});

		conductor.createTask({ name: "approval" }, { invocable: true }, async (_event, ctx) => {
			ctx.onSignal("decision", (payload) => {
				expectTypeOf(payload).toEqualTypeOf<Decision>();
			});
		});
	});

	test("without signals, ctx.onSignal behaves as before", () => {
		conductor.createTask({ name: "plain" }, { invocable: true }, async (_event, ctx) => {
			ctx.onSignal("input", (payload) => {
				expectTypeOf(payload).toEqualTypeOf<JsonValue>();
			});
			ctx.onSignal<Decision>("decision", (payload) => {
				expectTypeOf(payload).toEqualTypeOf<Decision>();
			});
			ctx.onSignal("note", (payload: string) => payload);
		});
		// A task outside the catalog
		conductor.createTask(
			{ name: "elsewhere" },
			{ cron: "* * * * *", name: "tick" },
			async (_event, ctx) => {
				ctx.onSignal("input", (payload) => {
					expectTypeOf(payload).toEqualTypeOf<JsonValue>();
				});
			},
		);
	});

	test("waitForAny types a signal branch by the task's declaration", () => {
		conductor.createTask({ name: "session" }, { invocable: true }, async (_event, ctx) => {
			const note: SignalBranch<{ note: string }> = { signal: "note" };
			expectTypeOf(
				await ctx.waitForAny("idle", {
					input: { signal: "input" },
					withdrawn: { signal: "withdraw" },
					listened: { signal: "chat.message" },
					note,
				}),
			).toEqualTypeOf<
				| { key: "input"; signal: { id: string; name: string; payload: Delivery } }
				| { key: "withdrawn"; signal: { id: string; name: string; payload: Withdraw } }
				| { key: "listened"; signal: { id: string; name: string; payload: JsonValue } }
				| { key: "note"; signal: { id: string; name: string; payload: { note: string } } }
				| { key: "timeout" }
			>();
		});

		conductor.createTask({ name: "plain" }, { invocable: true }, async (_event, ctx) => {
			expectTypeOf(await ctx.waitForAny("idle", { input: { signal: "input" } })).toEqualTypeOf<
				| { key: "input"; signal: { id: string; name: string; payload: JsonValue } }
				| { key: "timeout" }
			>();
		});
	});

	test("conductor.signal types the payload by the target task", async () => {
		const delivery: Delivery = { text: "hi", from: "u1" };
		if (false) {
			await conductor.signal(
				{ task: { name: "session" }, key: "k" },
				{ name: "input", id: "1", payload: delivery },
			);
			await conductor.signal(
				{ task: { name: "session" }, id: "execution-id" },
				{
					name: "withdraw",
					id: "2",
					payload: {},
				},
			);
			await conductor.signal(
				{ task: { name: "report", queue: "reports" }, key: "k" },
				{ name: "progress", id: "3", payload: 0.5 },
			);
			await conductor.signal(
				{ task: { name: "approval" }, key: "k" },
				{ name: "decision", id: "4", payload: { status: "approved", by: "u1" } },
			);
			// Undeclared names and tasks without signals take any JSON payload, or none
			await conductor.signal(
				{ task: { name: "session" }, key: "k" },
				{ name: "chat.message", id: "5", payload: [1, "two"] },
			);
			await conductor.signal(
				{ task: { name: "plain" }, id: "execution-id" },
				{ name: "input", id: "6" },
			);
			// By execution id alone, untyped as before
			await conductor.signal("execution-id", { name: "input", id: "7", payload: "anything" });

			// @ts-expect-error - not a Delivery
			await conductor.signal(
				{ task: { name: "session" }, key: "k" },
				{ name: "input", id: "1", payload: { text: 1 } },
			);
			// @ts-expect-error - a declared signal needs its payload
			await conductor.signal(
				{ task: { name: "session" }, id: "execution-id" },
				{ name: "input", id: "1" },
			);
			// @ts-expect-error - not a status
			await conductor.signal(
				{ task: { name: "approval" }, key: "k" },
				{ name: "decision", id: "4", payload: { status: "maybe", by: "u1" } },
			);
			// @ts-expect-error - not a task of the catalog
			await conductor.signal(
				{ task: { name: "missing" }, id: "execution-id" },
				{ name: "input", id: "1" },
			);
		}
	});

	test("signalWithStart types the payload by the target task", async () => {
		if (false) {
			await conductor.signalWithStart({
				task: "session",
				key: "k",
				signal: { name: "input", id: "1", payload: { text: "hi", from: "u1" } },
				input: { thread: "t" },
			});
			await conductor.signalWithStart({
				task: "session",
				key: "k",
				signal: { name: "chat.message", id: "2", payload: null },
				input: { thread: "t" },
			});
			await conductor.signalWithStart({
				task: "report",
				queue: "reports",
				key: "k",
				signal: { name: "progress", id: "3", payload: 1 },
			});
			await conductor.signalWithStart({
				task: "session",
				key: "k",
				// @ts-expect-error - not a Delivery
				signal: { name: "input", id: "1", payload: "hi" },
				input: { thread: "t" },
			});
		}
	});

	test("ctx.sendSignal and ctx.signalWithStart type the payload by the target task", () => {
		conductor.createTask({ name: "plain" }, { invocable: true }, async (_event, ctx) => {
			await ctx.sendSignal(
				{ task: { name: "session" }, key: "k" },
				{ name: "input", id: "1", payload: { text: "hi", from: "u1" } },
			);
			await ctx.sendSignal(
				{ task: { name: "approval" }, id: "execution-id" },
				{
					name: "decision",
					id: "2",
					payload: { status: "rejected", by: "u2" },
				},
			);
			await ctx.sendSignal("execution-id", { name: "input", id: "3", payload: 1 });
			await ctx.signalWithStart("start", {
				task: "session",
				key: "k",
				signal: { name: "withdraw", id: "4", payload: {} },
				input: { thread: "t" },
			});
			if (false) {
				// @ts-expect-error - not a Delivery
				await ctx.sendSignal(
					{ task: { name: "session" }, id: "execution-id" },
					{ name: "input", id: "1", payload: { from: "u1" } },
				);
				await ctx.signalWithStart("start", {
					task: "approval",
					key: "k",
					// @ts-expect-error - a declared signal needs its payload
					signal: { name: "decision", id: "1" },
					input: { requestId: "r" },
				});
			}
		});
	});
});
