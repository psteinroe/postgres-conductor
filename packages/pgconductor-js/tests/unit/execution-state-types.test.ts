import { test, describe } from "vitest";
import { expectTypeOf } from "expect-type";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import type { ExecutionInfo, Payload } from "../../src/database-client";
import { defineTask, type DefineTask, type InferState } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";

const mockSql = Object.assign((() => Promise.resolve([])) as any, {
	json: (val: any) => val,
	unsafe: () => Promise.resolve([]),
});

type AgentState = { status: "idle" | "busy"; topics: string[] };

const agent = defineTask({
	name: "agent",
	payload: z.object({ owner: z.string() }),
	state: z.object({ status: z.enum(["idle", "busy"]), topics: z.array(z.string()) }),
});
const report = defineTask({
	name: "report",
	queue: "reports",
	returns: z.object({ url: z.string() }),
	state: z.object({ progress: z.number() }),
});
const plain = defineTask({ name: "plain", payload: z.object({ id: z.string() }) });

type Counter = DefineTask<{
	name: "counter";
	payload: { start: number };
	state: { count: number };
}>;
type Untyped = DefineTask<{ name: "untyped"; payload: { id: string } }>;

describe("execution state types", () => {
	const conductor = Conductor.create({
		sql: mockSql,
		tasks: TaskSchemas.fromSchema([agent, report, plain]).fromUnion<Counter | Untyped>(),
		context: {},
	});

	test("InferState reads the state schema or type", () => {
		expectTypeOf<InferState<typeof agent>>().toEqualTypeOf<AgentState>();
		expectTypeOf<InferState<typeof report>>().toEqualTypeOf<{ progress: number }>();
		expectTypeOf<InferState<Counter>>().toEqualTypeOf<{ count: number }>();
		// Without a schema or type, any JSON object
		expectTypeOf<InferState<typeof plain>>().toEqualTypeOf<Payload>();
		expectTypeOf<InferState<Untyped>>().toEqualTypeOf<Payload>();
	});

	test("ctx.state, setState and updateState are typed by the task's state", () => {
		conductor.createTask({ name: "agent" }, { invocable: true }, async (_event, ctx) => {
			expectTypeOf(ctx.state).toEqualTypeOf<Readonly<AgentState> | null>();
			expectTypeOf(ctx.setState).parameter(0).toEqualTypeOf<AgentState | null>();
			expectTypeOf(
				ctx.updateState((state) => ({
					status: "busy",
					topics: [...(state?.topics || []), "billing"],
				})),
			).toEqualTypeOf<Promise<AgentState | null | undefined>>();
			// Returning undefined writes nothing
			await ctx.updateState(() => undefined);

			if (false) {
				// @ts-expect-error - not a status
				await ctx.setState({ status: "away", topics: [] });
				// @ts-expect-error - missing topics
				await ctx.updateState(() => ({ status: "idle" }));
			}
		});

		conductor.createTask(
			{ name: "report", queue: "reports" },
			{ invocable: true },
			async (_event, ctx) => {
				expectTypeOf(ctx.state).toEqualTypeOf<Readonly<{ progress: number }> | null>();
				return { url: "" };
			},
		);

		conductor.createTask({ name: "counter" }, { invocable: true }, async (_event, ctx) => {
			expectTypeOf(ctx.state).toEqualTypeOf<Readonly<{ count: number }> | null>();
			await ctx.updateState((state) => ({ count: (state?.count || 0) + 1 }));
			if (false) {
				// @ts-expect-error - count is a number
				await ctx.setState({ count: "1" });
			}
		});
	});

	test("without a state schema, state stays any JSON object", () => {
		conductor.createTask({ name: "plain" }, { invocable: true }, async (_event, ctx) => {
			expectTypeOf(ctx.state).toEqualTypeOf<Readonly<Payload> | null>();
			await ctx.setState({ anything: [1, "two"] });
		});
		conductor.createTask({ name: "untyped" }, { invocable: true }, async (_event, ctx) => {
			expectTypeOf(ctx.state).toEqualTypeOf<Readonly<Payload> | null>();
		});
		// A task outside the catalog
		conductor.createTask(
			{ name: "elsewhere" },
			{ cron: "* * * * *", name: "tick" },
			async (_event, ctx) => {
				expectTypeOf(ctx.state).toEqualTypeOf<Readonly<Payload> | null>();
			},
		);
	});

	test("signalWithStart takes the state of the task it starts", async () => {
		const signal = { name: "go", id: "1" };
		if (false) {
			await conductor.signalWithStart({
				task: "agent",
				key: "k",
				signal,
				state: { status: "idle", topics: [] },
			});
			await conductor.signalWithStart({
				task: "counter",
				key: "k",
				signal,
				state: { count: 0 },
			});
			await conductor.signalWithStart({ task: "plain", key: "k", signal, state: { any: 1 } });
			await conductor.signalWithStart({
				task: "agent",
				key: "k",
				signal,
				// @ts-expect-error - not the agent's state
				state: { count: 0 },
			});
		}

		conductor.createTask({ name: "plain" }, { invocable: true }, async (_event, ctx) => {
			await ctx.signalWithStart("start", {
				task: "agent",
				key: "k",
				signal,
				state: { status: "idle", topics: [] },
			});
			await ctx.signalWithStart("start-report", {
				task: "report",
				queue: "reports",
				key: "k",
				signal,
				state: { progress: 0 },
			});
			if (false) {
				await ctx.signalWithStart("start", {
					task: "counter",
					key: "k",
					signal,
					// @ts-expect-error - count is a number
					state: { count: "0" },
				});
			}
		});
	});

	test("getExecution and listExecutions type the state by task", () => {
		expectTypeOf(conductor.getExecution("id")).toEqualTypeOf<Promise<ExecutionInfo | null>>();
		expectTypeOf(conductor.getExecution({ name: "agent" }, "id")).toEqualTypeOf<
			Promise<ExecutionInfo<void, Payload, AgentState> | null>
		>();
		expectTypeOf(conductor.getExecution({ name: "report", queue: "reports" }, "id")).toEqualTypeOf<
			Promise<ExecutionInfo<{ url: string }, Payload, { progress: number }> | null>
		>();
		expectTypeOf(conductor.getExecution({ name: "plain" }, "id")).toEqualTypeOf<
			Promise<ExecutionInfo<void, Payload, Payload> | null>
		>();
		expectTypeOf(conductor.listExecutions({ task: { name: "counter" } })).toEqualTypeOf<
			Promise<ExecutionInfo<void, { start: number }, { count: number }>[]>
		>();
		expectTypeOf(conductor.listExecutions({ task: { name: "untyped" } })).toEqualTypeOf<
			Promise<ExecutionInfo<void, { id: string }, Payload>[]>
		>();
	});
});
