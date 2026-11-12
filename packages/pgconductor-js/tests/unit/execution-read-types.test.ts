import { test, describe } from "vitest";
import { expectTypeOf } from "expect-type";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import type { ExecutionInfo, ListenerInfo, Payload } from "../../src/database-client";
import { defineEvent } from "../../src/event-definition";
import { defineTask, type DefineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";

const mockSql = Object.assign((() => Promise.resolve([])) as any, {
	json: (val: any) => val,
	unsafe: () => Promise.resolve([]),
});

type Request = { requestId: string; amount: number };
type WorkerState = { status: "idle" | "busy" };

// Schema-based tasks
const approval = defineTask({
	name: "approval",
	queue: "approvals",
	payload: z.object({ requestId: z.string(), amount: z.number() }),
	returns: z.object({ approved: z.boolean() }),
});
const worker = defineTask({
	name: "worker",
	payload: z.object({ workerId: z.string() }),
	state: z.object({ status: z.enum(["idle", "busy"]) }),
});

// Type-only tasks
type Session = DefineTask<{
	name: "session";
	payload: { threadId: string };
	returns: { summary: string };
	state: { turns: number };
}>;
type Untyped = DefineTask<{ name: "untyped"; payload: { id: string } }>;

const chatMessage = defineEvent({
	name: "chat.message",
	payload: z.object({ threadId: z.string() }),
});

describe("execution read types", () => {
	const conductor = Conductor.create({
		sql: mockSql,
		tasks: TaskSchemas.fromSchema([approval, worker]).fromUnion<Session | Untyped>(),
		events: [chatMessage],
		context: {},
	});

	test("getExecution without a task leaves the execution untyped", () => {
		expectTypeOf(conductor.getExecution("id")).toEqualTypeOf<Promise<ExecutionInfo | null>>();
		expectTypeOf<ExecutionInfo["payload"]>().toEqualTypeOf<Payload>();
		expectTypeOf<ExecutionInfo["state"]>().toEqualTypeOf<Payload | null>();
	});

	test("getExecution by id types the payload, state and result by a schema task", () => {
		expectTypeOf(
			conductor.getExecution({ name: "approval", queue: "approvals" }, "id"),
		).toEqualTypeOf<Promise<ExecutionInfo<{ approved: boolean }, Request, Payload> | null>>();
		expectTypeOf(conductor.getExecution({ name: "worker" }, "id")).toEqualTypeOf<
			Promise<ExecutionInfo<void, { workerId: string }, WorkerState> | null>
		>();
	});

	test("getExecution by id types the payload, state and result by a DefineTask task", () => {
		expectTypeOf(conductor.getExecution({ name: "session" }, "id")).toEqualTypeOf<
			Promise<ExecutionInfo<{ summary: string }, { threadId: string }, { turns: number }> | null>
		>();
		expectTypeOf(conductor.getExecution({ name: "untyped" }, "id")).toEqualTypeOf<
			Promise<ExecutionInfo<void, { id: string }, Payload> | null>
		>();
	});

	test("getExecution by key and dedupe key types the execution by its task", async () => {
		const request = await conductor.getExecution({
			task: { name: "approval", queue: "approvals" },
			dedupeKey: "r1",
		});
		expectTypeOf(request).toEqualTypeOf<ExecutionInfo<
			{ approved: boolean },
			Request,
			Payload
		> | null>();
		// The request is read from the payload, without parsing it
		expectTypeOf(request?.payload.amount).toEqualTypeOf<number | undefined>();

		expectTypeOf(conductor.getExecution({ task: { name: "worker" }, key: "w1" })).toEqualTypeOf<
			Promise<ExecutionInfo<void, { workerId: string }, WorkerState> | null>
		>();
		expectTypeOf(conductor.getExecution({ task: { name: "session" }, key: "t1" })).toEqualTypeOf<
			Promise<ExecutionInfo<{ summary: string }, { threadId: string }, { turns: number }> | null>
		>();
		expectTypeOf(
			conductor.getExecution({ task: { name: "untyped" }, dedupeKey: "d" }),
		).toEqualTypeOf<Promise<ExecutionInfo<void, { id: string }, Payload> | null>>();

		if (false) {
			// @ts-expect-error - unknown task
			await conductor.getExecution({ task: { name: "missing" }, dedupeKey: "d" });
			// @ts-expect-error - not a field of the payload
			request?.payload.missing;
		}
	});

	test("getExecution and listExecutions read the same types", () => {
		expectTypeOf(conductor.listExecutions({ task: { name: "session" } })).toEqualTypeOf<
			Promise<ExecutionInfo<{ summary: string }, { threadId: string }, { turns: number }>[]>
		>();
		expectTypeOf(
			conductor.listExecutions({ task: { name: "approval", queue: "approvals" } }),
		).toEqualTypeOf<Promise<ExecutionInfo<{ approved: boolean }, Request, Payload>[]>>();
	});

	test("listListeners without a task leaves the listeners untyped", () => {
		const query = { event: "chat.message", payload: { threadId: "1" } } as const;
		expectTypeOf(conductor.listListeners(query)).toEqualTypeOf<Promise<ListenerInfo[]>>();
		expectTypeOf(conductor.listListeners({ ...query, limit: 10 })).toEqualTypeOf<
			Promise<ListenerInfo[]>
		>();
		expectTypeOf<ListenerInfo>().toEqualTypeOf<ExecutionInfo & { matched: string[] }>();
	});

	test("listListeners with a task types the listeners by it", async () => {
		const event = "chat.message";
		const payload = { threadId: "1" };

		const workers = await conductor.listListeners({ event, payload, task: { name: "worker" } });
		expectTypeOf(workers).toEqualTypeOf<ListenerInfo<void, { workerId: string }, WorkerState>[]>();
		// The state is read without parsing it
		for (const listener of workers) {
			expectTypeOf(listener.state).toEqualTypeOf<WorkerState | null>();
			expectTypeOf(listener.matched).toEqualTypeOf<string[]>();
		}

		expectTypeOf(
			conductor.listListeners({
				event,
				payload,
				task: { name: "approval", queue: "approvals" },
				limit: 10,
			}),
		).toEqualTypeOf<Promise<ListenerInfo<{ approved: boolean }, Request, Payload>[]>>();
		expectTypeOf(
			conductor.listListeners({ event, payload, task: { name: "session" } }),
		).toEqualTypeOf<
			Promise<ListenerInfo<{ summary: string }, { threadId: string }, { turns: number }>[]>
		>();
		expectTypeOf(
			conductor.listListeners({ event, payload, task: { name: "untyped" } }),
		).toEqualTypeOf<Promise<ListenerInfo<void, { id: string }, Payload>[]>>();

		if (false) {
			// @ts-expect-error - unknown task
			await conductor.listListeners({ event, payload, task: { name: "missing" } });
			// @ts-expect-error - unknown event
			await conductor.listListeners({ event: "chat.missing", payload, task: { name: "worker" } });
		}
	});

	test("a task's ctx reads with the same types as the conductor", () => {
		conductor.createTask({ name: "untyped" }, { invocable: true }, async (_event, ctx) => {
			expectTypeOf(ctx.getExecution("id")).toEqualTypeOf<Promise<ExecutionInfo | null>>();
			expectTypeOf(
				ctx.getExecution({ task: { name: "approval", queue: "approvals" }, dedupeKey: "r1" }),
			).toEqualTypeOf<Promise<ExecutionInfo<{ approved: boolean }, Request, Payload> | null>>();
			expectTypeOf(ctx.getExecution({ name: "session" }, "id")).toEqualTypeOf<
				Promise<ExecutionInfo<{ summary: string }, { threadId: string }, { turns: number }> | null>
			>();
			expectTypeOf(ctx.listExecutions({ task: { name: "worker" } })).toEqualTypeOf<
				Promise<ExecutionInfo<void, { workerId: string }, WorkerState>[]>
			>();
			const event = "chat.message";
			const payload = { threadId: "1" };
			expectTypeOf(ctx.listListeners({ event, payload, task: { name: "worker" } })).toEqualTypeOf<
				Promise<ListenerInfo<void, { workerId: string }, WorkerState>[]>
			>();
			expectTypeOf(ctx.listListeners({ event, payload })).toEqualTypeOf<Promise<ListenerInfo[]>>();
			const messages = await ctx.listEvents({ name: "chat.message" });
			expectTypeOf(messages[0]?.payload.threadId).toEqualTypeOf<string | undefined>();
			expectTypeOf(ctx.countEvents({ name: "chat.message" })).toEqualTypeOf<Promise<number>>();

			if (false) {
				// @ts-expect-error - unknown task
				await ctx.getExecution({ task: { name: "missing" }, dedupeKey: "d" });
				// @ts-expect-error - unknown event
				await ctx.listEvents({ name: "chat.missing" });
			}
		});
	});
});
