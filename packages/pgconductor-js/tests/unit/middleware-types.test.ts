import { describe, expect, test } from "vitest";
import { expectTypeOf } from "expect-type";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import type { TaskContext } from "../../src/task-context";
import type { Middleware, MiddlewareExecution } from "../../src/index";

const task = defineTask({ name: "middleware.typed", payload: z.object({ id: z.string() }) });
const child = defineTask({
	name: "middleware.child",
	payload: z.object({ n: z.number() }),
	returns: z.object({ ok: z.boolean() }),
});

describe("middleware types", () => {
	test("infers keys added by every middleware into the handler context", () => {
		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([task]),
			context: { region: "eu" },
			middleware: [
				async ({ execution, ctx }, next) => {
					expectTypeOf(execution).toEqualTypeOf<MiddlewareExecution>();
					expectTypeOf(ctx.region).toEqualTypeOf<string>();
					expectTypeOf(ctx.signal).toEqualTypeOf<AbortSignal>();
					return next({ ...ctx, thread: { id: execution.id } });
				},
				async ({ ctx }, next) => next({ ...ctx, requestId: 1 }),
			],
		});

		conductor.createTask({ name: "middleware.typed" }, { invocable: true }, async (event, ctx) => {
			expectTypeOf(event.payload).toEqualTypeOf<{ id: string }>();
			expectTypeOf(ctx.thread).toEqualTypeOf<{ id: string }>();
			expectTypeOf(ctx.requestId).toEqualTypeOf<number>();
			expectTypeOf(ctx.region).toEqualTypeOf<string>();
			await ctx.step("typed", () => ctx.thread.id);
		});
		expect(conductor).toBeDefined();
	});

	test("types execution metadata from the metadata schema", () => {
		Conductor.create({
			sql: {} as any,
			context: {},
			metadata: z.object({ tenant: z.string() }),
			middleware: [
				async ({ execution, ctx }, next) => {
					expectTypeOf(execution.metadata).toEqualTypeOf<
						Readonly<{ tenant: string }> | undefined
					>();
					return next(ctx);
				},
			],
		});
	});

	test("accepts middleware declared separately", () => {
		const withThread: Middleware<TaskContext, { thread: string }> = async (
			{ execution, ctx },
			next,
		) => next({ ...ctx, thread: execution.task_key });

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([task]),
			context: {},
			middleware: [withThread],
		});

		conductor.createTask({ name: "middleware.typed" }, { invocable: true }, async (_event, ctx) => {
			expectTypeOf(ctx.thread).toEqualTypeOf<string>();
		});
	});

	test("overrides context methods with their original signature", () => {
		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([task, child]),
			context: { region: "eu" },
			metadata: z.object({ tenant: z.string().optional() }),
			middleware: [
				async ({ ctx }, next) => {
					const invoke = ctx.invoke.bind(ctx);
					const invokeAndWait = ctx.invokeAndWait.bind(ctx);
					return next({
						invoke: async (key, task, payload, options = {}) => {
							expectTypeOf(key).toEqualTypeOf<string>();
							return invoke(key, task, payload, { ...options, metadata: { tenant: "acme" } });
						},
						invokeAndWait: async (key, task, payload, options) =>
							invokeAndWait(key, task, payload, options),
						thread: "t",
					});
				},
			],
		});
		type Context = TaskContext<
			[typeof task, typeof child],
			readonly [],
			{ tenant?: string | undefined }
		>;

		conductor.createTask({ name: "middleware.typed" }, { invocable: true }, async (_event, ctx) => {
			expectTypeOf(ctx.invoke).toEqualTypeOf<Context["invoke"]>();
			expectTypeOf(ctx.invokeAndWait).toEqualTypeOf<Context["invokeAndWait"]>();
			expectTypeOf(ctx.thread).toEqualTypeOf<string>();
			expectTypeOf(
				await ctx.invokeAndWait("child", { name: "middleware.child" }, { n: 1 }),
			).toEqualTypeOf<{ ok: boolean }>();
			// @ts-expect-error the payload keeps its type
			await ctx.invoke("child", { name: "middleware.child" }, { n: "1" });
		});
	});

	test("rejects overrides that change a context method's signature", () => {
		Conductor.create({
			sql: {} as any,
			context: { region: "eu" },
			middleware: [
				// @ts-expect-error invoke returns the execution id
				async (_args, next) => next({ invoke: async (_key: string) => 1 }),
				// @ts-expect-error region is a string
				async (_args, next) => next({ region: 1 }),
			],
		});
	});

	test("leaves the handler context unchanged without middleware", () => {
		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([task]),
			context: { region: "eu" },
		});

		conductor.createTask({ name: "middleware.typed" }, { invocable: true }, async (_event, ctx) => {
			expectTypeOf(ctx.region).toEqualTypeOf<string>();
			// @ts-expect-error no middleware adds thread
			ctx.thread;
		});
	});

	test("requires middleware to return the result of next", () => {
		Conductor.create({
			sql: {} as any,
			context: {},
			// @ts-expect-error middleware must return what next returns
			middleware: [async ({ ctx }, next) => void (await next(ctx))],
		});
	});
});
