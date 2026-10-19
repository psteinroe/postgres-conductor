import { describe, expect, test } from "bun:test";
import { expectTypeOf } from "expect-type";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import type { TaskContext } from "../../src/task-context";
import type { Middleware, MiddlewareExecution } from "../../src/index";

const task = defineTask({ name: "middleware.typed", payload: z.object({ id: z.string() }) });

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
