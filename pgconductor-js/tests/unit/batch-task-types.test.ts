// @ts-nocheck - Event support commented out
import { test, expect, describe } from "bun:test";
import { expectTypeOf } from "expect-type";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TaskSchemas, EventSchemas } from "../../src/schemas";
import { z } from "zod";
import type { BatchTaskContext } from "../../src/task-context";

describe.skip("batch task types", () => {
	test("batch task with void return - receives array of events", () => {
		const taskDef = defineTask({
			name: "batch-task",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "batch-task", batch: { size: 10, timeoutMs: 5000 } },
			{ invocable: true },
			async (events, ctx) => {
				// Events should be an array
				expectTypeOf(events).toEqualTypeOf<
					Array<{ event: "pgconductor.invoke"; payload: { value: number } }>
				>();

				// Each event should have the correct shape
				expectTypeOf(events[0]!.event).toEqualTypeOf<"pgconductor.invoke">();
				expectTypeOf(events[0]!.payload.value).toEqualTypeOf<number>();

				// Context should be BatchTaskContext
				expectTypeOf(ctx).toEqualTypeOf<BatchTaskContext>();

				// Batch context has signal and logger
				expectTypeOf(ctx.signal).toMatchTypeOf<AbortSignal>();
				expectTypeOf(ctx.logger).toMatchTypeOf<{
					info: (...args: any[]) => void;
					warn: (...args: any[]) => void;
					error: (...args: any[]) => void;
					debug: (...args: any[]) => void;
				}>();

				// Return void (no array needed)
			},
		);

		expect(task.name).toBe("batch-task");
	});

	test("batch task with returns - must return array matching input length", () => {
		const taskDef = defineTask({
			name: "batch-with-results",
			payload: z.object({ value: z.number() }),
			returns: z.object({ doubled: z.number() }),
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "batch-with-results", batch: { size: 5, timeoutMs: 5000 } },
			{ invocable: true },
			async (events, ctx) => {
				// Events should be an array
				expectTypeOf(events).toEqualTypeOf<
					Array<{ event: "pgconductor.invoke"; payload: { value: number } }>
				>();

				// Context should be BatchTaskContext
				expectTypeOf(ctx).toEqualTypeOf<BatchTaskContext>();

				// Must return array of results
				const results: Array<{ doubled: number }> = events.map((e) => ({
					doubled: e.payload.value * 2,
				}));

				expectTypeOf(results).toEqualTypeOf<Array<{ doubled: number }>>();

				return results;
			},
		);

		expect(task.name).toBe("batch-with-results");
	});

	test("batch task with cron and invocable triggers", () => {
		const taskDef = defineTask({
			name: "batch-mixed",
			payload: z.object({ count: z.number() }),
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "batch-mixed", batch: { size: 20, timeoutMs: 5000 } },
			[{ invocable: true }, { cron: "*/10 * * * *", name: "every-10min" }],
			async (events, ctx) => {
				// Events array can contain either cron or invoke events
				expectTypeOf(events).toEqualTypeOf<
					Array<
						{ event: "every-10min" } | { event: "pgconductor.invoke"; payload: { count: number } }
					>
				>();

				expectTypeOf(ctx).toEqualTypeOf<BatchTaskContext>();

				// Can discriminate on each event
				for (const event of events) {
					if (event.event === "every-10min") {
						expectTypeOf(event).toEqualTypeOf<{ event: "every-10min" }>();

						// @ts-expect-error - cron events don't have payload
						const _invalid = event.payload;
					} else {
						expectTypeOf(event).toEqualTypeOf<{
							event: "pgconductor.invoke";
							payload: { count: number };
						}>();
						expectTypeOf(event.payload.count).toEqualTypeOf<number>();
					}
				}
			},
		);

		expect(task.name).toBe("batch-mixed");
	});

	test("batch task with custom event trigger", () => {
		const orderPlaced = defineEvent({
			name: "order.placed",
			payload: z.object({ orderId: z.string(), amount: z.number() }),
		});

		const taskDef = defineTask({
			name: "batch-orders",
			returns: z.object({ processed: z.boolean() }),
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([orderPlaced]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "batch-orders", batch: { size: 50, timeoutMs: 5000 } },
			{ event: "order.placed" },
			async (events, ctx) => {
				// Events should be array of custom events
				expectTypeOf(events).toEqualTypeOf<
					Array<{
						event: "order.placed";
						payload: { orderId: string; amount: number };
					}>
				>();

				expectTypeOf(ctx).toEqualTypeOf<BatchTaskContext>();

				// Process batch
				expectTypeOf(events[0]!.payload.orderId).toEqualTypeOf<string>();
				expectTypeOf(events[0]!.payload.amount).toEqualTypeOf<number>();

				// Return results array
				return events.map(() => ({ processed: true }));
			},
		);

		expect(task.name).toBe("batch-orders");
	});

	test("batch task with only cron trigger", () => {
		const taskDef = defineTask({
			name: "batch-cron",
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "batch-cron", batch: { size: 10, timeoutMs: 5000 } },
			{ cron: "0 0 * * *", name: "daily" },
			async (events, ctx) => {
				// Events should be array with only cron events
				expectTypeOf(events).toEqualTypeOf<Array<{ event: "daily" }>>();

				expectTypeOf(ctx).toEqualTypeOf<BatchTaskContext>();

				// All events should be the same cron trigger
				for (const event of events) {
					expectTypeOf(event.event).toEqualTypeOf<"daily">();
				}
			},
		);

		expect(task.name).toBe("batch-cron");
	});

	test("batch task with multiple custom events and invocable", () => {
		const userCreated = defineEvent({
			name: "user.created",
			payload: z.object({ userId: z.string() }),
		});

		const userDeleted = defineEvent({
			name: "user.deleted",
			payload: z.object({ userId: z.string(), reason: z.string() }),
		});

		const taskDef = defineTask({
			name: "batch-user-sync",
			payload: z.object({ syncAll: z.boolean() }),
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userCreated, userDeleted]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "batch-user-sync", batch: { size: 100, timeoutMs: 1000 } },
			[{ invocable: true }, { event: "user.created" }, { event: "user.deleted" }],
			async (events, ctx) => {
				// Events can be any of the three types
				expectTypeOf(events).toEqualTypeOf<
					Array<
						| { event: "pgconductor.invoke"; payload: { syncAll: boolean } }
						| { event: "user.created"; payload: { userId: string } }
						| { event: "user.deleted"; payload: { userId: string; reason: string } }
					>
				>();

				expectTypeOf(ctx).toEqualTypeOf<BatchTaskContext>();

				// Discriminate on event type
				for (const event of events) {
					if (event.event === "pgconductor.invoke") {
						expectTypeOf(event.payload.syncAll).toEqualTypeOf<boolean>();
					} else if (event.event === "user.created") {
						expectTypeOf(event.payload.userId).toEqualTypeOf<string>();

						// @ts-expect-error - created event doesn't have reason
						const _invalid = event.payload.reason;
					} else if (event.event === "user.deleted") {
						expectTypeOf(event.payload.userId).toEqualTypeOf<string>();
						expectTypeOf(event.payload.reason).toEqualTypeOf<string>();
					}
				}
			},
		);

		expect(task.name).toBe("batch-user-sync");
	});

	test("batch task batch config - size and timeoutMs", () => {
		const taskDef = defineTask({
			name: "batch-config-test",
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		// Test with size and timeoutMs
		const task1 = conductor.createTask(
			{ name: "batch-config-test", batch: { size: 50, timeoutMs: 5000 } },
			{ invocable: true },
			async (events, _ctx) => {
				expectTypeOf(events).toBeArray();
			},
		);

		expect(task1.batch).toEqual({ size: 50, timeoutMs: 5000 });

		// Test with different size and timeoutMs
		const task2 = conductor.createTask(
			{ name: "batch-config-test", batch: { size: 100, timeoutMs: 2000 } },
			{ invocable: true },
			async (events, _ctx) => {
				expectTypeOf(events).toBeArray();
			},
		);

		expect(task2.batch).toEqual({ size: 100, timeoutMs: 2000 });
	});

	test("type error: batch task with returns must return array", () => {
		const taskDef = defineTask({
			name: "batch-returns",
			returns: z.object({ result: z.string() }),
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		conductor.createTask(
			{ name: "batch-returns", batch: { size: 10, timeoutMs: 5000 } },
			{ invocable: true },
			// @ts-expect-error - batch task with returns must return Array<returns>
			async (events, _ctx) => {
				// Returning single object instead of array
				return { result: "wrong" };
			},
		);
	});

	test("type error: batch task cannot use regular TaskContext methods", () => {
		const taskDef = defineTask({
			name: "batch-no-ctx",
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		conductor.createTask(
			{ name: "batch-no-ctx", batch: { size: 10, timeoutMs: 5000 } },
			{ invocable: true },
			async (_events, ctx) => {
				// BatchTaskContext doesn't have step, sleep, invoke, etc.

				// @ts-expect-error - step not available in BatchTaskContext
				await ctx.step("test", async () => 42);

				// @ts-expect-error - sleep not available in BatchTaskContext
				await ctx.sleep({ seconds: 5 });

				// @ts-expect-error - invoke not available in BatchTaskContext
				await ctx.invoke({ name: "other-task" }, {});

				// Only signal and logger are available
				expectTypeOf(ctx.signal).toMatchTypeOf<AbortSignal>();
				expectTypeOf(ctx.logger).toMatchTypeOf<{
					info: (...args: any[]) => void;
					warn: (...args: any[]) => void;
					error: (...args: any[]) => void;
					debug: (...args: any[]) => void;
				}>();
			},
		);
	});

	test("non-batch task receives single event, not array", () => {
		const taskDef = defineTask({
			name: "regular-task",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		// Without batch config, task receives single event
		const task = conductor.createTask(
			{ name: "regular-task" }, // No batch config
			{ invocable: true },
			async (event, ctx) => {
				// Event is NOT an array
				expectTypeOf(event).toEqualTypeOf<{
					event: "pgconductor.invoke";
					payload: { value: number };
				}>();

				// @ts-expect-error - event is not an array
				const _invalid = event[0];

				// Regular context has step, sleep, invoke methods
				expectTypeOf(ctx.step).toBeFunction();
				expectTypeOf(ctx.sleep).toBeFunction();
				expectTypeOf(ctx.invoke).toBeFunction();
			},
		);

		expect(task.name).toBe("regular-task");
		expect(task.batch).toBeUndefined();
	});
});
