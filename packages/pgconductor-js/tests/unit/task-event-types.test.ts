import { test, expect, describe } from "bun:test";
import { expectTypeOf } from "expect-type";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
// import { defineEvent } from "../../src/event-definition";
import { TaskSchemas /*, EventSchemas */ } from "../../src/schemas";
import { z } from "zod";
import type { Database } from "../database.types";

describe("task event types", () => {
	test("createTask with discriminated union - cron has no payload, invoke has payload", () => {
		const taskDef = defineTask({
			name: "test-task",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "test-task" },
			[{ invocable: true }, { cron: "0 0 * * *", name: "hourly" }],
			async (event, _ctx) => {
				expectTypeOf(event).toExtend<
					{ name: "hourly" } | { name: "pgconductor.invoke"; payload: { value: number } }
				>();

				if (event.name === "hourly") {
					expectTypeOf(event).toEqualTypeOf<{ name: "hourly" }>();

					// @ts-expect-error - cron events don't have payload
					const _invalid = event.payload;
				} else {
					expectTypeOf(event).toEqualTypeOf<{
						name: "pgconductor.invoke";
						payload: { value: number };
					}>();

					expectTypeOf(event.payload).toEqualTypeOf<{ value: number }>();
					expectTypeOf(event.payload.value).toEqualTypeOf<number>();
				}
			},
		);

		expect(task.name).toBe("test-task");
	});

	test("createTask with empty payload - cron still has no payload", () => {
		const taskDef = defineTask({
			name: "empty-task",
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "empty-task" },
			[{ invocable: true }, { cron: "*/5 * * * *", name: "every-5min" }],
			async (event, _ctx) => {
				if (event.name === "every-5min") {
					expectTypeOf(event).toEqualTypeOf<{ name: "every-5min" }>();
				} else {
					expectTypeOf(event).toExtend<{
						name: "pgconductor.invoke";
						payload: object;
					}>();
				}
			},
		);

		expect(task.name).toBe("empty-task");
	});

	test("createTask with only cron trigger", () => {
		const taskDef = defineTask({
			name: "empty-task",
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "my-cron" },
			[{ cron: "*/5 * * * *", name: "every-5min" }],
			async (event, _ctx) => {
				// Event should only be cron, no invoke event possible
				expectTypeOf(event).toEqualTypeOf<{ name: "every-5min" }>();

				// Verify it's cron
				expectTypeOf(event.name).toEqualTypeOf<"every-5min">();
			},
		);

		expect(task.name).toBe("my-cron");
	});

	test("createTask with only invocable trigger - only has invoke event", () => {
		const taskDef = defineTask({
			name: "invocable-task",
			payload: z.object({ data: z.string() }),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "invocable-task" },
			{ invocable: true },
			async (event, _ctx) => {
				// Event should only be invoke, no cron event possible
				expectTypeOf(event).toEqualTypeOf<{
					name: "pgconductor.invoke";
					payload: { data: string };
				}>();

				// Verify event properties
				expectTypeOf(event.name).toEqualTypeOf<"pgconductor.invoke">();
				expectTypeOf(event.payload).toEqualTypeOf<{ data: string }>();
				expectTypeOf(event.payload.data).toEqualTypeOf<string>();
			},
		);

		expect(task.name).toBe("invocable-task");
	});

	test("createTask with both invocable and cron triggers - has both events", () => {
		const taskDef = defineTask({
			name: "both-task",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "both-task" },
			[{ invocable: true }, { cron: "0 0 * * *", name: "hourly" }],
			async (event, _ctx) => {
				// Event can be either cron or invoke
				expectTypeOf(event).toExtend<
					{ name: "hourly" } | { name: "pgconductor.invoke"; payload: { value: number } }
				>();

				if (event.name === "hourly") {
					expectTypeOf(event).toEqualTypeOf<{ name: "hourly" }>();
				} else {
					expectTypeOf(event).toEqualTypeOf<{
						name: "pgconductor.invoke";
						payload: { value: number };
					}>();
					expectTypeOf(event.payload.value).toEqualTypeOf<number>();
				}
			},
		);

		expect(task.name).toBe("both-task");
	});

	test("type error: invocable trigger without task definition", () => {
		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([]),
			context: {},
		});

		conductor.createTask(
			{ name: "undefined-task" },
			// @ts-expect-error - invocable trigger requires task definition
			{ invocable: true },
			async (_event, _ctx) => {},
		);
	});

	test("task definition with cron-only trigger is valid", () => {
		const taskDef = defineTask({
			name: "defined-task",
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		// Tasks in catalog can have any trigger type (invocable not required)
		conductor.createTask(
			{ name: "defined-task" },
			{ cron: "0 0 * * *", name: "hourly" },
			async (_event, _ctx) => {},
		);
	});

	// Event-specific tests below are commented out until event support is re-added
	// test.skip("createTask with only custom event trigger", () => {
	// 	const userCreated = defineEvent({
	// 		name: "user.created",
	// 		payload: z.object({ userId: z.string(), email: z.string() }),
	// 	});
	//
	// 	const taskDef = defineTask({
	// 		name: "on-user-created",
	// 	});
	//
	// 	const conductor = Conductor.create({
	// 		sql: {} as any,
	// 		tasks: TaskSchemas.fromSchema([taskDef]),
	// 		events: EventSchemas.fromSchema([userCreated]),
	// 		context: {},
	// 	});
	//
	// 	const task = conductor.createTask(
	// 		{ name: "on-user-created" },
	// 		{ event: "user.created" },
	// 		async (event, _ctx) => {
	// 			// Event should only be the custom event
	// 			expectTypeOf(event).toEqualTypeOf<{
	// 				event: "user.created";
	// 				payload: { userId: string; email: string };
	// 			}>();
	//
	// 			expectTypeOf(event.name).toEqualTypeOf<"user.created">();
	// 			expectTypeOf(event.payload.userId).toEqualTypeOf<string>();
	// 			expectTypeOf(event.payload.email).toEqualTypeOf<string>();
	// 		},
	// 	);
	//
	// 	expect(task.name).toBe("on-user-created");
	// });
	//
	// test.skip("createTask with custom event and invocable triggers", () => {
	// 	const orderPlaced = defineEvent({
	// 		name: "order.placed",
	// 		payload: z.object({ orderId: z.number(), total: z.number() }),
	// 	});
	//
	// 	const taskDef = defineTask({
	// 		name: "process-order",
	// 		payload: z.object({ manualOrderId: z.number() }),
	// 	});
	//
	// 	const conductor = Conductor.create({
	// 		sql: {} as any,
	// 		tasks: TaskSchemas.fromSchema([taskDef]),
	// 		events: EventSchemas.fromSchema([orderPlaced]),
	// 		context: {},
	// 	});
	//
	// 	const task = conductor.createTask(
	// 		{ name: "process-order" },
	// 		[{ invocable: true }, { event: "order.placed" }],
	// 		async (event, _ctx) => {
	// 			// Event can be either invoke or custom event
	// 			if (event.name === "pgconductor.invoke") {
	// 				expectTypeOf(event.payload).toEqualTypeOf<{
	// 					manualOrderId: number;
	// 				}>();
	// 			} else if (event.name === "order.placed") {
	// 				expectTypeOf(event.payload).toEqualTypeOf<{
	// 					orderId: number;
	// 					total: number;
	// 				}>();
	// 			}
	// 		},
	// 	);
	//
	// 	expect(task.name).toBe("process-order");
	// });
	//
	// test.skip("type error: custom event trigger without event definition", () => {
	// 	const taskDef = defineTask({
	// 		name: "undefined-event-task",
	// 	});
	//
	// 	const conductor = Conductor.create({
	// 		sql: {} as any,
	// 		tasks: TaskSchemas.fromSchema([taskDef]),
	// 		events: EventSchemas.fromSchema([]), // No events defined
	// 		context: {},
	// 	});
	//
	// 	conductor.createTask(
	// 		{ name: "undefined-event-task" },
	// 		// @ts-expect-error - event trigger requires event definition
	// 		{ event: "unknown.event" },
	// 		async (_event, _ctx) => {},
	// 	);
	// });
});
