import { test, expect, describe } from "bun:test";
import { expectTypeOf } from "expect-type";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TaskSchemas, EventSchemas, DatabaseSchema } from "../../src/schemas";
import { z } from "zod";
import type { Database } from "../database.types";

describe("task event types", () => {
	test("createTask with discriminated union - cron has no payload, invoke has payload", () => {
		const taskDef = defineTask({
			name: "test-task",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "test-task" },
			[{ invocable: true }, { cron: "0 0 * * *", name: "hourly" }],
			async (event, _ctx) => {
				expectTypeOf(event).toExtend<
					| { event: "hourly" }
					| { event: "pgconductor.invoke"; payload: { value: number } }
				>();

				if (event.event === "hourly") {
					expectTypeOf(event).toEqualTypeOf<{ event: "hourly" }>();

					// @ts-expect-error - cron events don't have payload
					const _invalid = event.payload;
				} else {
					expectTypeOf(event).toEqualTypeOf<{
						event: "pgconductor.invoke";
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
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "empty-task" },
			[{ invocable: true }, { cron: "*/5 * * * *", name: "every-5min" }],
			async (event, _ctx) => {
				if (event.event === "every-5min") {
					expectTypeOf(event).toEqualTypeOf<{ event: "every-5min" }>();
				} else {
					expectTypeOf(event).toExtend<{
						event: "pgconductor.invoke";
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
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "my-cron" },
			[{ cron: "*/5 * * * *", name: "every-5min" }],
			async (event, _ctx) => {
				// Event should only be cron, no invoke event possible
				expectTypeOf(event).toEqualTypeOf<{ event: "every-5min" }>();

				// Verify it's cron
				expectTypeOf(event.event).toEqualTypeOf<"every-5min">();
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
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "invocable-task" },
			{ invocable: true },
			async (event, _ctx) => {
				// Event should only be invoke, no cron event possible
				expectTypeOf(event).toEqualTypeOf<{
					event: "pgconductor.invoke";
					payload: { data: string };
				}>();

				// Verify event properties
				expectTypeOf(event.event).toEqualTypeOf<"pgconductor.invoke">();
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
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "both-task" },
			[{ invocable: true }, { cron: "0 0 * * *", name: "hourly" }],
			async (event, _ctx) => {
				// Event can be either cron or invoke
				expectTypeOf(event).toExtend<
					| { event: "hourly" }
					| { event: "pgconductor.invoke"; payload: { value: number } }
				>();

				if (event.event === "hourly") {
					expectTypeOf(event).toEqualTypeOf<{ event: "hourly" }>();
				} else {
					expectTypeOf(event).toEqualTypeOf<{
						event: "pgconductor.invoke";
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
			connectionString: "postgres://test",
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
			connectionString: "postgres://test",
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

	test("createTask with only custom event trigger", () => {
		const userCreated = defineEvent({
			name: "user.created",
			payload: z.object({ userId: z.string(), email: z.string() }),
		});

		const taskDef = defineTask({
			name: "on-user-created",
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "on-user-created" },
			{ event: "user.created" },
			async (event, _ctx) => {
				// Event should only be the custom event
				expectTypeOf(event).toEqualTypeOf<{
					event: "user.created";
					payload: { userId: string; email: string };
				}>();

				expectTypeOf(event.event).toEqualTypeOf<"user.created">();
				expectTypeOf(event.payload.userId).toEqualTypeOf<string>();
				expectTypeOf(event.payload.email).toEqualTypeOf<string>();
			},
		);

		expect(task.name).toBe("on-user-created");
	});

	test("createTask with only database event trigger", () => {
		const taskDef = defineTask({
			name: "on-contact-insert",
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "on-contact-insert" },
			{ schema: "public", table: "contact", operation: "insert" },
			async (event, _ctx) => {
				// Event should be the database event
				expectTypeOf(event.event).toEqualTypeOf<"public.contact.insert">();
				expectTypeOf(event.payload.tg_op).toEqualTypeOf<"INSERT">();
				expectTypeOf(event.payload.old).toEqualTypeOf<null>();

				// new should have contact row type
				if (event.payload.new) {
					expectTypeOf(event.payload.new.id).toEqualTypeOf<string>();
					expectTypeOf(event.payload.new.first_name).toEqualTypeOf<string>();
					expectTypeOf(event.payload.new.email).toEqualTypeOf<string | null>();
				}
			},
		);

		expect(task.name).toBe("on-contact-insert");
	});

	test("createTask with custom event and invocable triggers", () => {
		const orderPlaced = defineEvent({
			name: "order.placed",
			payload: z.object({ orderId: z.number(), total: z.number() }),
		});

		const taskDef = defineTask({
			name: "process-order",
			payload: z.object({ manualOrderId: z.number() }),
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([orderPlaced]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "process-order" },
			[{ invocable: true }, { event: "order.placed" }],
			async (event, _ctx) => {
				// Event can be either invoke or custom event
				if (event.event === "pgconductor.invoke") {
					expectTypeOf(event.payload).toEqualTypeOf<{
						manualOrderId: number;
					}>();
				} else if (event.event === "order.placed") {
					expectTypeOf(event.payload).toEqualTypeOf<{
						orderId: number;
						total: number;
					}>();
				}
			},
		);

		expect(task.name).toBe("process-order");
	});

	test("createTask with database event and cron triggers", () => {
		const taskDef = defineTask({
			name: "sync-contacts",
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "sync-contacts" },
			[
				{ cron: "0 * * * *", name: "hourly" },
				{ schema: "public", table: "contact", operation: "update" },
			],
			async (event, _ctx) => {
				// Event can be either cron or database event
				if (event.event === "hourly") {
					expectTypeOf(event).toEqualTypeOf<{ event: "hourly" }>();
				} else if (event.event === "public.contact.update") {
					expectTypeOf(event.payload.tg_op).toEqualTypeOf<"UPDATE">();
					// Both old and new should have values for update
					expectTypeOf(event.payload.old).not.toEqualTypeOf<null>();
					expectTypeOf(event.payload.new).not.toEqualTypeOf<null>();
				}
			},
		);

		expect(task.name).toBe("sync-contacts");
	});

	test("createTask with all trigger types", () => {
		const paymentReceived = defineEvent({
			name: "payment.received",
			payload: z.object({ paymentId: z.string(), amount: z.number() }),
		});

		const taskDef = defineTask({
			name: "audit-task",
			payload: z.object({ reason: z.string() }),
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([paymentReceived]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "audit-task" },
			[
				{ invocable: true },
				{ cron: "0 0 * * *", name: "daily" },
				{ event: "payment.received" },
				{ schema: "public", table: "contact", operation: "delete" },
			],
			async (event, _ctx) => {
				// Event can be any of the four types
				if (event.event === "pgconductor.invoke") {
					expectTypeOf(event.payload).toEqualTypeOf<{ reason: string }>();
				} else if (event.event === "daily") {
					expectTypeOf(event).toEqualTypeOf<{ event: "daily" }>();
				} else if (event.event === "payment.received") {
					expectTypeOf(event.payload.paymentId).toEqualTypeOf<string>();
					expectTypeOf(event.payload.amount).toEqualTypeOf<number>();
				} else if (event.event === "public.contact.delete") {
					expectTypeOf(event.payload.tg_op).toEqualTypeOf<"DELETE">();
					expectTypeOf(event.payload.new).toEqualTypeOf<null>();
					// old should have the deleted row
					expectTypeOf(event.payload.old).not.toEqualTypeOf<null>();
				}
			},
		);

		expect(task.name).toBe("audit-task");
	});

	test("type error: custom event trigger without event definition", () => {
		const taskDef = defineTask({
			name: "undefined-event-task",
		});

		const conductor = Conductor.create({
			connectionString: "postgres://test",
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([]), // No events defined
			context: {},
		});

		conductor.createTask(
			{ name: "undefined-event-task" },
			// @ts-expect-error - event trigger requires event definition
			{ event: "unknown.event" },
			async (_event, _ctx) => {},
		);
	});
});
