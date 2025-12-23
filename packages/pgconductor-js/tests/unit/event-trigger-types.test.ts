import { test, expect, describe } from "bun:test";
import { expectTypeOf } from "expect-type";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TaskSchemas, EventSchemas, DatabaseSchema } from "../../src/schemas";
import { z } from "zod";
import type { Database } from "../database.types";

describe("event triggers", () => {
	test("task with custom event trigger receives typed event", () => {
		const userCreated = defineEvent({
			name: "user.created",
			payload: z.object({ userId: z.string(), email: z.string() }),
		});

		const taskDef = defineTask({
			name: "on-user-created",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		// Task with custom event trigger - not invocable, triggered by event
		conductor.createTask({ name: "on-user-created" }, { event: "user.created" }, async (event) => {
			// Event should be typed as the custom event
			expectTypeOf(event).toEqualTypeOf<{
				name: "user.created";
				payload: { userId: string; email: string };
			}>();
		});
	});

	test("task with database event trigger receives typed payload", () => {
		const taskDef = defineTask({
			name: "on-contact-insert",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		// Task triggered by database insert - not invocable
		conductor.createTask(
			{ name: "on-contact-insert" },
			{ schema: "public", table: "contact", operation: "insert", columns: "id,email,first_name" },
			async (event) => {
				// Event should have database event payload with schema.table.op format
				expectTypeOf(event.name).toEqualTypeOf<"public.contact.insert">();
				expectTypeOf(event.payload.tg_op).toEqualTypeOf<"INSERT">();
				expectTypeOf(event.payload.old).toEqualTypeOf<null>();
				// new should have selected contact columns
				if (event.payload.new) {
					expectTypeOf(event.payload.new.id).toEqualTypeOf<string>();
					expectTypeOf(event.payload.new.email).toEqualTypeOf<string | null>();
					expectTypeOf(event.payload.new.first_name).toEqualTypeOf<string>();
				}
			},
		);
	});

	test("task with multiple triggers including event trigger", () => {
		const userCreated = defineEvent({
			name: "user.created",
			payload: z.object({ userId: z.string() }),
		});

		const taskDef = defineTask({
			name: "multi-trigger",
			payload: z.object({ data: z.string() }),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		// Task with both invocable and custom event trigger
		conductor.createTask(
			{ name: "multi-trigger" },
			[{ invocable: true }, { event: "user.created" }],
			async (event) => {
				// Event should be union of invoke and custom event
				if (event.name === "pgconductor.invoke") {
					expectTypeOf(event.payload).toEqualTypeOf<{ data: string }>();
				} else if (event.name === "user.created") {
					expectTypeOf(event.payload).toEqualTypeOf<{ userId: string }>();
				}
			},
		);
	});

	test("task with cron and event triggers", () => {
		const taskDef = defineTask({
			name: "cron-and-event",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		// Task with cron and database event trigger - neither is invocable
		conductor.createTask(
			{ name: "cron-and-event" },
			[
				{ cron: "0 * * * *", name: "hourly" },
				{ schema: "public", table: "contact", operation: "update", columns: "id,email" },
			],
			async (event) => {
				// Event should be union of cron and database event
				if (event.name === "hourly") {
					// Cron event has no payload
					expectTypeOf(event).toEqualTypeOf<{ name: "hourly" }>();
				} else if (event.name === "public.contact.update") {
					// Database update event
					expectTypeOf(event.payload.tg_op).toEqualTypeOf<"UPDATE">();
				}
			},
		);
	});

	test("custom event trigger with field selection", () => {
		const userCreated = defineEvent({
			name: "user.created",
			payload: z.object({
				userId: z.string(),
				email: z.string(),
				name: z.string(),
				plan: z.string(),
			}),
		});

		const taskDef = defineTask({
			name: "on-user-created-fields",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		// Task with field selection - should only receive selected fields
		// Note: Field selection type inference is not yet fully implemented at compile time
		// At runtime, only selected fields will be present in the payload
		conductor.createTask(
			{ name: "on-user-created-fields" },
			{ event: "user.created", fields: "userId,email" },
			async (event) => {
				// At runtime, payload will only have selected fields (userId, email)
				// Type-level inference for field selection is not yet implemented
				// so event.payload type will show all fields from the event definition
			},
		);
	});

	test("custom event trigger with when clause", () => {
		const orderPlaced = defineEvent({
			name: "order.placed",
			payload: z.object({ orderId: z.string(), total: z.number() }),
		});

		const taskDef = defineTask({
			name: "on-large-order",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([orderPlaced]),
			context: {},
		});

		// Task with when clause - still receives full payload
		conductor.createTask(
			{ name: "on-large-order" },
			{ event: "order.placed", when: "new.payload->>'total'::numeric > 1000" },
			async (event) => {
				expectTypeOf(event.payload).toEqualTypeOf<{
					orderId: string;
					total: number;
				}>();
			},
		);
	});

	test("database trigger with column selection", () => {
		const taskDef = defineTask({
			name: "on-contact-update-columns",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		// Task with column selection - should only receive selected columns
		conductor.createTask(
			{ name: "on-contact-update-columns" },
			{ schema: "public", table: "contact", operation: "update", columns: "id,email" },
			async (event) => {
				expectTypeOf(event.name).toEqualTypeOf<"public.contact.update">();
				expectTypeOf(event.payload.tg_op).toEqualTypeOf<"UPDATE">();

				// OLD and NEW should only have selected columns
				if (event.payload.old) {
					expectTypeOf(event.payload.old).toEqualTypeOf<{
						id: string;
						email: string | null;
					}>();

					// @ts-expect-error - name was not selected
					event.payload.old.name;
				}

				if (event.payload.new) {
					expectTypeOf(event.payload.new).toEqualTypeOf<{
						id: string;
						email: string | null;
					}>();

					// @ts-expect-error - name was not selected
					event.payload.new.name;
				}
			},
		);
	});

	test("database trigger with when clause", () => {
		const taskDef = defineTask({
			name: "on-contact-active",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		// Task with when clause - still receives selected columns
		conductor.createTask(
			{ name: "on-contact-active" },
			{
				schema: "public",
				table: "contact",
				operation: "insert",
				when: "NEW.active = true",
				columns: "id,email,first_name",
			},
			async (event) => {
				if (event.payload.new) {
					expectTypeOf(event.payload.new.id).toEqualTypeOf<string>();
					expectTypeOf(event.payload.new.email).toEqualTypeOf<string | null>();
					expectTypeOf(event.payload.new.first_name).toEqualTypeOf<string>();
				}
			},
		);
	});

	test("database trigger DELETE operation has correct OLD/NEW types", () => {
		const taskDef = defineTask({
			name: "on-contact-delete",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		conductor.createTask(
			{ name: "on-contact-delete" },
			{ schema: "public", table: "contact", operation: "delete", columns: "id" },
			async (event) => {
				expectTypeOf(event.payload.tg_op).toEqualTypeOf<"DELETE">();
				// DELETE has OLD but not NEW
				expectTypeOf(event.payload.new).toEqualTypeOf<null>();
				if (event.payload.old) {
					expectTypeOf(event.payload.old.id).toEqualTypeOf<string>();
				}
			},
		);
	});

	test("database trigger UPDATE operation has both OLD and NEW", () => {
		const taskDef = defineTask({
			name: "on-contact-update",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		conductor.createTask(
			{ name: "on-contact-update" },
			{ schema: "public", table: "contact", operation: "update", columns: "id" },
			async (event) => {
				expectTypeOf(event.payload.tg_op).toEqualTypeOf<"UPDATE">();
				// UPDATE has both OLD and NEW
				if (event.payload.old) {
					expectTypeOf(event.payload.old.id).toEqualTypeOf<string>();
				}
				if (event.payload.new) {
					expectTypeOf(event.payload.new.id).toEqualTypeOf<string>();
				}
			},
		);
	});
});
