import { test, expect, describe } from "bun:test";
import { expectTypeOf } from "expect-type";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import {
	defineEvent,
	type DatabaseEventPayload,
	type DefineEvent,
	type InferEventPayload,
} from "../../src/event-definition";
import { TaskSchemas, EventSchemas, DatabaseSchema } from "../../src/schemas";
import { z } from "zod";
import type { Database } from "../database.types";
import type { ValidateColumns, ParseSelection } from "../../src/select-columns";

type ContactRow = Database["public"]["contact"];

describe("event types", () => {
	describe("custom events with schema", () => {
		test("EventSchemas.fromSchema infers event types correctly", () => {
			const userCreated = defineEvent({
				name: "user.created",
				payload: z.object({ userId: z.string(), email: z.string() }),
			});

			const orderPlaced = defineEvent({
				name: "order.placed",
				payload: z.object({ orderId: z.number(), total: z.number() }),
			});

			const events = EventSchemas.fromSchema([userCreated, orderPlaced]);

			// Verify the definitions array has correct length
			expectTypeOf(events.definitions).toExtend<readonly unknown[]>();
			expectTypeOf(events.definitions[0]).toExtend<{ name: "user.created" }>();
			expectTypeOf(events.definitions[1]).toExtend<{ name: "order.placed" }>();

			expect(events.definitions.length).toBe(2);
		});

		test("chaining fromSchema preserves types", () => {
			const event1 = defineEvent({
				name: "event.one",
				payload: z.object({ a: z.string() }),
			});

			const event2 = defineEvent({
				name: "event.two",
				payload: z.object({ b: z.number() }),
			});

			const events = EventSchemas.fromSchema([event1]).fromSchema([event2]);

			expectTypeOf(events.definitions[0]).toExtend<{ name: "event.one" }>();
			expectTypeOf(events.definitions[1]).toExtend<{ name: "event.two" }>();

			expect(events.definitions.length).toBe(2);
		});

		test("InferEventPayload extracts payload from schema event", () => {
			const userCreated = defineEvent({
				name: "user.created",
				payload: z.object({ userId: z.string(), email: z.string() }),
			});

			type Payload = InferEventPayload<typeof userCreated>;

			expectTypeOf<Payload>().toEqualTypeOf<{
				userId: string;
				email: string;
			}>();
		});
	});

	describe("custom events with type-only definitions", () => {
		test("DefineEvent creates correct event type", () => {
			type AccountCreated = DefineEvent<{
				name: "account.created";
				payload: { accountId: string; type: "personal" | "business" };
			}>;

			// Verify the name literal type
			expectTypeOf<AccountCreated["name"]>().toEqualTypeOf<"account.created">();

			// Verify the payload type
			expectTypeOf<AccountCreated["payload"]>().toEqualTypeOf<{
				accountId: string;
				type: "personal" | "business";
			}>();
		});

		test("DefineEvent with no payload defaults to undefined", () => {
			type SimpleEvent = DefineEvent<{
				name: "simple.event";
			}>;

			expectTypeOf<SimpleEvent["name"]>().toEqualTypeOf<"simple.event">();
			expectTypeOf<SimpleEvent["payload"]>().toEqualTypeOf<undefined>();
		});

		test("InferEventPayload works with type-only events", () => {
			type UserDeleted = DefineEvent<{
				name: "user.deleted";
				payload: { userId: string; deletedAt: number };
			}>;

			type Payload = InferEventPayload<UserDeleted>;

			expectTypeOf<Payload>().toEqualTypeOf<{
				userId: string;
				deletedAt: number;
			}>();
		});

		test("EventSchemas.fromUnion creates schema from type-only events", () => {
			type UserDeleted = DefineEvent<{
				name: "user.deleted";
				payload: { userId: string };
			}>;

			const events = EventSchemas.fromUnion<UserDeleted>();

			expect(events.definitions.length).toBe(0); // No runtime definitions
		});

		test("mixing schema and type-only events via chaining", () => {
			const schemaEvent = defineEvent({
				name: "schema.event",
				payload: z.object({ data: z.string() }),
			});

			type TypeOnlyEvent = DefineEvent<{
				name: "type.only";
				payload: { value: number };
			}>;

			const events = EventSchemas.fromSchema([
				schemaEvent,
			]).fromUnion<TypeOnlyEvent>();

			expect(events.definitions.length).toBe(1); // Only schema events have runtime definitions
		});

		test("multiple type-only events via union", () => {
			type Event1 = DefineEvent<{ name: "event.1"; payload: { a: string } }>;
			type Event2 = DefineEvent<{ name: "event.2"; payload: { b: number } }>;

			const events = EventSchemas.fromUnion<Event1 | Event2>();

			expect(events.definitions.length).toBe(0);
		});
	});

	describe("database events", () => {
		test("DatabaseSchema.fromGeneratedTypes creates database schema", () => {
			const dbSchema = DatabaseSchema.fromGeneratedTypes<Database>();

			expectTypeOf(dbSchema).toExtend<DatabaseSchema<Database>>();
		});

		test("conductor with database schema has correct types", () => {
			const taskDef = defineTask({
				name: "db-event-handler",
				payload: z.object({}),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			expect(conductor).toBeDefined();
		});

		test("database schema preserves table types", () => {
			type ContactRow = Database["public"]["contact"];

			// Verify the contact table has expected fields
			expectTypeOf<ContactRow["id"]>().toEqualTypeOf<string>();
			expectTypeOf<ContactRow["first_name"]>().toEqualTypeOf<string>();
			expectTypeOf<ContactRow["email"]>().toEqualTypeOf<string | null>();
			expectTypeOf<ContactRow["is_favorite"]>().toEqualTypeOf<boolean>();
		});
	});

	describe("event inference in task context", () => {
		test("waitForEvent returns correct payload type for custom events", () => {
			const taskDef = defineTask({
				name: "event-waiter",
				payload: z.object({}),
			});

			const userCreated = defineEvent({
				name: "user.created",
				payload: z.object({ userId: z.string(), email: z.string() }),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				events: EventSchemas.fromSchema([userCreated]),
				context: {},
			});

			const task = conductor.createTask(
				{ name: "event-waiter" },
				{ invocable: true },
				async (_event, ctx) => {
					const result = await ctx.waitForEvent("wait-step", {
						event: "user.created",
					});

					// Verify the return type is correctly inferred
					expectTypeOf(result).toEqualTypeOf<{
						userId: string;
						email: string;
					}>();
					expectTypeOf(result.userId).toEqualTypeOf<string>();
					expectTypeOf(result.email).toEqualTypeOf<string>();
				},
			);

			expect(task.name).toBe("event-waiter");
		});

		test("waitForEvent with multiple custom events infers correct type", () => {
			const taskDef = defineTask({
				name: "multi-event-waiter",
				payload: z.object({}),
			});

			const userCreated = defineEvent({
				name: "user.created",
				payload: z.object({ userId: z.string() }),
			});

			const orderPlaced = defineEvent({
				name: "order.placed",
				payload: z.object({ orderId: z.number(), total: z.number() }),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				events: EventSchemas.fromSchema([userCreated, orderPlaced]),
				context: {},
			});

			const task = conductor.createTask(
				{ name: "multi-event-waiter" },
				{ invocable: true },
				async (_event, ctx) => {
					// Each event returns its specific payload type
					const userResult = await ctx.waitForEvent("user-step", {
						event: "user.created",
					});
					expectTypeOf(userResult).toEqualTypeOf<{ userId: string }>();

					const orderResult = await ctx.waitForEvent("order-step", {
						event: "order.placed",
					});
					expectTypeOf(orderResult).toEqualTypeOf<{
						orderId: number;
						total: number;
					}>();
				},
			);

			expect(task.name).toBe("multi-event-waiter");
		});

		test("waitForEvent returns correct payload type for database insert", () => {
			const taskDef = defineTask({
				name: "db-waiter",
				payload: z.object({}),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			const task = conductor.createTask(
				{ name: "db-waiter" },
				{ invocable: true },
				async (_event, ctx) => {
					const result = await ctx.waitForEvent("insert-step", {
						schema: "public",
						table: "contact",
						operation: "insert",
					});

					// For insert: old is null, new has the row
					expectTypeOf(result.old).toEqualTypeOf<null>();
					expectTypeOf(result.new).toExtend<{
						id: string;
						first_name: string;
						email: string | null;
					}>();
					expectTypeOf(result.tg_table).toEqualTypeOf<string>();
					expectTypeOf(result.tg_op).toEqualTypeOf<"INSERT">();
				},
			);

			expect(task.name).toBe("db-waiter");
		});

		test("waitForEvent returns correct payload type for database update", () => {
			const taskDef = defineTask({
				name: "db-update-waiter",
				payload: z.object({}),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			const task = conductor.createTask(
				{ name: "db-update-waiter" },
				{ invocable: true },
				async (_event, ctx) => {
					const result = await ctx.waitForEvent("update-step", {
						schema: "public",
						table: "contact",
						operation: "update",
					});

					// For update: both old and new have the row
					expectTypeOf(result.old).toExtend<{
						id: string;
						first_name: string;
					}>();
					expectTypeOf(result.new).toExtend<{
						id: string;
						first_name: string;
					}>();
					expectTypeOf(result.tg_op).toEqualTypeOf<"UPDATE">();
				},
			);

			expect(task.name).toBe("db-update-waiter");
		});

		test("waitForEvent returns correct payload type for database delete", () => {
			const taskDef = defineTask({
				name: "db-delete-waiter",
				payload: z.object({}),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			const task = conductor.createTask(
				{ name: "db-delete-waiter" },
				{ invocable: true },
				async (_event, ctx) => {
					const result = await ctx.waitForEvent("delete-step", {
						schema: "public",
						table: "contact",
						operation: "delete",
					});

					// For delete: old has the row, new is null
					expectTypeOf(result.old).toExtend<{
						id: string;
						first_name: string;
					}>();
					expectTypeOf(result.new).toEqualTypeOf<null>();
					expectTypeOf(result.tg_op).toEqualTypeOf<"DELETE">();
				},
			);

			expect(task.name).toBe("db-delete-waiter");
		});

		test("waitForEvent with column selection infers filtered type", () => {
			const taskDef = defineTask({
				name: "db-columns-waiter",
				payload: z.object({}),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			const task = conductor.createTask(
				{ name: "db-columns-waiter" },
				{ invocable: true },
				async (_event, ctx) => {
					const result = await ctx.waitForEvent("columns-step", {
						schema: "public",
						table: "contact",
						operation: "insert",
						columns: "id, email",
					});

					// With column selection, result.new should only have selected fields
					expectTypeOf(result.new).toEqualTypeOf<{
						id: string;
						email: string | null;
					}>();

					// Verify tg_op is correct
					expectTypeOf(result.tg_op).toEqualTypeOf<"INSERT">();
				},
			);

			expect(task.name).toBe("db-columns-waiter");
		});

		test("waitForEvent with invalid column selection surfaces payload error", () => {
			const taskDef = defineTask({
				name: "db-columns-invalid",
				payload: z.object({}),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			const task = conductor.createTask(
				{ name: "db-columns-invalid" },
				{ invocable: true },
				async (_event, ctx) => {
					let result: DatabaseEventPayload<
						ContactRow,
						"insert",
						"id, email, u"
					>;

					// @ts-expect-error invalid column selection should error at config level
					result = await ctx.waitForEvent("columns-step", {
						schema: "public",
						table: "contact",
						operation: "insert",
						columns: "id, email, u",
					});

					// @ts-expect-error invalid column selection should bubble to payload access
					result.new.address_book_id;
				},
			);

			expect(task.name).toBe("db-columns-invalid");
		});

		test("waitForEvent column selection with single column", () => {
			const taskDef = defineTask({
				name: "single-col-waiter",
				payload: z.object({}),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			const task = conductor.createTask(
				{ name: "single-col-waiter" },
				{ invocable: true },
				async (_event, ctx) => {
					const result = await ctx.waitForEvent("single-step", {
						schema: "public",
						table: "contact",
						operation: "update",
						columns: "first_name",
					});

					// Should only have first_name
					expectTypeOf(result.old).toEqualTypeOf<{ first_name: string }>();
					expectTypeOf(result.new).toEqualTypeOf<{ first_name: string }>();
				},
			);

			expect(task.name).toBe("single-col-waiter");
		});

		test("waitForEvent type error for invalid event name", () => {
			const taskDef = defineTask({
				name: "error-waiter",
				payload: z.object({}),
			});

			const userCreated = defineEvent({
				name: "user.created",
				payload: z.object({ userId: z.string() }),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				events: EventSchemas.fromSchema([userCreated]),
				context: {},
			});

			conductor.createTask(
				{ name: "error-waiter" },
				{ invocable: true },
				async (_event, ctx) => {
					// @ts-expect-error - invalid event name
					await ctx.waitForEvent("step", { event: "nonexistent.event" });
				},
			);
		});

		test("waitForEvent type error for invalid table name", () => {
			const taskDef = defineTask({
				name: "db-error-waiter",
				payload: z.object({}),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			conductor.createTask(
				{ name: "db-error-waiter" },
				{ invocable: true },
				async (_event, ctx) => {
					// @ts-expect-error - invalid table name
					await ctx.waitForEvent("step", {
						schema: "public",
						table: "nonexistent_table",
						operation: "insert",
					});
				},
			);
		});
	});

	describe("combined schema adapters", () => {
		test("conductor with all schema types", () => {
			const taskDef = defineTask({
				name: "full-task",
				payload: z.object({ input: z.string() }),
				returns: z.object({ output: z.string() }),
			});

			const customEvent = defineEvent({
				name: "custom.event",
				payload: z.object({ data: z.any() }),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				events: EventSchemas.fromSchema([customEvent]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: { userId: "123" },
			});

			expect(conductor).toBeDefined();
		});

		test("schema and type-only events combined with database", () => {
			const schemaEvent = defineEvent({
				name: "schema.event",
				payload: z.object({ x: z.number() }),
			});

			type TypeEvent = DefineEvent<{
				name: "type.event";
				payload: { y: string };
			}>;

			const taskDef = defineTask({
				name: "combo-task",
				payload: z.object({}),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				events: EventSchemas.fromSchema([schemaEvent]).fromUnion<TypeEvent>(),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			expect(conductor).toBeDefined();
		});
	});

	describe("column validation debug", () => {
		test("ParseSelection with valid columns", () => {
			type Result = ParseSelection<"id, email", ContactRow>;
			expectTypeOf<Result>().toEqualTypeOf<{
				id: string;
				email: string | null;
			}>();
		});

		test("ParseSelection with invalid column returns error object", () => {
			type Result = ParseSelection<"id, nonexistent", ContactRow>;
			// Should be a ColumnSelectionError wrapper with error message
			expectTypeOf<Result>().toExtend<{
				readonly __columnSelectionError: `[Column Selection Error]: ${string}`;
			}>();
		});

		test("ParseSelection with mixed valid and invalid columns returns error", () => {
			type Result = ParseSelection<"id, email, unknown", ContactRow>;
			// Should return error for "unknown", not the valid columns
			expectTypeOf<Result>().toExtend<{
				readonly __columnSelectionError: `[Column Selection Error]: ${string}`;
			}>();
		});

		test("ValidateColumns with valid columns returns original string", () => {
			type Result = ValidateColumns<"id, email", ContactRow>;
			expectTypeOf<Result>().toEqualTypeOf<"id, email">();
		});

		test("ValidateColumns with invalid column returns error string", () => {
			type Result = ValidateColumns<"id, nonexistent", ContactRow>;
			expectTypeOf<Result>().toExtend<`[Column Selection Error]: ${string}`>();
		});

		test("ValidateColumns with mixed valid/invalid returns error", () => {
			type Result = ValidateColumns<"id, email, unknown", ContactRow>;
			expectTypeOf<Result>().toExtend<`[Column Selection Error]: ${string}`>();
		});

		test("waitForEvent with invalid column should error", () => {
			const taskDef = defineTask({
				name: "test-invalid-col",
				payload: z.object({}),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			conductor.createTask(
				{ name: "test-invalid-col" },
				{ invocable: true },
				async (_event, ctx) => {
					// This should cause a type error because "unknown" is not a valid column
					// @ts-expect-error - "unknown" column doesn't exist
					await ctx.waitForEvent("step", {
						schema: "public",
						table: "contact",
						operation: "insert",
						columns: "id, email, unknown",
					});
				},
			);
		});
	});

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
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				events: EventSchemas.fromSchema([userCreated]),
				context: {},
			});

			// Task with custom event trigger - not invocable, triggered by event
			conductor.createTask(
				{ name: "on-user-created" },
				{ event: "user.created" },
				async (event) => {
					// Event should be typed as the custom event
					expectTypeOf(event).toEqualTypeOf<{
						event: "user.created";
						payload: { userId: string; email: string };
					}>();
				},
			);
		});

		test("task with database event trigger receives typed payload", () => {
			const taskDef = defineTask({
				name: "on-contact-insert",
				payload: z.object({}),
			});

			const conductor = Conductor.create({
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			// Task triggered by database insert - not invocable
			conductor.createTask(
				{ name: "on-contact-insert" },
				{ schema: "public", table: "contact", operation: "insert" },
				async (event) => {
					// Event should have database event payload with schema.table.op format
					expectTypeOf(event.event).toEqualTypeOf<"public.contact.insert">();
					expectTypeOf(event.payload.tg_op).toEqualTypeOf<"INSERT">();
					expectTypeOf(event.payload.old).toEqualTypeOf<null>();
					// new should have all contact columns
					if (event.payload.new) {
						expectTypeOf(event.payload.new.id).toEqualTypeOf<string>();
						expectTypeOf(event.payload.new.email).toEqualTypeOf<
							string | null
						>();
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
				connectionString: "postgres://test",
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
					if (event.event === "pgconductor.invoke") {
						expectTypeOf(event.payload).toEqualTypeOf<{ data: string }>();
					} else if (event.event === "user.created") {
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
				connectionString: "postgres://test",
				tasks: TaskSchemas.fromSchema([taskDef]),
				database: DatabaseSchema.fromGeneratedTypes<Database>(),
				context: {},
			});

			// Task with cron and database event trigger - neither is invocable
			conductor.createTask(
				{ name: "cron-and-event" },
				[
					{ cron: "0 * * * *" },
					{ schema: "public", table: "contact", operation: "update" },
				],
				async (event) => {
					// Event should be union of cron and database event
					if (event.event === "pgconductor.cron") {
						// Cron event has no payload
						expectTypeOf(event).toEqualTypeOf<{ event: "pgconductor.cron" }>();
					} else if (event.event === "public.contact.update") {
						// Database update event
						expectTypeOf(event.payload.tg_op).toEqualTypeOf<"UPDATE">();
					}
				},
			);
		});
	});
});
