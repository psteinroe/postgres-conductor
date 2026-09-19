import { test, expect, describe } from "bun:test";
import { expectTypeOf } from "expect-type";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { defineEvent, type DefineEvent } from "../../src/event-definition";
import { TaskSchemas, EventSchemas } from "../../src/schemas";
import { z } from "zod";

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
		conductor.createTask(
			{ name: "on-user-created-fields" },
			{ event: "user.created", fields: "userId,email" },
			async (event) => {
				// Event name should be correct
				expectTypeOf(event.name).toEqualTypeOf<"user.created">();

				// Payload should only have selected fields
				expectTypeOf(event.payload).toEqualTypeOf<{
					userId: string;
					email: string;
				}>();

				// Selected fields should be accessible
				expectTypeOf(event.payload.userId).toEqualTypeOf<string>();
				expectTypeOf(event.payload.email).toEqualTypeOf<string>();

				// Non-selected fields should cause type errors
				// @ts-expect-error - name was not selected
				event.payload.name;

				// @ts-expect-error - plan was not selected
				event.payload.plan;
			},
		);
	});

	test("custom event triggers validate event names and projected fields", () => {
		const userCreated = defineEvent({
			name: "user.created",
			payload: z.object({ userId: z.string(), email: z.string() }),
		});
		const taskDef = defineTask({ name: "on-user-created", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		if (false) {
			conductor.createTask(
				{ name: "on-user-created" },
				// @ts-expect-error The event is not in the conductor catalog.
				{ event: "user.typo" },
				async () => {},
			);
			conductor.createTask(
				{ name: "on-user-created" },
				// @ts-expect-error The selected field is not in the event payload.
				{ event: "user.created", fields: "userId,missing" },
				async () => {},
			);
			conductor.createTask(
				{ name: "on-user-created" },
				// @ts-expect-error Selected event fields must be unique.
				{ event: "user.created", fields: "userId,userId" },
				async () => {},
			);
			conductor.createTask(
				{ name: "on-user-created" },
				// @ts-expect-error Event field names use unquoted identifier syntax.
				{ event: "user.created", fields: '"userId"' },
				async () => {},
			);
		}

		expect(() =>
			conductor.createTask(
				{ name: "on-user-created" },
				{ event: "user.typo" } as any,
				async () => {},
			),
		).toThrow('Event "user.typo" is not defined in the conductor event catalog');
		expect(() =>
			conductor.createTask(
				{ name: "on-user-created" },
				{ event: "user.created", fields: "userId,userId" } as any,
				async () => {},
			),
		).toThrow('Fields for event "user.created" cannot contain duplicate names');
		expect(() =>
			conductor.createTask(
				{ name: "on-user-created" },
				{ event: "user.created", fields: '"userId"' } as any,
				async () => {},
			),
		).toThrow('Fields for event "user.created" contains invalid field');
	});

	test("type-only events remain valid beside runtime event definitions", () => {
		type ExternalEvent = DefineEvent<{
			name: "external.received";
			payload: { externalId: string };
			filterable: ["externalId"];
		}>;
		const runtimeEvent = defineEvent({
			name: "user.created",
			payload: z.object({ userId: z.string() }),
		});
		const taskDef = defineTask({ name: "external-task", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([runtimeEvent]).fromUnion<ExternalEvent>(),
			context: {},
		});

		expect(() =>
			conductor.createTask(
				{ name: "external-task" },
				{ event: "external.received", filter: { externalId: ["external-1"] } },
				async () => {},
			),
		).not.toThrow();
	});

	test("filterable event fields must contain scalar values", () => {
		if (false) {
			// @ts-expect-error Object-valued payload fields cannot be filterable.
			defineEvent({
				name: "user.metadata",
				payload: z.object({ userId: z.string(), metadata: z.object({ source: z.string() }) }),
				filterable: ["metadata"],
			});
		}
	});

	test("custom event trigger rejects a when clause", () => {
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

		expect(() =>
			conductor.createTask(
				{ name: "on-large-order" },
				{ event: "order.placed", when: "new.payload->>'total'::numeric > 1000" },
				async (event) => {
					expectTypeOf(event.payload).toEqualTypeOf<{
						orderId: string;
						total: number;
					}>();
				},
			),
		).toThrow("does not support a when clause");
	});

	test("managed database trigger configuration is not exposed", () => {
		const taskDef = defineTask({ name: "database-change", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: {} as any,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
			// @ts-expect-error Database schemas are not a Conductor option.
			database: {},
		});

		conductor.createTask(
			{ name: "database-change" },
			// @ts-expect-error Applications own database triggers and emit custom events from them.
			{ schema: "public", table: "contact", operation: "insert", columns: "id" },
			async () => {},
		);
	});
});
