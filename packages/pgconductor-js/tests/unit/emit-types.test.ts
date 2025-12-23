import { test, expect, describe } from "bun:test";
import { expectTypeOf } from "expect-type";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TaskSchemas, EventSchemas } from "../../src/schemas";
import { z } from "zod";

// Mock SQL instance for type-only tests
const mockSql = Object.assign((() => Promise.resolve([{ id: "mock-id" }])) as any, {
	json: (val: any) => val,
	unsafe: () => Promise.resolve([]),
});

describe("emit method types", () => {
	test("conductor.emit accepts typed event payload", () => {
		const userCreated = defineEvent({
			name: "user.created",
			payload: z.object({ userId: z.string(), email: z.string() }),
		});

		const orderPlaced = defineEvent({
			name: "order.placed",
			payload: z.object({ orderId: z.number(), total: z.number() }),
		});

		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([]),
			events: EventSchemas.fromSchema([userCreated, orderPlaced]),
			context: {},
		});

		// Should accept correct payload for user.created
		expectTypeOf(
			conductor.emit("user.created", { userId: "123", email: "test@test.com" }),
		).toMatchTypeOf<Promise<string>>();

		// Should accept correct payload for order.placed
		expectTypeOf(conductor.emit("order.placed", { orderId: 456, total: 100.5 })).toMatchTypeOf<
			Promise<string>
		>();

		// @ts-expect-error - wrong payload type
		conductor.emit("user.created", { orderId: 123 });

		// @ts-expect-error - non-existent event
		conductor.emit("non.existent", {});
	});

	test("ctx.emit accepts typed event payload", () => {
		const userCreated = defineEvent({
			name: "user.created",
			payload: z.object({ userId: z.string() }),
		});

		const taskDef = defineTask({
			name: "emitter-task",
			payload: z.object({ id: z.string() }),
		});

		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		conductor.createTask({ name: "emitter-task" }, { invocable: true }, async (event, ctx) => {
			// Should accept correct payload
			expectTypeOf(ctx.emit("user.created", { userId: event.payload.id })).toMatchTypeOf<
				Promise<string>
			>();

			// @ts-expect-error - wrong payload type
			ctx.emit("user.created", { orderId: 123 });

			// @ts-expect-error - non-existent event
			ctx.emit("non.existent", {});
		});
	});
});
