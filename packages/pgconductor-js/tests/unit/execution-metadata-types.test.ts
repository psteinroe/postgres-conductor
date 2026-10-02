import { test, describe } from "bun:test";
import { expectTypeOf } from "expect-type";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TaskSchemas, EventSchemas } from "../../src/schemas";
import type { Payload } from "../../src/database-client";

// Mock SQL instance for type-only tests
const mockSql = Object.assign((() => Promise.resolve([{ id: "mock-id" }])) as any, {
	json: (val: any) => val,
	unsafe: () => Promise.resolve([]),
});

const ping = defineEvent({ name: "ping", payload: z.object({}) });
const worker = defineTask({ name: "worker" });

describe("execution metadata types", () => {
	test("metadata is typed by the conductor schema", () => {
		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([worker]),
			events: EventSchemas.fromSchema([ping]),
			metadata: z.object({ tenant: z.string(), replyTo: z.string().optional() }),
			context: {},
		});

		conductor.createTask({ name: "worker" }, { invocable: true }, async (_event, ctx) => {
			expectTypeOf(ctx.metadata).toEqualTypeOf<
				Readonly<{ tenant: string; replyTo?: string | undefined }> | undefined
			>();

			await ctx.invoke("child", { name: "worker" }, {}, { metadata: { tenant: "acme" } });
			await ctx.invoke(
				"child",
				{ name: "worker" },
				{},
				{
					metadata: (metadata) => {
						expectTypeOf(metadata).toEqualTypeOf<typeof ctx.metadata>();
						return { tenant: metadata?.tenant || "acme", replyTo: "thread" };
					},
				},
			);

			if (false) {
				// @ts-expect-error - metadata is readonly
				ctx.metadata = { tenant: "acme" };
				// @ts-expect-error - wrong metadata shape
				await ctx.invoke("child", { name: "worker" }, {}, { metadata: { tenant: 1 } });
			}
		});

		conductor.invoke({ name: "worker" }, {}, { metadata: { tenant: "acme" } });
		conductor.invoke({ name: "worker" }, [{ payload: {}, metadata: { tenant: "acme" } }]);
		conductor.emit("ping", {}, { metadata: { tenant: "acme" } });

		if (false) {
			// @ts-expect-error - wrong metadata shape
			conductor.invoke({ name: "worker" }, {}, { metadata: { tenant: 1 } });
			// @ts-expect-error - wrong metadata shape
			conductor.emit("ping", {}, { metadata: { other: "x" } });
		}
	});

	test("metadata is a JSON object without a schema", () => {
		const conductor = Conductor.create({
			sql: mockSql,
			tasks: TaskSchemas.fromSchema([worker]),
			context: {},
		});

		conductor.createTask({ name: "worker" }, { invocable: true }, async (_event, ctx) => {
			expectTypeOf(ctx.metadata).toEqualTypeOf<Readonly<Payload> | undefined>();
		});

		conductor.invoke({ name: "worker" }, {}, { metadata: { any: { json: [1, "two"] } } });

		if (false) {
			// @ts-expect-error - metadata must be an object
			conductor.invoke({ name: "worker" }, {}, { metadata: "acme" });
		}
	});
});
