import { describe, expectTypeOf, test } from "bun:test";
import { z } from "zod";
import { defineEvent, type FilterForEvent } from "../../src/event-definition";

describe("event filter types", () => {
	test("omitting filterable fields permits no filters", () => {
		const event = defineEvent({
			name: "account.created",
			payload: z.object({ accountId: z.string() }),
		});
		expectTypeOf<FilterForEvent<typeof event>>().toEqualTypeOf<{}>();
	});

	test("infer allowed fields and values from the payload", () => {
		const event = defineEvent({
			name: "order.changed",
			payload: z.object({ status: z.enum(["pending", "paid"]), accountId: z.string() }),
			filterable: ["status"],
		});

		expectTypeOf<FilterForEvent<typeof event>>().toEqualTypeOf<{
			readonly status?: readonly ("pending" | "paid")[];
		}>();
	});
});
