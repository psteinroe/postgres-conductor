import { describe, expect, test } from "bun:test";
import { z } from "zod";
import { defineEvent } from "../../src/event-definition";
import { compileEventTriggers } from "../../src/event-trigger-validation";
import { Task } from "../../src/task";

const catalogChanged = defineEvent({
	name: "catalog.changed",
	payload: z.object({
		a: z.union([z.string(), z.number(), z.boolean(), z.null()]),
		["__proto__"]: z.string(),
		status: z.string(),
		score: z.number(),
		name: z.string(),
		deleted: z.boolean(),
		value: z.string(),
	}),
	filterable: ["a", "__proto__", "status", "score", "name", "deleted", "value"],
});

const definitions = [catalogChanged] as const;

describe("event trigger compilation", () => {
	const compile = (filter: Record<string, unknown[]>) =>
		compileEventTriggers([{ event: "catalog.changed", filter }], definitions);

	test("compiles fields, scalar alternatives, and selected payload fields", () => {
		expect(
			compileEventTriggers(
				[
					{ invocable: true },
					{
						event: "catalog.changed",
						fields: "status, score",
						filter: { a: ["1", 1, true, null] },
					},
				],
				definitions,
			),
		).toEqual([
			{
				event_key: "catalog.changed",
				payload_fields: ["status", "score"],
				required_field_count: 1,
				terms: [
					{ field_name: "a", operator: "exact", scalar_type: "string", text_value: "1" },
					{ field_name: "a", operator: "exact", scalar_type: "number", number_value: 1 },
					{ field_name: "a", operator: "exact", scalar_type: "boolean", boolean_value: true },
					{ field_name: "a", operator: "exact", scalar_type: "null" },
				],
			},
		]);
	});

	test("preserves prototype-named filter fields", () => {
		const [compiled] = compile(JSON.parse('{"__proto__":["safe"]}') as Record<string, unknown[]>);

		expect(compiled?.terms).toEqual([
			{
				field_name: "__proto__",
				operator: "exact",
				scalar_type: "string",
				text_value: "safe",
			},
		]);
	});

	test("compiles supported operators", () => {
		const [compiled] = compile({
			status: [{ "anything-but": "blocked" }],
			score: [{ numeric: ["<", 20, ">=", 10] }],
			name: [{ prefix: "literal%_\\" }, "exact"],
			deleted: [{ exists: false }],
		});

		expect(compiled?.required_field_count).toBe(4);
		expect(compiled?.terms).toEqual([
			{
				field_name: "status",
				operator: "anything_but",
				scalar_type: "string",
				text_value: "blocked",
			},
			{
				field_name: "score",
				operator: "numeric_range",
				scalar_type: "number",
				lower_value: 10,
				lower_inclusive: true,
				upper_value: 20,
				upper_inclusive: false,
			},
			{
				field_name: "name",
				operator: "prefix",
				scalar_type: "string",
				text_value: "literal%_\\",
			},
			{
				field_name: "name",
				operator: "exact",
				scalar_type: "string",
				text_value: "exact",
			},
			{ field_name: "deleted", operator: "exists", boolean_value: false },
		]);
	});

	test("rejects filters the database cannot represent meaningfully", () => {
		expect(() => compile({ value: [] })).toThrow(/between 1 and 4 values/);
		expect(() => compile({ value: ["a", "b", "c", "d", "e"] })).toThrow(/between 1 and 4 values/);
		expect(() => compile({ value: [{ "anything-but": "a" }, "b"] })).toThrow(
			/cannot combine anything-but/,
		);
		expect(() => compile({ score: [{ numeric: [">", 10, ">=", 11] }] })).toThrow(
			/more than one lower bound/,
		);
		expect(() => compile({ score: [{ numeric: [">", 10, "<", 10] }] })).toThrow(
			/empty numeric range/,
		);
		expect(() => compile({ undeclared: [true] })).toThrow(
			'Filter for event "catalog.changed" contains undeclared field "undeclared"',
		);
	});

	test("requires runtime definitions", () => {
		expect(() => compileEventTriggers([{ event: "catalog.changed" }], [])).toThrow(
			'Event "catalog.changed" is not defined in the conductor event catalog',
		);
		expect(
			() => new Task({ name: "catalog-task" }, { event: "catalog.changed" }, async () => {}),
		).toThrow('Event "catalog.changed" is not defined in the conductor event catalog');
		expect(
			() =>
				new Task(
					{ name: "catalog-task" },
					{ event: "catalog.changed" },
					async () => {},
					definitions,
				),
		).not.toThrow();
	});
});
