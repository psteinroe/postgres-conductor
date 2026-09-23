import { describe, expect, test } from "bun:test";
import { compileEventTrigger } from "../../src/event-trigger-validation";

describe("event trigger compilation", () => {
	test("sorts fields and deduplicates scalar alternatives type-sensitively", () => {
		const compiled = compileEventTrigger(
			{
				event: "catalog.changed",
				filter: {
					z: [1, 1, 0],
					a: ["1", 1, true, null],
				},
			},
			[],
			true,
		);

		if (!compiled) throw new Error("expected an event trigger");
		expect(compiled).toEqual({
			event_key: "catalog.changed",
			payload_fields: null,
			required_field_count: 2,
			terms: [
				{ field_name: "a", operator: "exact", scalar_type: "null" },
				{
					field_name: "a",
					operator: "exact",
					scalar_type: "boolean",
					boolean_value: true,
				},
				{
					field_name: "a",
					operator: "exact",
					scalar_type: "number",
					number_value: 1,
				},
				{
					field_name: "a",
					operator: "exact",
					scalar_type: "string",
					text_value: "1",
				},
				{
					field_name: "z",
					operator: "exact",
					scalar_type: "number",
					number_value: 0,
				},
				{
					field_name: "z",
					operator: "exact",
					scalar_type: "number",
					number_value: 1,
				},
			],
		});
	});

	test("preserves prototype-named filter fields", () => {
		const compiled = compileEventTrigger(
			{
				event: "catalog.changed",
				filter: JSON.parse('{"__proto__":["safe"]}') as Record<string, unknown>,
			},
			[],
			true,
		);

		expect(compiled?.terms).toEqual([
			{
				field_name: "__proto__",
				operator: "exact",
				scalar_type: "string",
				text_value: "safe",
			},
		]);
	});

	test("compiles supported atomic operators", () => {
		const compiled = compileEventTrigger(
			{
				event: "catalog.changed",
				filter: {
					status: [{ "anything-but": "blocked" }],
					score: [{ numeric: ["<", 20, ">=", 10] }],
					name: [{ prefix: "literal%_\\" }, "exact"],
					deleted: [{ exists: false }],
				},
			},
			[],
			true,
		);

		expect(compiled?.terms).toEqual([
			{ field_name: "deleted", operator: "exists", boolean_value: false },
			{
				field_name: "name",
				operator: "exact",
				scalar_type: "string",
				text_value: "exact",
			},
			{
				field_name: "name",
				operator: "prefix",
				scalar_type: "string",
				text_value: "literal%_\\",
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
				field_name: "status",
				operator: "anything_but",
				scalar_type: "string",
				text_value: "blocked",
			},
		]);
	});

	test("rejects malformed, unbounded, and non-atomic operators", () => {
		const compile = (value: unknown[]) =>
			compileEventTrigger({ event: "catalog.changed", filter: { value } }, [], true);

		expect(() => compile([{ prefix: "" }])).toThrow(/non-empty string/);
		expect(() => compile([{ prefix: "x".repeat(65) }])).toThrow(/64 characters/);
		expect(() => compile([{ numeric: [">", 10, ">=", 11] }])).toThrow(/duplicate lower bounds/);
		expect(() => compile([{ numeric: [">", 10, "<", 10] }])).toThrow(/is empty/);
		expect(() => compile([{ exists: "yes" }])).toThrow(/must be boolean/);
		expect(() => compile([{ "anything-but": ["a", "b"] }])).toThrow(/requires one scalar value/);
		expect(() => compile([{ "anything-but": "a" }, "b"])).toThrow(/must be atomic/);
		expect(() => compile([{ suffix: "x" }])).toThrow(/unsupported operator/);
	});

	test("enforces index-safe names and UTF-8 byte limits before registration", () => {
		expect(() =>
			compileEventTrigger({ event: "catalog.changed", filter: { " ": [true] } }, [], true),
		).toThrow(/empty field names/);
		expect(() => compileEventTrigger({ event: "e".repeat(256) }, [], true)).toThrow(
			/255 UTF-8 bytes/,
		);
		expect(() =>
			compileEventTrigger(
				{ event: "catalog.changed", filter: { value: ["x".repeat(1023)] } },
				[],
				true,
			),
		).toThrow(/1024 UTF-8 bytes/);
	});
});
