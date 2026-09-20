import { describe, expect, test } from "bun:test";
import { compileEventTrigger } from "../../src/event-trigger-validation";

describe("event trigger compilation", () => {
	test("canonicalizes field order and deduplicates scalar alternatives type-sensitively", () => {
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
		const { anchors, ...subscription } = compiled;
		expect(subscription).toEqual({
			event_key: "catalog.changed",
			payload_fields: null,
			filter: {
				a: [null, true, 1, "1"],
				z: [0, 1],
			},
		});
		expect(
			anchors.map(({ field_name, operator, scalar_type }) => ({
				field_name,
				operator,
				scalar_type,
			})),
		).toEqual([
			{ field_name: "a", operator: "exact", scalar_type: "null" },
			{ field_name: "a", operator: "exact", scalar_type: "boolean" },
			{ field_name: "a", operator: "exact", scalar_type: "number" },
			{ field_name: "a", operator: "exact", scalar_type: "string" },
		]);
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

		expect(compiled?.filter).toEqual(JSON.parse('{"__proto__":["safe"]}'));
		expect(compiled?.anchors).toEqual([
			{
				field_name: "__proto__",
				operator: "exact",
				scalar_type: "string",
				text_value: "safe",
			},
		]);
	});

	test("canonicalizes supported atomic operators", () => {
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

		expect(compiled?.filter).toEqual({
			deleted: [{ $operator: "exists", value: false }],
			name: ["exact", { $operator: "prefix", value: "literal%_\\" }],
			score: [
				{
					$operator: "numeric_range",
					lower: 10,
					lowerInclusive: true,
					upper: 20,
					upperInclusive: false,
				},
			],
			status: [{ $operator: "anything_but", value: "blocked" }],
		});
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
