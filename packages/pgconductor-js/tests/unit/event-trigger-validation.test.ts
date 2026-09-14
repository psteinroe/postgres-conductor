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

		expect(compiled).toEqual({
			event_key: "catalog.changed",
			payload_fields: null,
			filter: {
				a: [null, true, 1, "1"],
				z: [0, 1],
			},
		});
	});

	test("enforces index-safe UTF-8 byte limits before registration", () => {
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
