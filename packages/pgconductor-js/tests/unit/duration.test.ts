import { describe, expect, test } from "bun:test";
import { parseDuration } from "../../src/index";

describe("duration API", () => {
	test("parses numeric and unit durations", () => {
		expect(parseDuration(12.9)).toBe(12);
		expect(parseDuration("1.5s")).toBe(1500);
		expect(parseDuration("2m")).toBe(120000);
	});

	test("rejects invalid durations", () => {
		for (const value of [-1, Number.NaN, Number.POSITIVE_INFINITY, "", "1", "-1s", "1x"] as const) {
			expect(() => parseDuration(value as never)).toThrow();
		}
	});
});
