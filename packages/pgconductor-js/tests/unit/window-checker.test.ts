import { test, expect, describe } from "bun:test";
import { WindowChecker } from "../../src/lib/window-checker";

describe("WindowChecker", () => {
	test("isWithinWindow returns true when inside window", () => {
		const checker = new WindowChecker(["09:00", "17:00"]);

		// 10:00 UTC should be inside window
		const insideTime = new Date("2024-01-01T10:00:00Z");
		expect(checker.isWithinWindow(insideTime)).toBe(true);

		// 09:00 UTC should be inside window (start time)
		const startTime = new Date("2024-01-01T09:00:00Z");
		expect(checker.isWithinWindow(startTime)).toBe(true);
	});

	test("isWithinWindow returns false when outside window", () => {
		const checker = new WindowChecker(["09:00", "17:00"]);

		// 18:00 UTC should be outside window
		const outsideTime = new Date("2024-01-01T18:00:00Z");
		expect(checker.isWithinWindow(outsideTime)).toBe(false);

		// 17:00 UTC should be outside window (end time is exclusive)
		const endTime = new Date("2024-01-01T17:00:00Z");
		expect(checker.isWithinWindow(endTime)).toBe(false);

		// 08:00 UTC should be outside window
		const beforeStart = new Date("2024-01-01T08:00:00Z");
		expect(checker.isWithinWindow(beforeStart)).toBe(false);
	});

	test("getNextValidRunAt returns tomorrow when past window end", () => {
		const checker = new WindowChecker(["09:00", "17:00"]);

		// At 18:00, next valid run should be tomorrow at 09:00
		const now = new Date("2024-01-01T18:00:00Z");
		const nextRun = checker.getNextValidRunAt(now);

		expect(nextRun.toISOString()).toBe("2024-01-02T09:00:00.000Z");
	});

	test("getNextValidRunAt returns today when before window start", () => {
		const checker = new WindowChecker(["09:00", "17:00"]);

		// At 08:00, next valid run should be today at 09:00
		const now = new Date("2024-01-01T08:00:00Z");
		const nextRun = checker.getNextValidRunAt(now);

		expect(nextRun.toISOString()).toBe("2024-01-01T09:00:00.000Z");
	});
});
