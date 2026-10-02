import { expect, setSystemTime, test } from "bun:test";
import { uuidv7 } from "../../../src/lib/uuidv7";

test("uuidv7 encodes the current time and the version 7 and RFC 9562 variant bits", () => {
	setSystemTime(new Date("2026-10-02T12:00:00.123Z"));
	try {
		const id = uuidv7();
		expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
		expect(parseInt(id.replace("-", "").slice(0, 12), 16)).toBe(
			Date.parse("2026-10-02T12:00:00.123Z"),
		);
		expect(uuidv7()).not.toBe(id);
	} finally {
		setSystemTime();
	}
});
