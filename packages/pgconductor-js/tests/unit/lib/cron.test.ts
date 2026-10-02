import { afterEach, expect, test } from "bun:test";
import { nextCronOccurrence } from "../../../src/lib/cron";

const originalTz = process.env.TZ;

afterEach(() => {
	if (originalTz === undefined) {
		delete process.env.TZ;
	} else {
		process.env.TZ = originalTz;
	}
});

test.each(["UTC", "America/New_York", "Asia/Tokyo"])(
	"nextCronOccurrence evaluates in UTC when TZ=%s",
	(tz) => {
		process.env.TZ = tz;

		expect(nextCronOccurrence("0 0 9 * * *", new Date("2026-10-02T12:00:00Z"))).toEqual(
			new Date("2026-10-03T09:00:00Z"),
		);
	},
);
