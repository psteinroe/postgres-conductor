import type { Sql } from "postgres";
import { parseDuration, type DurationInput } from "./lib/duration.js";

export type TestClock = {
	/** Current time: the faked time, or the system time */
	now(): Date;
	/** Fake database time */
	set(date: Date): Promise<void>;
	/** Move time forward, starting from the system time if it is not faked */
	advance(duration: DurationInput): Promise<void>;
	/** Return to real time */
	reset(): Promise<void>;
};

/**
 * Control time for everything that uses `sql`; pass the clock to `Conductor.create({ clock })` so workers follow it.
 * Fake time is a session setting, so `sql` must have a single connection (`max: 1`).
 */
export function createTestClock(sql: Sql): TestClock {
	let fakeNow: Date | null = null;

	const setFakeNow = async (date: Date | null) => {
		const copy = date && new Date(date);
		await sql`select set_config('pgconductor.fake_now', ${copy?.toISOString() || ""}, false)`;
		fakeNow = copy;
	};

	return {
		now: () => new Date(fakeNow || Date.now()),
		set: (date) => setFakeNow(date),
		advance: (duration) =>
			setFakeNow(new Date((fakeNow || new Date()).getTime() + parseDuration(duration))),
		reset: () => setFakeNow(null),
	};
}
