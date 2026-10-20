import type { Sql } from "postgres";
import { refreshClocks } from "./lib/clock.js";
import { parseDuration, type DurationInput } from "./lib/duration.js";

export type TestClock = {
	/** Fake database time */
	set(date: Date): Promise<void>;
	/** Move database time forward, starting from real time if it is not faked */
	advance(duration: DurationInput): Promise<void>;
	/** Return to real time */
	reset(): Promise<void>;
};

/**
 * Control database time for everything that uses `sql`, including running workers of conductors created with it.
 * Fake time is a session setting, so `sql` must have a single connection (`max: 1`).
 */
export function createTestClock(sql: Sql): TestClock {
	const setFakeTime = async (value: string) => {
		await sql`select set_config('pgconductor.fake_now', ${value}, false)`;
		await refreshClocks(sql);
	};

	return {
		set: (date) => setFakeTime(date.toISOString()),
		advance: async (duration) => {
			await sql`
				select set_config(
					'pgconductor.fake_now',
					(pgconductor._private_current_time() + interval '1 millisecond' * ${parseDuration(duration)}::float8)::text,
					false
				)
			`;
			await refreshClocks(sql);
		},
		reset: () => setFakeTime(""),
	};
}
