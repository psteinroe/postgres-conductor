import type { PendingQuery, Row, Sql } from "postgres";
import { parseDuration, type DurationInput } from "./lib/duration.js";

export type TestClock = {
	/** Current time: the faked time, or the system time */
	now(): Date;
	/** Fake database time, which keeps running from `date` */
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
	let offsetMs = 0;

	// the database clock keeps the offset, so database and worker time run on together
	const setOffset = async (offset: PendingQuery<Row[]>) => {
		const [row] = await sql<{ offset_ms: number }[]>`
			with o as materialized (select ${offset} as value)
			select
				set_config('pgconductor.fake_offset', coalesce(o.value::text, ''), false),
				coalesce(extract(epoch from o.value) * 1000, 0)::float8 as offset_ms
			from o
		`;
		offsetMs = row?.offset_ms || 0;
	};

	return {
		now: () => new Date(Date.now() + offsetMs),
		set: (date) => setOffset(sql`${date.toISOString()}::timestamptz - clock_timestamp()`),
		advance: (duration) =>
			setOffset(sql`
				coalesce(nullif(current_setting('pgconductor.fake_offset', true), '')::interval, interval '0')
					+ ${parseDuration(duration)}::float8 * interval '1 millisecond'
			`),
		reset: () => setOffset(sql`null::interval`),
	};
}
