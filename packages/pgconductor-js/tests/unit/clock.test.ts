import { beforeEach, describe, expect, test, vi } from "vitest";
import { Clock, refreshClocks } from "../../src/lib/clock";
import { nextCronOccurrence } from "../../src/lib/cron";

const logger = {
	info: vi.fn(),
	warn: vi.fn(),
	error: vi.fn(),
	debug: vi.fn(),
};

const date = (ms: number): Date => new Date(ms);
const sample = (ms: number) => ({ now: date(ms), frozen: false });

describe("Clock", () => {
	beforeEach(() => {
		logger.warn.mockClear();
	});

	test("corrects positive and negative local clock skew", async () => {
		let now = 1_000;
		const positive = new Clock({
			sampleDatabaseTime: async () => sample(2_000),
			logger,
			source: {},
			localClock: () => date(now),
		});

		await positive.refresh();
		now = 1_100;
		expect(positive.now()).toEqual(date(2_100));

		now = 1_000;
		const negative = new Clock({
			sampleDatabaseTime: async () => sample(0),
			logger,
			source: {},
			localClock: () => date(now),
		});

		await negative.refresh();
		now = 1_100;
		expect(negative.now()).toEqual(date(100));
	});

	test("uses the request midpoint to account for latency", async () => {
		const localTimes = [date(1_000), date(1_300), date(2_000)];
		const clock = new Clock({
			sampleDatabaseTime: async () => sample(1_100),
			logger,
			source: {},
			localClock: () => {
				const value = localTimes.shift();
				if (!value) throw new Error("local clock exhausted");
				return value;
			},
		});

		await clock.refresh();
		expect(clock.now()).toEqual(date(1_950));
	});

	test("gives skewed workers the same cron slot", async () => {
		const databaseNow = date(Date.UTC(2025, 0, 1, 12, 0, 0));
		const positive = new Clock({
			sampleDatabaseTime: async () => ({ now: databaseNow, frozen: false }),
			logger,
			source: {},
			localClock: () => date(databaseNow.getTime() - 5 * 60 * 1000),
		});
		const negative = new Clock({
			sampleDatabaseTime: async () => ({ now: databaseNow, frozen: false }),
			logger,
			source: {},
			localClock: () => date(databaseNow.getTime() + 5 * 60 * 1000),
		});

		await positive.refresh();
		await negative.refresh();

		expect(nextCronOccurrence("0 */5 * * * *", positive.now())).toEqual(
			nextCronOccurrence("0 */5 * * * *", negative.now()),
		);
	});

	test("retains the local clock when the initial sample fails", async () => {
		const clock = new Clock({
			sampleDatabaseTime: async () => {
				throw new Error("database unavailable");
			},
			logger,
			source: {},
			localClock: () => date(1_000),
		});

		await clock.start();
		clock.stop();

		expect(clock.now()).toEqual(date(1_000));
		expect(logger.warn).toHaveBeenCalledWith(
			"Database clock offset refresh failed; retaining previous offset",
			expect.any(Error),
		);
	});

	test("retains the previous offset when a refresh fails", async () => {
		let fail = false;
		const clock = new Clock({
			sampleDatabaseTime: async () => {
				if (fail) throw new Error("database unavailable");
				return sample(2_000);
			},
			logger,
			source: {},
			localClock: () => date(1_000),
		});

		await clock.refresh();
		fail = true;
		await clock.refresh();

		expect(clock.now()).toEqual(date(2_000));
		expect(logger.warn).toHaveBeenCalledWith(
			"Database clock offset refresh failed; retaining previous offset",
			expect.any(Error),
		);
	});

	test("refreshes periodically without overlapping samples and stops its timer", async () => {
		let samples = 0;
		let inFlight = 0;
		let maxInFlight = 0;
		const clock = new Clock({
			sampleDatabaseTime: async () => {
				samples++;
				inFlight++;
				maxInFlight = Math.max(maxInFlight, inFlight);
				await new Promise((resolve) => setTimeout(resolve, 10));
				inFlight--;
				return sample(1_000 + samples);
			},
			logger,
			source: {},
			localClock: () => date(0),
			refreshIntervalMs: 1,
		});

		await clock.start();
		await new Promise((resolve) => setTimeout(resolve, 25));
		clock.stop();
		const samplesAfterStop = samples;
		await new Promise((resolve) => setTimeout(resolve, 25));

		expect(samples).toBeGreaterThan(1);
		expect(maxInFlight).toBe(1);
		expect(samples).toBe(samplesAfterStop);
	});

	test("stands still while the database uses fake time", async () => {
		let now = 1_000;
		const clock = new Clock({
			sampleDatabaseTime: async () => ({ now: date(5_000), frozen: true }),
			logger,
			source: {},
			localClock: () => date(now),
		});

		await clock.refresh();
		now = 9_000;

		expect(clock.now()).toEqual(date(5_000));
	});

	test("refreshClocks re-samples the running clocks of a source", async () => {
		const source = {};
		let databaseNow = 1_000;
		const sampleDatabaseTime = async () => ({ now: date(databaseNow), frozen: true });
		const running = new Clock({ sampleDatabaseTime, logger, source });
		const stopped = new Clock({ sampleDatabaseTime, logger, source });
		const other = new Clock({ sampleDatabaseTime, logger, source: {} });

		await running.start();
		await stopped.start();
		stopped.stop();
		await other.start();
		databaseNow = 2_000;
		await refreshClocks(source);
		running.stop();
		other.stop();

		expect(running.now()).toEqual(date(2_000));
		expect(stopped.now()).toEqual(date(1_000));
		expect(other.now()).toEqual(date(1_000));
	});
});
