import { beforeEach, describe, expect, mock, test } from "bun:test";
import { Clock } from "../../src/lib/clock";
import { nextCronOccurrence } from "../../src/lib/cron";

const logger = {
	info: mock(),
	warn: mock(),
	error: mock(),
	debug: mock(),
};

const date = (ms: number): Date => new Date(ms);

describe("Clock", () => {
	beforeEach(() => {
		logger.warn.mockClear();
	});

	test("corrects positive and negative local clock skew", async () => {
		let now = 1_000;
		const positive = new Clock({
			sampleDatabaseTime: async () => date(2_000),
			logger,
			localClock: () => date(now),
		});

		await positive.refresh();
		now = 1_100;
		expect(positive.now()).toEqual(date(2_100));

		now = 1_000;
		const negative = new Clock({
			sampleDatabaseTime: async () => date(0),
			logger,
			localClock: () => date(now),
		});

		await negative.refresh();
		now = 1_100;
		expect(negative.now()).toEqual(date(100));
	});

	test("uses the request midpoint to account for latency", async () => {
		const localTimes = [date(1_000), date(1_300)];
		const clock = new Clock({
			sampleDatabaseTime: async () => date(1_100),
			logger,
			localClock: () => {
				const value = localTimes.shift();
				if (!value) throw new Error("local clock exhausted");
				return value;
			},
		});

		await clock.refresh();
		expect(clock.offset).toBe(-50);
	});

	test("gives skewed workers the same cron slot", async () => {
		const databaseNow = date(Date.UTC(2025, 0, 1, 12, 0, 0));
		const positive = new Clock({
			sampleDatabaseTime: async () => databaseNow,
			logger,
			localClock: () => date(databaseNow.getTime() - 5 * 60 * 1000),
		});
		const negative = new Clock({
			sampleDatabaseTime: async () => databaseNow,
			logger,
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
				return date(2_000);
			},
			logger,
			localClock: () => date(1_000),
		});

		await clock.refresh();
		fail = true;
		await clock.refresh();

		expect(clock.offset).toBe(1_000);
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
				return date(1_000 + samples);
			},
			logger,
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
});
