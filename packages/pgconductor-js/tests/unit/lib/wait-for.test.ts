import { test, expect, describe } from "bun:test";
import { waitFor } from "../../../src/lib/wait-for";

describe("waitFor", () => {
	test("waits for specified duration", async () => {
		const start = Date.now();
		await waitFor(100);
		const elapsed = Date.now() - start;

		expect(elapsed).toBeGreaterThanOrEqual(95); // Allow small margin
		expect(elapsed).toBeLessThan(150);
	});

	test("applies jitter to delay", async () => {
		const delays: number[] = [];

		// Run multiple times to test jitter variance
		for (let i = 0; i < 5; i++) {
			const start = Date.now();
			await waitFor(50, { jitter: 50 });
			delays.push(Date.now() - start);
		}

		// All delays should be >= 50ms (base)
		expect(delays.every((d) => d >= 45)).toBe(true);

		// At least one delay should be > base (jitter applied)
		// Note: This is probabilistic but very likely
		expect(delays.some((d) => d > 60)).toBe(true);
	});

	test("resolves immediately when signal is already aborted", async () => {
		const controller = new AbortController();
		controller.abort();

		const start = Date.now();
		await waitFor(100, { signal: controller.signal });
		const elapsed = Date.now() - start;

		// js is slow
		expect(elapsed).toBeLessThan(5);
	});

	test("resolves when signal is aborted during wait", async () => {
		const controller = new AbortController();

		const start = Date.now();

		// Abort after 50ms
		setTimeout(() => controller.abort(), 50);

		await waitFor(1000, { signal: controller.signal });
		const elapsed = Date.now() - start;

		expect(elapsed).toBeGreaterThanOrEqual(45);
		expect(elapsed).toBeLessThan(200); // Should abort before full 1000ms
	});

	test("works without options", async () => {
		const start = Date.now();
		await waitFor(50);
		const elapsed = Date.now() - start;

		expect(elapsed).toBeGreaterThanOrEqual(45);
	});

	test("works with signal but no jitter", async () => {
		const controller = new AbortController();
		const start = Date.now();
		await waitFor(50, { signal: controller.signal });
		const elapsed = Date.now() - start;

		expect(elapsed).toBeGreaterThanOrEqual(45);
	});

	test("works with jitter but no signal", async () => {
		const start = Date.now();
		await waitFor(50, { jitter: 25 });
		const elapsed = Date.now() - start;

		expect(elapsed).toBeGreaterThanOrEqual(45);
		expect(elapsed).toBeLessThan(100);
	});

	test("zero delay resolves immediately", async () => {
		const start = Date.now();
		await waitFor(0);
		const elapsed = Date.now() - start;

		expect(elapsed).toBeLessThan(50);
	});
});
