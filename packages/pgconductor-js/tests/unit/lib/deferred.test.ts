import { test, expect, describe } from "bun:test";
import { Deferred } from "../../../src/lib/deferred";

describe("Deferred", () => {
	test("creates a promise with external resolve", async () => {
		const deferred = new Deferred<number>();
		let resolved = false;

		deferred.promise.then((value) => {
			expect(value).toBe(42);
			resolved = true;
		});

		// Promise should not be resolved yet
		await new Promise((r) => setTimeout(r, 10));
		expect(resolved).toBe(false);

		// Resolve externally
		deferred.resolve(42);

		// Wait for promise to settle
		await deferred.promise;
		expect(resolved).toBe(true);
	});

	test("creates a promise with external reject", async () => {
		const deferred = new Deferred<number>();
		const error = new Error("test error");

		let rejected = false;
		deferred.promise.catch((err) => {
			expect(err).toBe(error);
			rejected = true;
		});

		// Promise should not be rejected yet
		await new Promise((r) => setTimeout(r, 10));
		expect(rejected).toBe(false);

		// Reject externally
		deferred.reject(error);

		// Wait for promise to settle
		await deferred.promise.catch(() => {});
		expect(rejected).toBe(true);
	});

	test("can be awaited directly", async () => {
		const deferred = new Deferred<string>();

		setTimeout(() => deferred.resolve("hello"), 10);

		const result = await deferred.promise;
		expect(result).toBe("hello");
	});

	test("works with void type", async () => {
		const deferred = new Deferred<void>();

		setTimeout(() => deferred.resolve(), 10);

		await deferred.promise;
		// Should complete without error
	});

	test("can resolve with a promise", async () => {
		const deferred = new Deferred<number>();
		const innerPromise = Promise.resolve(123);

		deferred.resolve(innerPromise);

		const result = await deferred.promise;
		expect(result).toBe(123);
	});

	test("allows multiple then handlers", async () => {
		const deferred = new Deferred<number>();
		const results: number[] = [];

		deferred.promise.then((v) => results.push(v));
		deferred.promise.then((v) => results.push(v * 2));
		deferred.promise.then((v) => results.push(v * 3));

		deferred.resolve(10);

		await deferred.promise;
		await new Promise((r) => setTimeout(r, 10)); // Let all handlers run

		expect(results).toEqual([10, 20, 30]);
	});

	test("resolving multiple times uses first value", async () => {
		const deferred = new Deferred<number>();

		deferred.resolve(1);
		deferred.resolve(2);
		deferred.resolve(3);

		const result = await deferred.promise;
		expect(result).toBe(1);
	});
});
