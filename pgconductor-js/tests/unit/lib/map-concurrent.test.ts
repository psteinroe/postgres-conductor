import { test, expect, describe } from "bun:test";
import { mapConcurrent } from "../../../src/lib/map-concurrent";
import type { PollableAsyncIterable } from "../../../src/lib/async-queue";

class PollableGenerator<T> implements PollableAsyncIterable<T> {
	private buffer: T[] = [];
	private done = false;
	private iterator: AsyncIterator<T>;

	constructor(source: AsyncIterable<T>) {
		this.iterator = source[Symbol.asyncIterator]();
	}

	tryNext(): T | undefined {
		return this.buffer.shift();
	}

	async next(): Promise<IteratorResult<T>> {
		if (this.buffer.length > 0) {
			return { value: this.buffer.shift()!, done: false };
		}
		if (this.done) {
			return { value: undefined as any, done: true };
		}
		const result = await this.iterator.next();
		if (result.done) {
			this.done = true;
		}
		return result;
	}

	[Symbol.asyncIterator]() {
		return this;
	}
}

async function* generateNumbers(count: number): AsyncGenerator<number> {
	for (let i = 0; i < count; i++) {
		yield i;
	}
}

function pollable<T>(source: AsyncIterable<T>): PollableAsyncIterable<T> {
	return new PollableGenerator(source);
}

describe("mapConcurrent", () => {
	test("maps items with concurrency limit", async () => {
		const source = pollable(generateNumbers(5));
		const results: number[] = [];

		for await (const result of mapConcurrent(source, 2, async (n) => n * 2)) {
			results.push(result);
		}

		results.sort((a, b) => a - b); // Order may vary due to concurrency
		expect(results).toEqual([0, 2, 4, 6, 8]);
	});

	test("respects concurrency limit", async () => {
		const concurrent: number[] = [];
		let maxConcurrent = 0;
		let currentConcurrent = 0;

		const source = pollable(generateNumbers(10));

		for await (const _ of mapConcurrent(source, 3, async (n) => {
			currentConcurrent++;
			maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
			concurrent.push(currentConcurrent);

			await new Promise((r) => setTimeout(r, 50));

			currentConcurrent--;
			return n;
		})) {
			// Just consume
		}

		expect(maxConcurrent).toBeGreaterThan(0);
		expect(maxConcurrent).toBeLessThanOrEqual(3);
	});

	test("handles async mapper function", async () => {
		const source = pollable(generateNumbers(3));
		const results: string[] = [];

		for await (const result of mapConcurrent(source, 2, async (n) => {
			await new Promise((r) => setTimeout(r, 10));
			return `item-${n}`;
		})) {
			results.push(result);
		}

		results.sort();
		expect(results).toEqual(["item-0", "item-1", "item-2"]);
	});

	test("handles empty source", async () => {
		async function* empty() {
			// Yields nothing
		}

		const results: number[] = [];
		for await (const result of mapConcurrent(pollable(empty()), 2, async (n) => n)) {
			results.push(result);
		}

		expect(results).toEqual([]);
	});

	test("propagates errors from mapper", async () => {
		const source = pollable(generateNumbers(5));

		try {
			for await (const _ of mapConcurrent(source, 2, async (n) => {
				if (n === 2) throw new Error("test error");
				return n;
			})) {
				// Should throw before completing
			}
			expect.unreachable();
		} catch (err) {
			expect(err).toBeInstanceOf(Error);
			expect((err as Error).message).toBe("test error");
		}
	});

	test("handles single concurrency", async () => {
		const order: number[] = [];
		const source = pollable(generateNumbers(5));

		for await (const _result of mapConcurrent(source, 1, async (n) => {
			order.push(n);
			await new Promise((r) => setTimeout(r, 5));
			return n;
		})) {
			// Just track order
		}

		expect(order).toEqual([0, 1, 2, 3, 4]);
	});

	test("handles high concurrency", async () => {
		const source = pollable(generateNumbers(100));
		const results: number[] = [];

		for await (const result of mapConcurrent(source, 50, async (n) => n)) {
			results.push(result);
		}

		results.sort((a, b) => a - b);
		expect(results.length).toBe(100);
		expect(results[0]).toBe(0);
		expect(results[99]).toBe(99);
	});

	test("yields results as they complete", async () => {
		const source = pollable(generateNumbers(5));
		const completionOrder: number[] = [];
		const yieldOrder: number[] = [];

		for await (const result of mapConcurrent(source, 3, async (n) => {
			// Reverse delay so higher numbers complete first
			await new Promise((r) => setTimeout(r, (5 - n) * 10));
			completionOrder.push(n);
			return n;
		})) {
			yieldOrder.push(result);
		}

		// Results should be yielded in completion order, not input order
		expect(completionOrder).toEqual(yieldOrder);
	});

	test("works with array source", async () => {
		async function* fromArray<T>(arr: T[]): AsyncGenerator<T> {
			for (const item of arr) {
				yield item;
			}
		}

		const source = pollable(fromArray([10, 20, 30]));
		const results: number[] = [];

		for await (const result of mapConcurrent(source, 2, async (n) => n / 10)) {
			results.push(result);
		}

		results.sort();
		expect(results).toEqual([1, 2, 3]);
	});

	test("completes all pending operations", async () => {
		const source = pollable(generateNumbers(10));
		const started: number[] = [];
		const completed: number[] = [];

		for await (const _ of mapConcurrent(source, 3, async (n) => {
			started.push(n);
			await new Promise((r) => setTimeout(r, 10));
			completed.push(n);
			return n;
		})) {
			// Just consume
		}

		expect(started.length).toBe(10);
		expect(completed.length).toBe(10);
	});
});
