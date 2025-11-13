import { test, expect, describe } from "bun:test";
import { AsyncQueue } from "../../../src/lib/async-queue";

describe("AsyncQueue", () => {
	test("pushes and iterates items", async () => {
		const queue = new AsyncQueue<number>(10);
		const items = [1, 2, 3, 4, 5];

		for (const item of items) {
			await queue.push(item);
		}
		queue.close();

		const results: number[] = [];
		for await (const item of queue) {
			results.push(item);
		}

		expect(results).toEqual(items);
	});

	test("respects capacity limit", async () => {
		const queue = new AsyncQueue<number>(2);

		await queue.push(1);
		await queue.push(2);

		// Queue is now full, push should wait
		let thirdPushResolved = false;
		const pushPromise = queue.push(3).then(() => {
			thirdPushResolved = true;
		});

		// Should not resolve immediately
		await new Promise((r) => setTimeout(r, 20));
		expect(thirdPushResolved).toBe(false);

		// Consume one item to make space
		const item = await queue.next();
		expect(item.value).toBe(1);

		// Now the third push should complete
		await pushPromise;
		expect(thirdPushResolved).toBe(true);

		queue.close();
	});

	test("handles async iteration with for-await", async () => {
		const queue = new AsyncQueue<string>(5);

		// Producer
		(async () => {
			for (const word of ["hello", "world", "test"]) {
				await queue.push(word);
			}
			queue.close();
		})();

		// Consumer
		const results: string[] = [];
		for await (const word of queue) {
			results.push(word);
		}

		expect(results).toEqual(["hello", "world", "test"]);
	});

	test("close stops accepting new items", async () => {
		const queue = new AsyncQueue<number>(10);

		await queue.push(1);
		queue.close();
		await queue.push(2); // Should be ignored

		const results: number[] = [];
		for await (const item of queue) {
			results.push(item);
		}

		expect(results).toEqual([1]);
	});

	test("returns done when closed and empty", async () => {
		const queue = new AsyncQueue<number>(10);

		await queue.push(1);
		await queue.push(2);
		queue.close();

		const first = await queue.next();
		expect(first.done).toBe(false);
		expect(first.value).toBe(1);

		const second = await queue.next();
		expect(second.done).toBe(false);
		expect(second.value).toBe(2);

		const third = await queue.next();
		expect(third.done).toBe(true);
	});

	test("handles concurrent producers and consumers", async () => {
		const queue = new AsyncQueue<number>(5);
		const produced: number[] = [];
		const consumed: number[] = [];

		// Producer
		const producer = (async () => {
			for (let i = 0; i < 10; i++) {
				await queue.push(i);
				produced.push(i);
				await new Promise((r) => setTimeout(r, 1));
			}
			queue.close();
		})();

		// Consumer
		const consumer = (async () => {
			for await (const item of queue) {
				consumed.push(item);
				await new Promise((r) => setTimeout(r, 2));
			}
		})();

		await Promise.all([producer, consumer]);

		expect(consumed).toEqual(produced);
		expect(consumed.length).toBe(10);
	});

	test("waits for consumers when full", async () => {
		const queue = new AsyncQueue<number>(3);

		// Fill the queue
		await queue.push(1);
		await queue.push(2);
		await queue.push(3);

		// Next push should wait
		let pushComplete = false;
		const pushPromise = queue.push(4).then(() => {
			pushComplete = false;
		});

		// Verify it's waiting
		await new Promise((r) => setTimeout(r, 20));
		expect(pushComplete).toBe(false);

		// Start consuming
		const consumer = (async () => {
			for await (const _ of queue) {
				// Just drain
			}
		})();

		queue.close();
		await consumer;
	});

	test("handles empty queue with pending next", async () => {
		const queue = new AsyncQueue<number>(10);

		// Call next before any items are pushed
		const nextPromise = queue.next();

		// Push an item
		setTimeout(() => queue.push(42), 50);

		const result = await nextPromise;
		expect(result.value).toBe(42);
		expect(result.done).toBe(false);

		queue.close();
	});

	test("resolves all pending next calls on close", async () => {
		const queue = new AsyncQueue<number>(10);

		const next1 = queue.next();
		const next2 = queue.next();
		const next3 = queue.next();

		queue.close();

		const [r1, r2, r3] = await Promise.all([next1, next2, next3]);

		expect(r1.done).toBe(true);
		expect(r2.done).toBe(true);
		expect(r3.done).toBe(true);
	});
});
