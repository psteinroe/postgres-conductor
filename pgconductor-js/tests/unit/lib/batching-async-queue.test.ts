import { test, expect, describe } from "bun:test";
import { BatchingAsyncQueue } from "../../../src/lib/batching-async-queue";

type TestItem = {
	task_key: string;
	id: string;
};

describe("BatchingAsyncQueue - AsyncQueue compatibility (no batching)", () => {
	// All tests from AsyncQueue, adapted to work with arrays

	test("pushes and iterates items", async () => {
		const queue = new BatchingAsyncQueue<TestItem>(10, new Map());
		const items: TestItem[] = [
			{ task_key: "task-a", id: "1" },
			{ task_key: "task-a", id: "2" },
			{ task_key: "task-a", id: "3" },
			{ task_key: "task-a", id: "4" },
			{ task_key: "task-a", id: "5" },
		];

		for (const item of items) {
			await queue.push(item);
		}
		queue.close();

		const results: TestItem[] = [];
		for await (const group of queue) {
			results.push(...group.items); // Access items from BatchGroup
		}

		expect(results).toEqual(items);
	});

	test("respects capacity limit", async () => {
		const queue = new BatchingAsyncQueue<TestItem>(2, new Map());

		await queue.push({ task_key: "task-a", id: "1" });
		await queue.push({ task_key: "task-a", id: "2" });

		// Queue is now full, push should wait
		let thirdPushResolved = false;
		const pushPromise = queue.push({ task_key: "task-a", id: "3" }).then(() => {
			thirdPushResolved = true;
		});

		// Should not resolve immediately
		await new Promise((r) => setTimeout(r, 20));
		expect(thirdPushResolved).toBe(false);

		// Consume one item to make space
		const item = await queue.next();
		expect(item.value).toEqual({ taskKey: "task-a", items: [{ task_key: "task-a", id: "1" }] });

		// Now the third push should complete
		await pushPromise;
		expect(thirdPushResolved).toBe(true);

		queue.close();
	});

	test("handles async iteration with for-await", async () => {
		const queue = new BatchingAsyncQueue<TestItem>(5, new Map());

		// Producer
		(async () => {
			const items = [
				{ task_key: "task-a", id: "hello" },
				{ task_key: "task-a", id: "world" },
				{ task_key: "task-a", id: "test" },
			];
			for (const item of items) {
				await queue.push(item);
			}
			queue.close();
		})();

		// Consumer
		const results: TestItem[] = [];
		for await (const group of queue) {
			results.push(...group.items);
		}

		expect(results).toEqual([
			{ task_key: "task-a", id: "hello" },
			{ task_key: "task-a", id: "world" },
			{ task_key: "task-a", id: "test" },
		]);
	});

	test("close stops accepting new items", async () => {
		const queue = new BatchingAsyncQueue<TestItem>(10, new Map());

		await queue.push({ task_key: "task-a", id: "1" });
		queue.close();
		await queue.push({ task_key: "task-a", id: "2" }); // Should be ignored

		const results: TestItem[] = [];
		for await (const group of queue) {
			results.push(...group.items);
		}

		expect(results).toEqual([{ task_key: "task-a", id: "1" }]);
	});

	test("returns done when closed and empty", async () => {
		const queue = new BatchingAsyncQueue<TestItem>(10, new Map());

		await queue.push({ task_key: "task-a", id: "1" });
		await queue.push({ task_key: "task-a", id: "2" });
		queue.close();

		const first = await queue.next();
		expect(first.done).toBe(false);
		expect(first.value).toEqual({ taskKey: "task-a", items: [{ task_key: "task-a", id: "1" }] });

		const second = await queue.next();
		expect(second.done).toBe(false);
		expect(second.value).toEqual({ taskKey: "task-a", items: [{ task_key: "task-a", id: "2" }] });

		const third = await queue.next();
		expect(third.done).toBe(true);
	});

	test("handles concurrent producers and consumers", async () => {
		const queue = new BatchingAsyncQueue<TestItem>(5, new Map());
		const produced: TestItem[] = [];
		const consumed: TestItem[] = [];

		// Producer
		const producer = (async () => {
			for (let i = 0; i < 10; i++) {
				const item = { task_key: "task-a", id: `${i}` };
				await queue.push(item);
				produced.push(item);
				await new Promise((r) => setTimeout(r, 1));
			}
			queue.close();
		})();

		// Consumer
		const consumer = (async () => {
			for await (const group of queue) {
				consumed.push(...group.items);
				await new Promise((r) => setTimeout(r, 2));
			}
		})();

		await Promise.all([producer, consumer]);

		expect(consumed).toEqual(produced);
		expect(consumed.length).toBe(10);
	});

	test("waits for consumers when full", async () => {
		const queue = new BatchingAsyncQueue<TestItem>(3, new Map());

		// Fill the queue
		await queue.push({ task_key: "task-a", id: "1" });
		await queue.push({ task_key: "task-a", id: "2" });
		await queue.push({ task_key: "task-a", id: "3" });

		// Next push should wait
		let pushComplete = false;
		queue.push({ task_key: "task-a", id: "4" }).then(() => {
			pushComplete = true;
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
		const queue = new BatchingAsyncQueue<TestItem>(10, new Map());

		// Call next before any items are pushed
		const nextPromise = queue.next();

		// Push an item
		setTimeout(() => queue.push({ task_key: "task-a", id: "42" }), 50);

		const result = await nextPromise;
		expect(result.value).toEqual({ taskKey: "task-a", items: [{ task_key: "task-a", id: "42" }] });
		expect(result.done).toBe(false);

		queue.close();
	});

	test("resolves all pending next calls on close", async () => {
		const queue = new BatchingAsyncQueue<TestItem>(10, new Map());

		const next1 = queue.next();
		const next2 = queue.next();
		const next3 = queue.next();

		queue.close();

		const [r1, r2, r3] = await Promise.all([next1, next2, next3]);

		expect(r1.done).toBe(true);
		expect(r2.done).toBe(true);
		expect(r3.done).toBe(true);
	});

	test("does not duplicate items when resolver is waiting", async () => {
		const queue = new BatchingAsyncQueue<TestItem>(10, new Map());

		// Start a consumer that's waiting for an item
		const firstNextPromise = queue.next();

		// Push an item - should deliver directly to the waiting resolver, not queue it
		await queue.push({ task_key: "task-a", id: "42" });

		// First next() should receive the item
		const firstResult = await firstNextPromise;
		expect(firstResult.value).toEqual({
			taskKey: "task-a",
			items: [{ task_key: "task-a", id: "42" }],
		});
		expect(firstResult.done).toBe(false);

		// Second next() should wait (not get the same item again)
		let secondNextResolved = false;
		const secondNextPromise = queue.next().then((result) => {
			secondNextResolved = true;
			return result;
		});

		// Give it time to resolve if it's going to
		await new Promise((r) => setTimeout(r, 20));
		expect(secondNextResolved).toBe(false);

		// Push a different item to unblock the second next()
		await queue.push({ task_key: "task-a", id: "99" });

		const secondResult = await secondNextPromise;
		expect(secondResult.value).toEqual({
			taskKey: "task-a",
			items: [{ task_key: "task-a", id: "99" }],
		});
		expect(secondResult.done).toBe(false);

		queue.close();
	});

	test("delivers each item exactly once with multiple waiting consumers", async () => {
		const queue = new BatchingAsyncQueue<TestItem>(10, new Map());

		// Create multiple waiting consumers
		const consumer1 = queue.next();
		const consumer2 = queue.next();
		const consumer3 = queue.next();

		// Push items - each should go to a different consumer
		await queue.push({ task_key: "task-a", id: "1" });
		await queue.push({ task_key: "task-a", id: "2" });
		await queue.push({ task_key: "task-a", id: "3" });

		const [r1, r2, r3] = await Promise.all([consumer1, consumer2, consumer3]);

		// Each consumer should receive exactly one unique item
		const receivedValues = [r1.value, r2.value, r3.value].map((group) => group.items[0].id).sort();
		expect(receivedValues).toEqual(["1", "2", "3"]);

		// Queue should now be empty
		let nextResolved = false;
		const nextPromise = queue.next().then(() => {
			nextResolved = true;
		});

		await new Promise((r) => setTimeout(r, 20));
		expect(nextResolved).toBe(false);

		queue.close();
		await nextPromise;
	});
});

describe("BatchingAsyncQueue - Batching behavior", () => {
	test("batched items are accumulated and emitted when size reached", async () => {
		const batchConfigs = new Map([["batched", { size: 3, timeoutMs: 1000 }]]);
		const queue = new BatchingAsyncQueue<TestItem>(10, batchConfigs);

		const items: TestItem[][] = [];
		const consumer = (async () => {
			for await (const group of queue) {
				items.push(group.items);
			}
		})();

		await queue.push({ task_key: "batched", id: "1" });
		await queue.push({ task_key: "batched", id: "2" });
		await queue.push({ task_key: "batched", id: "3" });

		// Wait for emission
		await new Promise((r) => setTimeout(r, 10));

		queue.close();
		await consumer;

		expect(items).toEqual([
			[
				{ task_key: "batched", id: "1" },
				{ task_key: "batched", id: "2" },
				{ task_key: "batched", id: "3" },
			],
		]);
	});

	test("batched items are emitted on timeout", async () => {
		const batchConfigs = new Map([["batched", { size: 10, timeoutMs: 50 }]]);
		const queue = new BatchingAsyncQueue<TestItem>(10, batchConfigs);

		const items: TestItem[][] = [];
		const consumer = (async () => {
			for await (const group of queue) {
				items.push(group.items);
			}
		})();

		await queue.push({ task_key: "batched", id: "1" });
		await queue.push({ task_key: "batched", id: "2" });

		// Wait for timeout
		await new Promise((r) => setTimeout(r, 100));

		queue.close();
		await consumer;

		expect(items).toEqual([
			[
				{ task_key: "batched", id: "1" },
				{ task_key: "batched", id: "2" },
			],
		]);
	});

	test("different tasks maintain separate batches", async () => {
		const batchConfigs = new Map([
			["batched-1", { size: 2, timeoutMs: 1000 }],
			["batched-2", { size: 2, timeoutMs: 1000 }],
		]);
		const queue = new BatchingAsyncQueue<TestItem>(10, batchConfigs);

		const items: TestItem[][] = [];
		const consumer = (async () => {
			for await (const group of queue) {
				items.push(group.items);
			}
		})();

		await queue.push({ task_key: "batched-1", id: "1" });
		await queue.push({ task_key: "batched-2", id: "2" });
		await queue.push({ task_key: "batched-1", id: "3" });
		await queue.push({ task_key: "batched-2", id: "4" });

		// Wait for emission
		await new Promise((r) => setTimeout(r, 10));

		queue.close();
		await consumer;

		// Each batch should have 2 items from same task
		expect(items.length).toBe(2);
		expect(items[0]).toEqual([
			{ task_key: "batched-1", id: "1" },
			{ task_key: "batched-1", id: "3" },
		]);
		expect(items[1]).toEqual([
			{ task_key: "batched-2", id: "2" },
			{ task_key: "batched-2", id: "4" },
		]);
	});

	test("mixed batched and non-batched items", async () => {
		const batchConfigs = new Map([["batched", { size: 3, timeoutMs: 1000 }]]);
		const queue = new BatchingAsyncQueue<TestItem>(10, batchConfigs);

		const items: TestItem[][] = [];
		const consumer = (async () => {
			for await (const group of queue) {
				items.push(group.items);
			}
		})();

		await queue.push({ task_key: "normal", id: "1" });
		await queue.push({ task_key: "batched", id: "2" });
		await queue.push({ task_key: "batched", id: "3" });
		await queue.push({ task_key: "normal", id: "4" });
		await queue.push({ task_key: "batched", id: "5" });

		// Wait for batch to emit
		await new Promise((r) => setTimeout(r, 10));

		queue.close();
		await consumer;

		// Normal items emitted immediately as singles
		// Batched items accumulated and emitted as batch of 3
		expect(items).toEqual([
			[{ task_key: "normal", id: "1" }],
			[{ task_key: "normal", id: "4" }],
			[
				{ task_key: "batched", id: "2" },
				{ task_key: "batched", id: "3" },
				{ task_key: "batched", id: "5" },
			],
		]);
	});

	test("close flushes pending batches", async () => {
		const batchConfigs = new Map([["batched", { size: 10, timeoutMs: 10000 }]]);
		const queue = new BatchingAsyncQueue<TestItem>(10, batchConfigs);

		const items: TestItem[][] = [];
		const consumer = (async () => {
			for await (const group of queue) {
				items.push(group.items);
			}
		})();

		await queue.push({ task_key: "batched", id: "1" });
		await queue.push({ task_key: "batched", id: "2" });

		// Close before timeout or size reached
		queue.close();
		await consumer;

		// Should still emit the partial batch
		expect(items).toEqual([
			[
				{ task_key: "batched", id: "1" },
				{ task_key: "batched", id: "2" },
			],
		]);
	});

	test("batch respects queue capacity", async () => {
		const batchConfigs = new Map([["batched", { size: 2, timeoutMs: 1000 }]]);
		const queue = new BatchingAsyncQueue<TestItem>(2, batchConfigs);

		// Push 4 batched items (will create 2 batches of 2)
		await queue.push({ task_key: "batched", id: "1" });
		await queue.push({ task_key: "batched", id: "2" }); // First batch ready

		await queue.push({ task_key: "batched", id: "3" });
		await queue.push({ task_key: "batched", id: "4" }); // Second batch ready

		// Queue now has 2 groups, capacity is full
		// Next push should wait because queue is at capacity
		let fifthPushResolved = false;
		const fifthPushPromise = queue.push({ task_key: "batched", id: "5" }).then(() => {
			fifthPushResolved = true;
		});

		await new Promise((r) => setTimeout(r, 20));
		expect(fifthPushResolved).toBe(false); // Should NOT resolve yet - queue is full

		// Consume one group to make space
		await queue.next();

		// Now the push should complete
		await new Promise((r) => setTimeout(r, 20));
		expect(fifthPushResolved).toBe(true);

		queue.close();
	});

	test("batched items deliver directly to waiting consumers", async () => {
		const batchConfigs = new Map([["batched", { size: 2, timeoutMs: 1000 }]]);
		const queue = new BatchingAsyncQueue<TestItem>(10, batchConfigs);

		// Start a consumer waiting for items
		const nextPromise = queue.next();

		// Push batched items
		await queue.push({ task_key: "batched", id: "1" });
		await queue.push({ task_key: "batched", id: "2" }); // Batch complete

		// Should deliver directly to waiting consumer
		const result = await nextPromise;
		expect(result.value).toEqual({
			taskKey: "batched",
			items: [
				{ task_key: "batched", id: "1" },
				{ task_key: "batched", id: "2" },
			],
		});

		queue.close();
	});
});
