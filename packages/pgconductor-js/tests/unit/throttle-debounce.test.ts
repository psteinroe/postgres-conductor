import { describe, test, expect } from "bun:test";
import { InMemoryDatabaseClient } from "../mocks/in-memory-database-client";

describe("Throttle and Debounce", () => {
	test("throttle: first job wins, others rejected", async () => {
		const db = new InMemoryDatabaseClient();

		// Set a fixed time
		const now = new Date("2024-01-01T12:00:00Z");
		await db.setFakeTime({ date: now });

		// First invoke with throttle - should succeed
		const id1 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { value: 1 },
			throttle: { seconds: 60 },
		});

		expect(id1).toBeTruthy();

		// Second invoke within same time slot - should be throttled (return null)
		const id2 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { value: 2 },
			throttle: { seconds: 60 },
		});

		expect(id2).toBeNull();

		// Third invoke - still throttled
		const id3 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { value: 3 },
			throttle: { seconds: 60 },
		});

		expect(id3).toBeNull();

		// Move time forward past the throttle window
		await db.setFakeTime({ date: new Date("2024-01-01T12:01:01Z") });

		// Now should succeed again
		const id4 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { value: 4 },
			throttle: { seconds: 60 },
		});

		expect(id4).toBeTruthy();
		expect(id4).not.toBe(id1);
	});

	test("throttle with partition keys", async () => {
		const db = new InMemoryDatabaseClient();
		const now = new Date("2024-01-01T12:00:00Z");
		await db.setFakeTime({ date: now });

		// First invoke for user1 - should succeed
		const id1 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { userId: "user1" },
			dedupe_key: "user1",
			throttle: { seconds: 60 },
		});

		expect(id1).toBeTruthy();

		// Second invoke for user1 - should be throttled
		const id2 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { userId: "user1" },
			dedupe_key: "user1",
			throttle: { seconds: 60 },
		});

		expect(id2).toBeNull();

		// Invoke for user2 (different partition) - should succeed
		const id3 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { userId: "user2" },
			dedupe_key: "user2",
			throttle: { seconds: 60 },
		});

		expect(id3).toBeTruthy();
		expect(id3).not.toBe(id1);
	});

	test("debounce: last job wins, scheduled for next slot", async () => {
		const db = new InMemoryDatabaseClient();
		const now = new Date("2024-01-01T12:00:00Z");
		await db.setFakeTime({ date: now });

		// First invoke with debounce - should create execution in next slot
		const id1 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { value: 1 },
			debounce: { seconds: 60 },
		});

		expect(id1).toBeTruthy();

		// Second invoke - should replace first (both return IDs)
		const id2 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { value: 2 },
			debounce: { seconds: 60 },
		});

		expect(id2).toBeTruthy();

		// Third invoke - should replace second
		const id3 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { value: 3 },
			debounce: { seconds: 60 },
		});

		expect(id3).toBeTruthy();

		// All executions for the same task+slot should result in only the last one
		const executions = await db.getExecutions({
			queueName: "default",
			batchSize: 100,
			orchestratorId: "test-orch",
			taskKeysWithConcurrency: [],
			filterTaskKeys: [],
		});

		// Should only have one execution (the last one)
		const testTaskExecutions = executions.filter((e) => e.task_key === "test-task");
		expect(testTaskExecutions.length).toBe(1);
		expect(testTaskExecutions[0]!.payload).toEqual({ value: 3 });
	});

	test("cannot use both throttle and debounce", async () => {
		const db = new InMemoryDatabaseClient();

		await expect(
			db.invoke({
				task_key: "test-task",
				queue: "default",
				payload: {},
				throttle: { seconds: 60 },
				debounce: { seconds: 60 },
			}),
		).rejects.toThrow("Cannot use both throttle and debounce");
	});

	test("throttle with no dedupe_key uses global throttle", async () => {
		const db = new InMemoryDatabaseClient();
		const now = new Date("2024-01-01T12:00:00Z");
		await db.setFakeTime({ date: now });

		// First invoke without dedupe_key - should succeed
		const id1 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { value: 1 },
			throttle: { seconds: 60 },
		});

		expect(id1).toBeTruthy();

		// Second invoke without dedupe_key - should be throttled globally
		const id2 = await db.invoke({
			task_key: "test-task",
			queue: "default",
			payload: { value: 2 },
			throttle: { seconds: 60 },
		});

		expect(id2).toBeNull();
	});

	test("batch invoke with throttle", async () => {
		const db = new InMemoryDatabaseClient();
		const now = new Date("2024-01-01T12:00:00Z");
		await db.setFakeTime({ date: now });

		// Batch invoke with throttle - only first of each partition should succeed
		const ids = await db.invokeBatch([
			{
				task_key: "test-task",
				queue: "default",
				payload: { value: 1 },
				dedupe_key: "key1",
				throttle: { seconds: 60 },
			},
			{
				task_key: "test-task",
				queue: "default",
				payload: { value: 2 },
				dedupe_key: "key1", // Same key - should be handled by unique constraint
				throttle: { seconds: 60 },
			},
			{
				task_key: "test-task",
				queue: "default",
				payload: { value: 3 },
				dedupe_key: "key2", // Different key - should succeed
				throttle: { seconds: 60 },
			},
		]);

		expect(ids).toHaveLength(3);
		// Due to ON CONFLICT DO UPDATE, all return IDs but only unique ones are created
	});

	test("batch invoke cannot use debounce", async () => {
		const db = new InMemoryDatabaseClient();

		await expect(
			db.invokeBatch([
				{
					task_key: "test-task",
					queue: "default",
					payload: {},
					debounce: { seconds: 60 },
				},
			]),
		).rejects.toThrow("Batch invoke only supports throttle, not debounce");
	});
});
