import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { TestDatabasePool } from "../fixtures/test-database";
import { Conductor } from "../../src/conductor";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { z } from "zod";

let pool: TestDatabasePool;

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterAll(async () => {
	await pool?.destroy();
});

describe("Throttle and Debounce Integration", () => {
	test("throttle: first job wins, others rejected", async () => {
		const db = await pool.child();
		const taskDefinition = defineTask({
			name: "test-task",
			payload: z.any(),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		await conductor.ensureInstalled();

		// First invoke with throttle - should succeed
		const id1 = await conductor.invoke(
			{ name: "test-task" },
			{ value: 1 },
			{ throttle: { seconds: 60 } },
		);

		expect(id1).toBeTruthy();

		// Second invoke within same time slot - should be throttled (return null)
		const id2 = await conductor.invoke(
			{ name: "test-task" },
			{ value: 2 },
			{ throttle: { seconds: 60 } },
		);

		expect(id2).toBeNull();

		// Third invoke - still throttled
		const id3 = await conductor.invoke(
			{ name: "test-task" },
			{ value: 3 },
			{ throttle: { seconds: 60 } },
		);

		expect(id3).toBeNull();

		// Verify only one execution exists
		const executions = await db.sql`
			select * from pgconductor.executions_default
			where task_key = 'test-task'
		`;

		expect(executions.length).toBe(1);
		expect(executions[0]?.payload).toEqual({ value: 1 });
	});

	test("throttle with partition keys", async () => {
		const db = await pool.child();
		const taskDefinition = defineTask({
			name: "test-task",
			payload: z.any(),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		await conductor.ensureInstalled();

		// First invoke for user1 - should succeed
		const id1 = await conductor.invoke(
			{ name: "test-task" },
			{ userId: "user1" },
			{
				dedupe_key: "user1",
				throttle: { seconds: 60 },
			},
		);

		expect(id1).toBeTruthy();

		// Second invoke for user1 - should be throttled
		const id2 = await conductor.invoke(
			{ name: "test-task" },
			{ userId: "user1" },
			{
				dedupe_key: "user1",
				throttle: { seconds: 60 },
			},
		);

		expect(id2).toBeNull();

		// Invoke for user2 (different partition) - should succeed
		const id3 = await conductor.invoke(
			{ name: "test-task" },
			{ userId: "user2" },
			{
				dedupe_key: "user2",
				throttle: { seconds: 60 },
			},
		);

		expect(id3).toBeTruthy();
		expect(id3).not.toBe(id1);

		// Verify two executions exist
		const executions = await db.sql`
			select * from pgconductor.executions_default
			where task_key = 'test-task'
			order by created_at
		`;

		expect(executions.length).toBe(2);
		expect(executions[0]?.dedupe_key).toBe("user1");
		expect(executions[1]?.dedupe_key).toBe("user2");
	});

	test("debounce: last job wins, scheduled for next slot", async () => {
		const db = await pool.child();
		const taskDefinition = defineTask({
			name: "test-task",
			payload: z.any(),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		await conductor.ensureInstalled();

		// First invoke with debounce - should create execution in next slot
		const id1 = await conductor.invoke(
			{ name: "test-task" },
			{ value: 1 },
			{ debounce: { seconds: 60 } },
		);

		expect(id1).toBeTruthy();

		// Second invoke - should replace first (both return IDs)
		const id2 = await conductor.invoke(
			{ name: "test-task" },
			{ value: 2 },
			{ debounce: { seconds: 60 } },
		);

		expect(id2).toBeTruthy();

		// Third invoke - should replace second
		const id3 = await conductor.invoke(
			{ name: "test-task" },
			{ value: 3 },
			{ debounce: { seconds: 60 } },
		);

		expect(id3).toBeTruthy();

		// All executions for the same task+slot should result in only the last one
		const executions = await db.sql`
			select * from pgconductor.executions_default
			where task_key = 'test-task'
		`;

		// Should only have one execution (the last one)
		expect(executions.length).toBe(1);
		expect(executions[0]?.payload).toEqual({ value: 3 });
	});

	test("debounce with partition keys", async () => {
		const db = await pool.child();
		const taskDefinition = defineTask({
			name: "test-task",
			payload: z.any(),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		await conductor.ensureInstalled();

		// First invoke for user1 - should succeed
		const id1 = await conductor.invoke(
			{ name: "test-task" },
			{ userId: "user1" },
			{
				dedupe_key: "user1",
				debounce: { seconds: 60 },
			},
		);

		expect(id1).toBeTruthy();

		// Second invoke for user1 - should replace first
		const id2 = await conductor.invoke(
			{ name: "test-task" },
			{ userId: "user1", value: 2 },
			{
				dedupe_key: "user1",
				debounce: { seconds: 60 },
			},
		);

		expect(id2).toBeTruthy();

		// Invoke for user2 (different partition) - should succeed
		const id3 = await conductor.invoke(
			{ name: "test-task" },
			{ userId: "user2" },
			{
				dedupe_key: "user2",
				debounce: { seconds: 60 },
			},
		);

		expect(id3).toBeTruthy();

		// Verify two executions exist (one per partition)
		const executions = await db.sql`
			select * from pgconductor.executions_default
			where task_key = 'test-task'
			order by dedupe_key
		`;

		expect(executions.length).toBe(2);
		expect(executions[0]?.dedupe_key).toBe("user1");
		expect(executions[0]?.payload).toEqual({ userId: "user1", value: 2 });
		expect(executions[1]?.dedupe_key).toBe("user2");
	});

	test("cannot use both throttle and debounce", async () => {
		const db = await pool.child();
		const taskDefinition = defineTask({
			name: "test-task",
			payload: z.any(),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		await conductor.ensureInstalled();

		await expect(
			conductor.invoke(
				{ name: "test-task" },
				{},
				{
					throttle: { seconds: 60 },
					debounce: { seconds: 60 },
				},
			),
		).rejects.toThrow("Cannot use both throttle and debounce");
	});

	test("throttle with no dedupe_key uses global throttle", async () => {
		const db = await pool.child();
		const taskDefinition = defineTask({
			name: "test-task",
			payload: z.any(),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		await conductor.ensureInstalled();

		// First invoke without dedupe_key - should succeed
		const id1 = await conductor.invoke(
			{ name: "test-task" },
			{ value: 1 },
			{ throttle: { seconds: 60 } },
		);

		expect(id1).toBeTruthy();

		// Second invoke without dedupe_key - should be throttled globally
		const id2 = await conductor.invoke(
			{ name: "test-task" },
			{ value: 2 },
			{ throttle: { seconds: 60 } },
		);

		expect(id2).toBeNull();

		// Verify only one execution exists
		const executions = await db.sql`
			select * from pgconductor.executions_default
			where task_key = 'test-task'
		`;

		expect(executions.length).toBe(1);
	});

	test.skip("batch invoke with throttle", async () => {
		const db = await pool.child();
		const taskDefinition = defineTask({
			name: "test-task",
			payload: z.any(),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		await conductor.ensureInstalled();

		// Batch invoke with throttle - uses ON CONFLICT DO UPDATE semantics
		const ids = await conductor.invoke({ name: "test-task" }, [
			{
				payload: { value: 1 },
				dedupe_key: "key1",
				throttle: { seconds: 60 },
			},
			{
				payload: { value: 2 },
				dedupe_key: "key1", // Same key - handled by unique constraint
				throttle: { seconds: 60 },
			},
			{
				payload: { value: 3 },
				dedupe_key: "key2", // Different key - should succeed
				throttle: { seconds: 60 },
			},
		]);

		expect(ids).toHaveLength(3);
		expect(Array.isArray(ids)).toBe(true);

		// Verify actual executions in database
		const executions = await db.sql`
			select * from pgconductor.executions_default
			where task_key = 'test-task'
			order by dedupe_key
		`;

		// Should have 2 unique executions (key1 and key2)
		expect(executions.length).toBe(2);
		expect(executions[0]?.dedupe_key).toBe("key1");
		expect(executions[1]?.dedupe_key).toBe("key2");
	});

	test("batch invoke cannot use debounce", async () => {
		const db = await pool.child();
		const taskDefinition = defineTask({
			name: "test-task",
			payload: z.any(),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		await conductor.ensureInstalled();

		await expect(
			conductor.invoke({ name: "test-task" }, [
				{
					payload: {},
					debounce: { seconds: 60 },
				},
			]),
		).rejects.toThrow("Batch invoke only supports throttle, not debounce");
	});

	test("throttle respects time slots - jobs in different slots succeed", async () => {
		const db = await pool.child();
		const taskDefinition = defineTask({
			name: "test-task",
			payload: z.any(),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		await conductor.ensureInstalled();

		// First invoke - should succeed
		const id1 = await conductor.invoke(
			{ name: "test-task" },
			{ value: 1 },
			{ throttle: { seconds: 2 } }, // 2 second slots
		);

		expect(id1).toBeTruthy();

		// Wait for slot to expire (2+ seconds)
		await new Promise((resolve) => setTimeout(resolve, 2100));

		// Second invoke in new slot - should succeed
		const id2 = await conductor.invoke(
			{ name: "test-task" },
			{ value: 2 },
			{ throttle: { seconds: 2 } },
		);

		expect(id2).toBeTruthy();
		expect(id2).not.toBe(id1);

		// Verify both executions exist
		const executions = await db.sql`
			select * from pgconductor.executions_default
			where task_key = 'test-task'
			order by created_at
		`;

		expect(executions.length).toBe(2);
	});
});
