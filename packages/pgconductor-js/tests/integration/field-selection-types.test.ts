import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TaskSchemas, EventSchemas } from "../../src/schemas";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";

describe("Field Selection - Type Safety & Runtime", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		await Promise.all(databases.map((db) => db.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	test("field selection preserves JSON types (numbers, booleans, objects, arrays)", async () => {
		const db = await pool.child();
		databases.push(db);

		const dataEvent = defineEvent({
			name: "data.test",
			payload: z.object({
				stringField: z.string(),
				numberField: z.number(),
				booleanField: z.boolean(),
				objectField: z.object({ nested: z.string() }),
				arrayField: z.array(z.string()),
				// Extra fields that should be filtered out
				extraString: z.string(),
				extraNumber: z.number(),
			}),
		});

		const taskDef = defineTask({
			name: "on-data-test",
			payload: z.object({}),
		});

		const taskFn = mock(async (event) => {
			// Type-level: TypeScript should know these fields exist with correct types
			const str: string = event.payload.stringField;
			const num: number = event.payload.numberField;
			const bool: boolean = event.payload.booleanField;
			const obj: { nested: string } = event.payload.objectField;
			const arr: string[] = event.payload.arrayField;

			// Runtime: Verify correct values and types
			expect(event.payload.stringField).toBe("test-string");
			expect(event.payload.numberField).toBe(42);
			expect(typeof event.payload.numberField).toBe("number");
			expect(event.payload.booleanField).toBe(true);
			expect(typeof event.payload.booleanField).toBe("boolean");
			expect(event.payload.objectField).toEqual({ nested: "value" });
			expect(typeof event.payload.objectField).toBe("object");
			expect(event.payload.arrayField).toEqual(["a", "b", "c"]);
			expect(Array.isArray(event.payload.arrayField)).toBe(true);

			// Non-selected fields should not exist at runtime
			expect(event.payload.extraString).toBeUndefined();
			expect(event.payload.extraNumber).toBeUndefined();
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([dataEvent]),
			context: {},
		});

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [
				conductor.createTask(
					{ name: "on-data-test" },
					{
						event: "data.test",
						fields: "stringField,numberField,booleanField,objectField,arrayField",
					},
					taskFn,
				),
			],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Emit event with all fields
		await conductor.emit("data.test", {
			stringField: "test-string",
			numberField: 42,
			booleanField: true,
			objectField: { nested: "value" },
			arrayField: ["a", "b", "c"],
			extraString: "should-be-filtered",
			extraNumber: 999,
		});

		// Wait for task to execute
		await new Promise((r) => setTimeout(r, 300));

		expect(taskFn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);

	test("field selection with camelCase field names", async () => {
		const db = await pool.child();
		databases.push(db);

		const userEvent = defineEvent({
			name: "user.signup",
			payload: z.object({
				userId: z.string(),
				emailAddress: z.string(),
				firstName: z.string(),
				lastName: z.string(),
				accountType: z.string(),
			}),
		});

		const taskDef = defineTask({
			name: "on-user-signup",
			payload: z.object({}),
		});

		const taskFn = mock(async (event) => {
			// Type-level: Selected fields with correct camelCase
			const userId: string = event.payload.userId;
			const emailAddress: string = event.payload.emailAddress;

			// Runtime: Verify camelCase preserved
			expect(event.payload.userId).toBe("user-123");
			expect(event.payload.emailAddress).toBe("test@example.com");
			expect(event.payload.firstName).toBeUndefined();
			expect(event.payload.lastName).toBeUndefined();
			expect(event.payload.accountType).toBeUndefined();
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userEvent]),
			context: {},
		});

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [
				conductor.createTask(
					{ name: "on-user-signup" },
					{ event: "user.signup", fields: "userId,emailAddress" },
					taskFn,
				),
			],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		await conductor.emit("user.signup", {
			userId: "user-123",
			emailAddress: "test@example.com",
			firstName: "John",
			lastName: "Doe",
			accountType: "premium",
		});

		await new Promise((r) => setTimeout(r, 300));

		expect(taskFn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);

	test("no field selection - all fields available", async () => {
		const db = await pool.child();
		databases.push(db);

		const fullEvent = defineEvent({
			name: "full.event",
			payload: z.object({
				field1: z.string(),
				field2: z.number(),
				field3: z.boolean(),
			}),
		});

		const taskDef = defineTask({
			name: "on-full-event",
			payload: z.object({}),
		});

		const taskFn = mock(async (event) => {
			// Type-level: All fields should be available
			const f1: string = event.payload.field1;
			const f2: number = event.payload.field2;
			const f3: boolean = event.payload.field3;

			// Runtime: All fields should exist
			expect(event.payload.field1).toBe("value1");
			expect(event.payload.field2).toBe(123);
			expect(event.payload.field3).toBe(true);
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([fullEvent]),
			context: {},
		});

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [
				// No fields parameter - should get all fields
				conductor.createTask({ name: "on-full-event" }, { event: "full.event" }, taskFn),
			],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		await conductor.emit("full.event", {
			field1: "value1",
			field2: 123,
			field3: true,
		});

		await new Promise((r) => setTimeout(r, 300));

		expect(taskFn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);
});
