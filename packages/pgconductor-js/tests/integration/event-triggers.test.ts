import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TaskSchemas, EventSchemas, DatabaseSchema } from "../../src/schemas";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import type { Database } from "../database.types";

describe("Event Triggers - Custom Events", () => {
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

	test("task triggered by custom event", async () => {
		const db = await pool.child();
		databases.push(db);

		const userCreated = defineEvent({
			name: "user.created",
			payload: z.object({ userId: z.string(), email: z.string() }),
		});

		const taskDef = defineTask({
			name: "on-user-created",
			payload: z.object({}),
		});

		const taskFn = mock(async (event) => {
			expect(event.name).toBe("user.created");
			expect(event.payload.userId).toBe("user-123");
			expect(event.payload.email).toBe("test@example.com");
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		conductor.createTask({ name: "on-user-created" }, { event: "user.created" }, taskFn);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [conductor.createTask({ name: "on-user-created" }, { event: "user.created" }, taskFn)],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Emit event
		await conductor.emit("user.created", { userId: "user-123", email: "test@example.com" });

		// Wait for task to execute
		await new Promise((r) => setTimeout(r, 300));

		expect(taskFn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);

	test("custom event with field selection", async () => {
		const db = await pool.child();
		databases.push(db);

		const userCreated = defineEvent({
			name: "user.created.fields",
			payload: z.object({
				userId: z.string(),
				email: z.string(),
				name: z.string(),
				plan: z.string(),
			}),
		});

		const taskDef = defineTask({
			name: "on-user-created-fields",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		const taskFn = mock(async (event) => {
			// Should only have selected fields
			expect(event.payload.userId).toBe("user-123");
			expect(event.payload.email).toBe("test@example.com");
			// Non-selected fields should not exist
			expect(event.payload.name).toBeUndefined();
			expect(event.payload.plan).toBeUndefined();
		});

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [
				conductor.createTask(
					{ name: "on-user-created-fields" },
					{ event: "user.created.fields", fields: "userId,email" },
					taskFn,
				),
			],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Emit event with all fields
		await conductor.emit("user.created.fields", {
			userId: "user-123",
			email: "test@example.com",
			name: "John Doe",
			plan: "pro",
		});

		// Wait for task to execute
		await new Promise((r) => setTimeout(r, 300));

		expect(taskFn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);

	test("custom event with when clause filters events", async () => {
		const db = await pool.child();
		databases.push(db);

		const orderPlaced = defineEvent({
			name: "order.placed",
			payload: z.object({ orderId: z.string(), total: z.number() }),
		});

		const taskDef = defineTask({
			name: "on-large-order",
			payload: z.object({}),
		});

		const taskFn = mock(async (event) => {
			// Should only be called for orders > 1000
			expect(event.payload.total).toBeGreaterThan(1000);
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([orderPlaced]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "on-large-order" },
			{ event: "order.placed", when: "(new.payload->>'total')::numeric > 1000" },
			taskFn,
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Emit small order (should not trigger)
		await conductor.emit("order.placed", { orderId: "order-1", total: 500 });

		// Emit large order (should trigger)
		await conductor.emit("order.placed", { orderId: "order-2", total: 1500 });

		// Wait for tasks to execute
		await new Promise((r) => setTimeout(r, 300));

		// Should only be called once (for large order)
		expect(taskFn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);

	test("multiple tasks subscribe to same custom event", async () => {
		const db = await pool.child();
		databases.push(db);

		const userCreated = defineEvent({
			name: "user.created.multiple",
			payload: z.object({ userId: z.string() }),
		});

		const taskDef1 = defineTask({
			name: "send-welcome-email",
			payload: z.object({}),
		});

		const taskDef2 = defineTask({
			name: "create-profile",
			payload: z.object({}),
		});

		const task1Fn = mock(async () => {});
		const task2Fn = mock(async () => {});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef1, taskDef2]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		const task1 = conductor.createTask(
			{ name: "send-welcome-email" },
			{ event: "user.created.multiple" },
			task1Fn,
		);

		const task2 = conductor.createTask(
			{ name: "create-profile" },
			{ event: "user.created.multiple" },
			task2Fn,
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task1, task2],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Emit event once
		await conductor.emit("user.created.multiple", { userId: "user-123" });

		// Wait for both tasks to execute
		await new Promise((r) => setTimeout(r, 300));

		// Both tasks should be triggered
		expect(task1Fn).toHaveBeenCalledTimes(1);
		expect(task2Fn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);
});

describe("Event Triggers - Database CDC", () => {
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

	test("task triggered on INSERT", async () => {
		const db = await pool.child();
		databases.push(db);

		// Create contact table for testing
		await db.sql`
			create table if not exists public.contact (
				id text primary key default gen_random_uuid()::text,
				email text,
				name text,
				active boolean default true
			)
		`;

		// Insert initial data so table exists before orchestrator starts
		// (triggers are created during orchestrator.start(), so table must exist first)
		await db.sql`insert into public.contact (email, name) values ('initial@example.com', 'Initial')`;

		const taskDef = defineTask({
			name: "on-contact-insert",
			payload: z.object({}),
		});

		const taskFn = mock(async (event) => {
			expect(event.name).toBe("public.contact.insert");
			expect(event.payload.tg_op).toBe("INSERT");
			expect(event.payload.old).toBeNull();
			expect(event.payload.new).toBeTruthy();
			expect(event.payload.new.email).toBe("test@example.com");
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "on-contact-insert" },
			{ schema: "public", table: "contact", operation: "insert", columns: "id,email,name" },
			taskFn,
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Insert a contact (this should trigger the task)
		await db.sql`insert into public.contact (email, name) values ('test@example.com', 'Test User')`;

		// Wait for task to execute
		await new Promise((r) => setTimeout(r, 300));

		expect(taskFn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);

	test("task triggered on UPDATE", async () => {
		const db = await pool.child();
		databases.push(db);

		// Create contact table
		await db.sql`
			create table if not exists public.contact (
				id text primary key default gen_random_uuid()::text,
				email text,
				name text,
				active boolean default true
			)
		`;

		// Insert initial contact
		const [contact] = await db.sql<[{ id: string }]>`
			insert into public.contact (email, name)
			values ('test@example.com', 'Test User')
			returning id
		`;

		const taskDef = defineTask({
			name: "on-contact-update",
			payload: z.object({}),
		});

		const taskFn = mock(async (event) => {
			expect(event.name).toBe("public.contact.update");
			expect(event.payload.tg_op).toBe("UPDATE");
			expect(event.payload.old).toBeTruthy();
			expect(event.payload.new).toBeTruthy();
			expect(event.payload.old.name).toBe("Test User");
			expect(event.payload.new.name).toBe("Updated User");
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "on-contact-update" },
			{ schema: "public", table: "contact", operation: "update", columns: "id,email,name" },
			taskFn,
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Update the contact
		await db.sql`update public.contact set name = 'Updated User' where id = ${contact?.id}`;

		// Wait for task to execute
		await new Promise((r) => setTimeout(r, 300));

		expect(taskFn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);

	test("task triggered on DELETE", async () => {
		const db = await pool.child();
		databases.push(db);

		// Create contact table
		await db.sql`
			create table if not exists public.contact (
				id text primary key default gen_random_uuid()::text,
				email text,
				name text,
				active boolean default true
			)
		`;

		// Insert initial contact
		const [contact] = await db.sql<[{ id: string }]>`
			insert into public.contact (email, name)
			values ('test@example.com', 'Test User')
			returning id
		`;

		const taskDef = defineTask({
			name: "on-contact-delete",
			payload: z.object({}),
		});

		const taskFn = mock(async (event) => {
			expect(event.name).toBe("public.contact.delete");
			expect(event.payload.tg_op).toBe("DELETE");
			expect(event.payload.old).toBeTruthy();
			expect(event.payload.new).toBeNull();
			expect(event.payload.old.email).toBe("test@example.com");
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "on-contact-delete" },
			{ schema: "public", table: "contact", operation: "delete", columns: "id,email,name" },
			taskFn,
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Delete the contact
		await db.sql`delete from public.contact where id = ${contact?.id}`;

		// Wait for task to execute
		await new Promise((r) => setTimeout(r, 300));

		expect(taskFn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);

	test("database trigger with column selection", async () => {
		const db = await pool.child();
		databases.push(db);

		// Create contact table for testing
		await db.sql`
			create table if not exists public.contact (
				id text primary key default gen_random_uuid()::text,
				email text,
				name text,
				active boolean default true
			)
		`;

		const taskDef = defineTask({
			name: "on-contact-update-columns",
			payload: z.object({}),
		});

		const taskFn = mock(async (event) => {
			// Should only have selected columns
			expect(event.payload.new.id).toBeTruthy();
			expect(event.payload.new.email).toBe("updated@example.com");
			// Non-selected columns should not exist
			expect(event.payload.new.name).toBeUndefined();
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "on-contact-update-columns" },
			{ schema: "public", table: "contact", operation: "update", columns: "id,email" },
			taskFn,
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Insert and update a contact
		const [contact] = await db.sql<[{ id: string }]>`
			insert into public.contact (email, name)
			values ('test@example.com', 'Test User')
			returning id
		`;
		await db.sql`update public.contact set email = 'updated@example.com', name = 'Updated Name' where id = ${contact?.id}`;

		// Wait for task to execute
		await new Promise((r) => setTimeout(r, 300));

		expect(taskFn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);

	test("database trigger with when clause filters rows", async () => {
		const db = await pool.child();
		databases.push(db);

		// Create contact table for testing
		await db.sql`
			create table if not exists public.contact (
				id text primary key default gen_random_uuid()::text,
				email text,
				name text,
				active boolean default true
			)
		`;

		const taskDef = defineTask({
			name: "on-contact-active",
			payload: z.object({}),
		});

		const taskFn = mock(async (event) => {
			// Should only be called for active contacts
			expect(event.payload.new.active).toBe(true);
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "on-contact-active" },
			{
				schema: "public",
				table: "contact",
				operation: "insert",
				when: "NEW.active = true",
				columns: "id,email,name,active",
			},
			taskFn,
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Insert inactive contact (should not trigger)
		await db.sql`insert into public.contact (email, name, active) values ('inactive@example.com', 'Inactive', false)`;

		// Insert active contact (should trigger)
		await db.sql`insert into public.contact (email, name, active) values ('active@example.com', 'Active', true)`;

		// Wait for tasks to execute
		await new Promise((r) => setTimeout(r, 300));

		// Should only be called once (for active contact)
		expect(taskFn).toHaveBeenCalledTimes(1);

		await orchestrator.stop();
	}, 30000);
});
