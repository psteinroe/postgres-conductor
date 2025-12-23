import { test, expect, describe, beforeAll, afterAll, afterEach, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TaskSchemas } from "../../src/schemas";
import { EventSchemas } from "../../src/schemas";
import { DatabaseSchema } from "../../src/schemas";
import { z } from "zod";
import { TestDatabasePool, TestDatabase } from "../fixtures/test-database";
import type { Database } from "../database.types";

describe("Event Subscription Lifecycle", () => {
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

	test("custom event triggers are created on orchestrator start", async () => {
		const db = await pool.child();
		databases.push(db);

		const userCreated = defineEvent({
			name: "user.created",
			payload: z.object({ userId: z.string() }),
		});

		const taskDef = defineTask({
			name: "on-user-created",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "on-user-created" },
			{ event: "user.created" },
			mock(async () => {}),
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Check subscription was created
		const [sub] = await db.sql<[{ event_key: string; task_key: string }]>`
			select event_key, task_key
			from pgconductor._private_event_subscriptions
			where event_key = 'user.created'
		`;

		expect(sub).toBeTruthy();
		expect(sub.event_key).toBe("user.created");
		expect(sub.task_key).toBe("on-user-created");

		// Check trigger function was created
		const [trigger] = await db.sql<[{ exists: boolean }]>`
			select exists(
				select 1
				from pg_trigger
				where tgname = 'pgconductor_custom_event'
					and tgrelid = 'pgconductor._private_custom_events'::regclass
			)
		`;

		expect(trigger.exists).toBe(true);

		await orchestrator.stop();
	}, 30000);

	test("database triggers are created on orchestrator start", async () => {
		const db = await pool.child();
		databases.push(db);

		// Create contact table
		await db.sql`
			create table if not exists public.contact (
				id text primary key default gen_random_uuid()::text,
				email text,
				name text
			)
		`;

		const taskDef = defineTask({
			name: "on-contact-insert",
			payload: z.object({}),
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
			mock(async () => {}),
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Check subscription was created
		const [sub] = await db.sql<[{ schema_name: string; table_name: string; operation: string }]>`
			select schema_name, table_name, operation::text
			from pgconductor._private_event_subscriptions
			where schema_name = 'public' and table_name = 'contact' and operation = 'insert'
		`;

		expect(sub).toBeTruthy();
		expect(sub.schema_name).toBe("public");
		expect(sub.table_name).toBe("contact");
		expect(sub.operation).toBe("insert");

		// Check trigger was created on table
		const [trigger] = await db.sql<[{ exists: boolean }]>`
			select exists(
				select 1
				from pg_trigger
				where tgname = 'pgconductor_event_insert'
					and tgrelid = 'public.contact'::regclass
			)
		`;

		expect(trigger.exists).toBe(true);

		await orchestrator.stop();
	}, 30000);

	test("stopping orchestrator does not remove triggers", async () => {
		const db = await pool.child();
		databases.push(db);

		const userCreated = defineEvent({
			name: "user.created.persistent",
			payload: z.object({ userId: z.string() }),
		});

		const taskDef = defineTask({
			name: "on-user-persistent",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "on-user-persistent" },
			{ event: "user.created.persistent" },
			mock(async () => {}),
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Verify trigger exists
		const [beforeStop] = await db.sql<[{ exists: boolean }]>`
			select exists(
				select 1
				from pg_trigger
				where tgname = 'pgconductor_custom_event'
					and tgrelid = 'pgconductor._private_custom_events'::regclass
			)
		`;
		expect(beforeStop.exists).toBe(true);

		await orchestrator.stop();

		// Verify trigger still exists after stop
		const [afterStop] = await db.sql<[{ exists: boolean }]>`
			select exists(
				select 1
				from pg_trigger
				where tgname = 'pgconductor_custom_event'
					and tgrelid = 'pgconductor._private_custom_events'::regclass
			)
		`;
		expect(afterStop.exists).toBe(true);

		// Verify subscription still exists
		const [sub] = await db.sql<[{ exists: boolean }]>`
			select exists(
				select 1
				from pgconductor._private_event_subscriptions
				where event_key = 'user.created.persistent'
			)
		`;
		expect(sub.exists).toBe(true);
	}, 30000);

	test("triggers are recreated when subscriptions change", async () => {
		const db = await pool.child();
		databases.push(db);

		const userCreated = defineEvent({
			name: "user.created.change",
			payload: z.object({ userId: z.string(), email: z.string() }),
		});

		const taskDef1 = defineTask({
			name: "task-version-1",
			payload: z.object({}),
		});

		const taskDef2 = defineTask({
			name: "task-version-2",
			payload: z.object({}),
		});

		// First orchestrator with no field selection
		const conductor1 = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef1]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		const task1 = conductor1.createTask(
			{ name: "task-version-1" },
			{ event: "user.created.change" },
			mock(async () => {}),
		);

		const orchestrator1 = Orchestrator.create({
			conductor: conductor1,
			tasks: [task1],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator1.start();

		// Check subscription without field selection
		const [sub1] = await db.sql<[{ payload_fields: string[] | null }]>`
			select payload_fields
			from pgconductor._private_event_subscriptions
			where event_key = 'user.created.change'
		`;
		expect(sub1.payload_fields).toBeNull();

		await orchestrator1.stop();

		// Second orchestrator with field selection
		const conductor2 = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef2]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		const orchestrator2 = Orchestrator.create({
			conductor: conductor2,
			tasks: [
				conductor2.createTask(
					{ name: "task-version-2" },
					{ event: "user.created.change", fields: "userId" },
					mock(async () => {}),
				),
			],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator2.start();

		// Check subscription was updated with field selection
		const [sub2] = await db.sql<[{ payload_fields: string[]; task_key: string }]>`
			select payload_fields, task_key
			from pgconductor._private_event_subscriptions
			where event_key = 'user.created.change'
		`;
		expect(sub2.payload_fields).toEqual(["userId"]);
		expect(sub2.task_key).toBe("task-version-2");

		await orchestrator2.stop();
	}, 30000);

	test("multiple workers can register different subscriptions on same event", async () => {
		const db = await pool.child();
		databases.push(db);

		const userCreated = defineEvent({
			name: "user.created.multi",
			payload: z.object({ userId: z.string() }),
		});

		const taskDef1 = defineTask({
			name: "handler-1",
			payload: z.object({}),
		});

		const taskDef2 = defineTask({
			name: "handler-2",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef1, taskDef2]),
			events: EventSchemas.fromSchema([userCreated]),
			context: {},
		});

		const task1 = conductor.createTask(
			{ name: "handler-1" },
			{ event: "user.created.multi" },
			mock(async () => {}),
		);

		const task2 = conductor.createTask(
			{ name: "handler-2" },
			{ event: "user.created.multi" },
			mock(async () => {}),
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task1, task2],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Check both subscriptions exist
		const subs = await db.sql<{ task_key: string }[]>`
			select task_key
			from pgconductor._private_event_subscriptions
			where event_key = 'user.created.multi'
			order by task_key
		`;

		expect(subs.length).toBe(2);
		if (subs[0] && subs[1]) {
			expect(subs[0].task_key).toBe("handler-1");
			expect(subs[1].task_key).toBe("handler-2");
		}

		await orchestrator.stop();
	}, 30000);

	test("when clause changes trigger recreation", async () => {
		const db = await pool.child();
		databases.push(db);

		// Create contact table
		await db.sql`
			create table if not exists public.contact (
				id text primary key default gen_random_uuid()::text,
				email text,
				active boolean default true
			)
		`;

		const taskDef = defineTask({
			name: "on-contact-conditional",
			payload: z.object({}),
		});

		// First orchestrator without when clause
		const conductor1 = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		const task1 = conductor1.createTask(
			{ name: "on-contact-conditional" },
			{ schema: "public", table: "contact", operation: "insert", columns: "id,email,active" },
			mock(async () => {}),
		);

		const orchestrator1 = Orchestrator.create({
			conductor: conductor1,
			tasks: [task1],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator1.start();

		// Check subscription without when clause
		const [sub1] = await db.sql<[{ when_clause: string | null }]>`
			select when_clause
			from pgconductor._private_event_subscriptions
			where schema_name = 'public' and table_name = 'contact'
		`;
		expect(sub1.when_clause).toBeNull();

		await orchestrator1.stop();

		// Second orchestrator with when clause
		const conductor2 = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			database: DatabaseSchema.fromGeneratedTypes<Database>(),
			context: {},
		});

		const task2 = conductor2.createTask(
			{ name: "on-contact-conditional" },
			{
				schema: "public",
				table: "contact",
				operation: "insert",
				when: "NEW.active = true",
				columns: "id,email,active",
			},
			mock(async () => {}),
		);

		const orchestrator2 = Orchestrator.create({
			conductor: conductor2,
			tasks: [task2],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator2.start();

		// Check subscription was updated with when clause
		const [sub2] = await db.sql<[{ when_clause: string }]>`
			select when_clause
			from pgconductor._private_event_subscriptions
			where schema_name = 'public' and table_name = 'contact'
		`;
		expect(sub2.when_clause).toBe("NEW.active = true");

		await orchestrator2.stop();
	}, 30000);
});
