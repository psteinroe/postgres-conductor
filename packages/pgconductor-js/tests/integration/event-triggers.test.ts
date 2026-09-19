import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TaskSchemas, EventSchemas } from "../../src/schemas";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

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
			filterable: ["total"],
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
			{ event: "order.placed", filter: { total: [1500] } },
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

	test("user-owned database triggers can emit transactional custom events", async () => {
		const db = await pool.child();
		databases.push(db);

		await db.sql`
			create table public.contact (
				id text primary key,
				email text not null
			)
		`;
		await db.sql`
			create function public.emit_contact_created()
			returns trigger
			language plpgsql
			as $$
			begin
				perform pgconductor.emit_event(
					'contact.created',
					jsonb_build_object('id', new.id, 'email', new.email)
				);
				return new;
			end;
			$$
		`;
		await db.sql`
			create trigger emit_contact_created
			after insert on public.contact
			for each row execute function public.emit_contact_created()
		`;

		const contactCreated = defineEvent({
			name: "contact.created",
			payload: z.object({ id: z.string(), email: z.string() }),
		});
		const taskDef = defineTask({ name: "handle-contact-created", payload: z.object({}) });
		const taskFn = mock(async () => {});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			events: EventSchemas.fromSchema([contactCreated]),
			context: {},
		});
		const task = conductor.createTask(
			{ name: "handle-contact-created" },
			{ event: "contact.created" },
			taskFn,
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});

		await orchestrator.start();
		await db.sql`insert into public.contact (id, email) values ('committed', 'ok@example.com')`;
		await waitForCondition(() => taskFn.mock.calls.length === 1);

		await expect(
			db.sql.begin(async (transaction) => {
				await transaction`
					insert into public.contact (id, email)
					values ('rolled-back', 'rollback@example.com')
				`;
				throw new Error("roll back");
			}),
		).rejects.toThrow("roll back");

		const [rolledBack] = await db.sql<{ count: number }[]>`
			select count(*)::int as count
			from pgconductor._private_executions
			where queue = 'pgconductor.internal'
				and task_key = 'pgconductor.event-dispatch'
				and payload ->> 'eventKey' = 'contact.created'
				and payload -> 'payload' ->> 'id' = 'rolled-back'
		`;
		expect(rolledBack?.count).toBe(0);

		await orchestrator.stop();
	}, 30000);
});
