import { test, expect, describe, beforeAll, afterAll, afterEach, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TaskSchemas } from "../../src/schemas";
import { EventSchemas } from "../../src/schemas";
import { z } from "zod";
import { TestDatabasePool, TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

describe("Event Subscription Lifecycle", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const orchestrators: Orchestrator[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		await Promise.all(orchestrators.map((orchestrator) => orchestrator.stop()));
		orchestrators.length = 0;
		await Promise.all(databases.map((db) => db.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	test("custom event subscriptions are persisted and processed asynchronously", async () => {
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

		const taskFn = mock(async () => {});
		const task = conductor.createTask(
			{ name: "on-user-created" },
			{ event: "user.created" },
			taskFn,
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});
		orchestrators.push(orchestrator);

		await orchestrator.start();

		// Check the persistent subscription was created
		const [sub] = await db.sql<[{ id: string; event_key: string; task_key: string }]>`
			select id, event_key, task_key
			from pgconductor._private_custom_event_subscriptions
			where event_key = 'user.created'
		`;

		expect(sub).toBeTruthy();
		expect(sub.event_key).toBe("user.created");
		expect(sub.task_key).toBe("on-user-created");

		const [compiledFilter] = await db.sql<
			[{ filter: Record<string, unknown>; field_count: number; predicate_count: string }]
		>`
			select subscription.filter, subscription.field_count, count(predicate.id)::text as predicate_count
			from pgconductor._private_custom_event_subscriptions subscription
			left join pgconductor._private_custom_event_predicates predicate
				on predicate.subscription_id = subscription.id
			where subscription.id = ${sub.id}
			group by subscription.id
		`;
		expect(compiledFilter).toEqual({ filter: {}, field_count: 0, predicate_count: "0" });

		const eventId = await conductor.emit("user.created", { userId: "user-123" });
		await waitForCondition(() => taskFn.mock.calls.length === 1);
		await waitForCondition(async () => {
			const [source] = await db.sql<{ exists: boolean }[]>`
				select exists(
					select 1 from pgconductor._private_executions
					where id = ${eventId}::uuid and queue = 'pgconductor.internal'
				) as exists
			`;
			return !source?.exists;
		});
	}, 30000);

	test("custom event subscriptions and compiled filters persist after stop", async () => {
		const db = await pool.child();
		databases.push(db);

		const userCreated = defineEvent({
			name: "user.created.persistent",
			payload: z.object({ userId: z.string() }),
			filterable: ["userId"],
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
			{
				event: "user.created.persistent",
				filter: { userId: ["user-123", "user-456", "user-123"] },
			},
			mock(async () => {}),
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});
		orchestrators.push(orchestrator);

		await orchestrator.start();

		const [beforeStop] = await db.sql<[{ id: string; exists: boolean }]>`
			select subscription.id, exists(
				select 1
				from pgconductor._private_custom_event_predicates predicate
				where predicate.subscription_id = subscription.id
			) as exists
			from pgconductor._private_custom_event_subscriptions subscription
			where event_key = 'user.created.persistent'
		`;
		expect(beforeStop).toBeTruthy();
		expect(beforeStop.exists).toBe(true);

		const [invariants] = await db.sql<
			{
				predicates: number;
				field_count: number;
				events_match: boolean;
				has_expected_predicate: boolean;
			}[]
		>`
			select
				count(predicate.id)::integer as predicates,
				subscription.field_count,
				bool_and(predicate.event_key = subscription.event_key) as events_match,
				bool_or(
					predicate.field_name = 'userId'
					and predicate.value = '"user-123"'::jsonb
				) as has_expected_predicate
			from pgconductor._private_custom_event_subscriptions subscription
			join pgconductor._private_custom_event_predicates predicate
				on predicate.subscription_id = subscription.id
			where subscription.id = ${beforeStop.id}
			group by subscription.id
		`;
		expect(invariants).toEqual({
			predicates: 2,
			field_count: 1,
			events_match: true,
			has_expected_predicate: true,
		});

		await orchestrator.stop();

		const [afterStop] = await db.sql<[{ exists: boolean; filter_count: string }]>`
			select exists(
				select 1 from pgconductor._private_custom_event_subscriptions
				where id = ${beforeStop.id}
			) as exists,
			(
				select count(*)::text
				from pgconductor._private_custom_event_predicates predicate
				where predicate.subscription_id = ${beforeStop.id}
			) as filter_count
		`;
		expect(afterStop.exists).toBe(true);
		expect(afterStop.filter_count).toBe("2");
	}, 30000);

	test("custom event subscriptions are replaced when their configuration changes", async () => {
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
		orchestrators.push(orchestrator1);

		await orchestrator1.start();

		// Check subscription without field selection
		const [sub1] = await db.sql<[{ payload_fields: string[] | null }]>`
			select payload_fields
			from pgconductor._private_custom_event_subscriptions
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
		orchestrators.push(orchestrator2);

		await orchestrator2.start();

		// Check subscription was updated with field selection
		const [sub2] = await db.sql<[{ payload_fields: string[]; task_key: string }]>`
			select payload_fields, task_key
			from pgconductor._private_custom_event_subscriptions
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
		orchestrators.push(orchestrator);

		await orchestrator.start();

		// Check both subscriptions exist
		const subs = await db.sql<{ task_key: string }[]>`
			select task_key
			from pgconductor._private_custom_event_subscriptions
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
});
