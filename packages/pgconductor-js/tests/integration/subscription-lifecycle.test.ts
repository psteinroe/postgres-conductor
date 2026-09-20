import { afterAll, afterEach, beforeAll, describe, expect, mock, test } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineEvent } from "../../src/event-definition";
import { Orchestrator } from "../../src/orchestrator";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

describe("event subscription lifecycle", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const orchestrators: Orchestrator[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60_000);

	afterEach(async () => {
		await Promise.all(orchestrators.map((orchestrator) => orchestrator.stop()));
		orchestrators.length = 0;
		await Promise.all(databases.map((database) => database.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	async function database(): Promise<TestDatabase> {
		const db = await pool.child();
		databases.push(db);
		return db;
	}

	test("persists an unconditional subscription with one fallback anchor", async () => {
		const db = await database();
		const event = defineEvent({
			name: "user.created",
			payload: z.object({ userId: z.string() }),
		});
		const definition = defineTask({ name: "on-user-created", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const handler = mock(async () => {});
		const task = conductor.createTask(
			{ name: "on-user-created" },
			{ event: "user.created" },
			handler,
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		orchestrators.push(orchestrator);

		await orchestrator.start();

		const [stored] = await db.sql<
			{
				id: string;
				event_key: string;
				task_key: string;
				filter: Record<string, unknown>;
				operator: string;
				field_name: string | null;
			}[]
		>`
			select subscription.id, subscription.event_key, subscription.task_key,
				subscription.filter, anchor.operator, anchor.field_name
			from pgconductor._private_custom_event_subscriptions subscription
			join pgconductor._private_event_filter_anchors anchor
				on anchor.subscription_id = subscription.id
			where subscription.event_key = 'user.created'
		`;
		expect(stored).toEqual({
			id: expect.any(String),
			event_key: "user.created",
			task_key: "on-user-created",
			filter: {},
			operator: "fallback",
			field_name: null,
		});

		const eventId = await conductor.emit("user.created", { userId: "user-123" });
		await waitForCondition(() => handler.mock.calls.length === 1);
		await waitForCondition(async () => {
			const [source] = await db.sql<{ exists: boolean }[]>`
				select exists(
					select 1 from pgconductor._private_executions
					where id = ${eventId}::uuid and queue = 'pgconductor.internal'
				) as exists
			`;
			return source?.exists === false;
		});
	}, 30_000);

	test("stores the canonical filter and only its selected candidate anchors", async () => {
		const db = await database();
		const event = defineEvent({
			name: "user.qualified",
			payload: z.object({ active: z.boolean(), region: z.string(), score: z.number() }),
			filterable: ["active", "region", "score"],
		});
		const definition = defineTask({ name: "on-qualified-user", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const task = conductor.createTask(
			{ name: "on-qualified-user" },
			{
				event: "user.qualified",
				filter: { score: [7], region: ["us", "eu", "us"], active: [true] },
			},
			mock(async () => {}),
		);
		const orchestrator = Orchestrator.create({ conductor, tasks: [task] });
		orchestrators.push(orchestrator);
		await orchestrator.start();

		const rows = await db.sql<
			{
				filter: Record<string, unknown[]>;
				field_name: string;
				operator: string;
				scalar_type: string;
				boolean_value: boolean | null;
			}[]
		>`
			select subscription.filter, anchor.field_name, anchor.operator,
				anchor.scalar_type, anchor.boolean_value
			from pgconductor._private_custom_event_subscriptions subscription
			join pgconductor._private_event_filter_anchors anchor
				on anchor.subscription_id = subscription.id
			where subscription.event_key = 'user.qualified'
			order by anchor.anchor_number
		`;
		expect([...rows]).toEqual([
			{
				filter: { active: [true], region: ["eu", "us"], score: [7] },
				field_name: "active",
				operator: "exact",
				scalar_type: "boolean",
				boolean_value: true,
			},
		]);

		await orchestrator.stop();
		const [counts] = await db.sql<{ subscriptions: number; anchors: number }[]>`
			select
				(select count(*)::integer from pgconductor._private_custom_event_subscriptions)
					as subscriptions,
				(select count(*)::integer from pgconductor._private_event_filter_anchors) as anchors
		`;
		expect(counts).toEqual({ subscriptions: 1, anchors: 1 });
	}, 30_000);

	test("replaces only the registered queue subscription snapshot", async () => {
		const db = await database();
		const event = defineEvent({
			name: "user.changed",
			payload: z.object({ userId: z.string(), email: z.string() }),
		});
		const firstDefinition = defineTask({ name: "first-handler", payload: z.object({}) });
		const secondDefinition = defineTask({ name: "second-handler", payload: z.object({}) });

		const firstConductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([firstDefinition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const first = Orchestrator.create({
			conductor: firstConductor,
			tasks: [
				firstConductor.createTask(
					{ name: "first-handler" },
					{ event: "user.changed" },
					mock(async () => {}),
				),
			],
		});
		orchestrators.push(first);
		await first.start();
		await first.stop();

		const secondConductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([secondDefinition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const second = Orchestrator.create({
			conductor: secondConductor,
			tasks: [
				secondConductor.createTask(
					{ name: "second-handler" },
					{ event: "user.changed", fields: "userId" },
					mock(async () => {}),
				),
			],
		});
		orchestrators.push(second);
		await second.start();

		const subscriptions = await db.sql<{ task_key: string; payload_fields: string[] | null }[]>`
			select task_key, payload_fields
			from pgconductor._private_custom_event_subscriptions
			where queue = 'default' and event_key = 'user.changed'
		`;
		expect([...subscriptions]).toEqual([
			{ task_key: "second-handler", payload_fields: ["userId"] },
		]);
		const [normalized] = await db.sql<{ anchors: number; orphan_anchors: number }[]>`
			select
				count(*)::integer as anchors,
				count(*) filter (where subscription.id is null)::integer as orphan_anchors
			from pgconductor._private_event_filter_anchors anchor
			left join pgconductor._private_custom_event_subscriptions subscription
				on subscription.id = anchor.subscription_id
		`;
		expect(normalized).toEqual({ anchors: 1, orphan_anchors: 0 });
	}, 30_000);

	test("stores multiple subscriptions on the same event", async () => {
		const db = await database();
		const event = defineEvent({
			name: "user.multi",
			payload: z.object({ userId: z.string() }),
		});
		const firstDefinition = defineTask({ name: "handler-1", payload: z.object({}) });
		const secondDefinition = defineTask({ name: "handler-2", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([firstDefinition, secondDefinition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [
				conductor.createTask(
					{ name: "handler-1" },
					{ event: "user.multi" },
					mock(async () => {}),
				),
				conductor.createTask(
					{ name: "handler-2" },
					{ event: "user.multi" },
					mock(async () => {}),
				),
			],
		});
		orchestrators.push(orchestrator);
		await orchestrator.start();

		const subscriptions = await db.sql<{ task_key: string }[]>`
			select task_key
			from pgconductor._private_custom_event_subscriptions
			where event_key = 'user.multi'
			order by task_key
		`;
		expect([...subscriptions]).toEqual([{ task_key: "handler-1" }, { task_key: "handler-2" }]);
	}, 30_000);
});
