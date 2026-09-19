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

	test("persists an unconditional subscription as a fallback DNF group", async () => {
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
				strategy: string;
				anchor_clause_number: number | null;
				clause_count: number;
				predicate_count: number;
			}[]
		>`
			select subscription.id, subscription.event_key, subscription.task_key,
				filter_group.strategy, filter_group.anchor_clause_number,
				count(distinct clause.clause_number)::integer as clause_count,
				count(predicate.predicate_number)::integer as predicate_count
			from pgconductor._private_custom_event_subscriptions subscription
			join pgconductor._private_event_filter_groups filter_group
				on filter_group.subscription_id = subscription.id
			left join pgconductor._private_event_filter_clauses clause
				on clause.subscription_id = filter_group.subscription_id
				and clause.group_number = filter_group.group_number
			left join pgconductor._private_event_filter_predicates predicate
				on predicate.subscription_id = clause.subscription_id
				and predicate.group_number = clause.group_number
				and predicate.clause_number = clause.clause_number
			where subscription.event_key = 'user.created'
			group by subscription.id, filter_group.strategy, filter_group.anchor_clause_number
		`;
		expect(stored).toEqual({
			id: expect.any(String),
			event_key: "user.created",
			task_key: "on-user-created",
			strategy: "fallback",
			anchor_clause_number: null,
			clause_count: 0,
			predicate_count: 0,
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

	test("normalizes typed exact filters and marks the complete first clause as anchor", async () => {
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
				field_name: string;
				clause_number: number;
				scalar_type: string;
				text_value: string | null;
				number_value: string | null;
				boolean_value: boolean | null;
				is_anchor: boolean;
			}[]
		>`
			select clause.field_name, clause.clause_number, predicate.scalar_type,
				predicate.text_value, predicate.number_value::text,
				predicate.boolean_value, predicate.is_anchor
			from pgconductor._private_custom_event_subscriptions subscription
			join pgconductor._private_event_filter_clauses clause
				on clause.subscription_id = subscription.id
			join pgconductor._private_event_filter_predicates predicate
				on predicate.subscription_id = clause.subscription_id
				and predicate.group_number = clause.group_number
				and predicate.clause_number = clause.clause_number
			where subscription.event_key = 'user.qualified'
			order by clause.clause_number, predicate.predicate_number
		`;
		expect([...rows]).toEqual([
			{
				field_name: "active",
				clause_number: 1,
				scalar_type: "boolean",
				text_value: null,
				number_value: null,
				boolean_value: true,
				is_anchor: true,
			},
			{
				field_name: "region",
				clause_number: 2,
				scalar_type: "string",
				text_value: "eu",
				number_value: null,
				boolean_value: null,
				is_anchor: false,
			},
			{
				field_name: "region",
				clause_number: 2,
				scalar_type: "string",
				text_value: "us",
				number_value: null,
				boolean_value: null,
				is_anchor: false,
			},
			{
				field_name: "score",
				clause_number: 3,
				scalar_type: "number",
				text_value: null,
				number_value: "7",
				boolean_value: null,
				is_anchor: false,
			},
		]);

		await orchestrator.stop();
		const [counts] = await db.sql<{ groups: number; clauses: number; predicates: number }[]>`
			select
				(select count(*)::integer from pgconductor._private_event_filter_groups) as groups,
				(select count(*)::integer from pgconductor._private_event_filter_clauses) as clauses,
				(select count(*)::integer from pgconductor._private_event_filter_predicates) as predicates
		`;
		expect(counts).toEqual({ groups: 1, clauses: 3, predicates: 4 });
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
		const [normalized] = await db.sql<{ groups: number; orphan_groups: number }[]>`
			select
				count(*)::integer as groups,
				count(*) filter (where subscription.id is null)::integer as orphan_groups
			from pgconductor._private_event_filter_groups filter_group
			left join pgconductor._private_custom_event_subscriptions subscription
				on subscription.id = filter_group.subscription_id
		`;
		expect(normalized).toEqual({ groups: 1, orphan_groups: 0 });
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
