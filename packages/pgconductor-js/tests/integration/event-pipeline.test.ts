import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import postgres from "postgres";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { DatabaseClient, type EventSubscriptionSpec } from "../../src/database-client";
import { defineEvent } from "../../src/event-definition";
import {
	compileEventFilterTerms,
	compileEventTrigger,
	type CompiledEventTrigger,
	type EventFilter,
} from "../../src/event-trigger-validation";
import { DefaultLogger } from "../../src/lib/logger";
import { Orchestrator } from "../../src/orchestrator";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const INTERNAL_QUEUE = "pgconductor.internal";
const DISPATCH_TASK = "pgconductor.event-dispatch";
const FANOUT_STEP = "pgconductor.internal.event-fanout.v1";

type CustomSubscription = {
	taskKey: string;
	eventKey: string;
	filter?: EventFilter;
	compiledFilter?: Pick<CompiledEventTrigger, "required_field_count" | "terms">;
	payloadFields?: string[];
	maxAttempts?: number;
};

describe("event pipeline", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60_000);

	afterEach(async () => {
		await Promise.all(databases.map((database) => database.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	async function database(): Promise<TestDatabase> {
		const db = await pool.child();
		databases.push(db);
		await Conductor.create({ sql: db.sql, context: {} }).ensureInstalled();
		return db;
	}

	async function registerSubscriptions(
		db: TestDatabase,
		subscriptions: CustomSubscription[],
	): Promise<void> {
		const eventSubscriptions: EventSubscriptionSpec[] = subscriptions.map((subscription) => {
			const filter = subscription.filter || null;
			return {
				task_key: subscription.taskKey,
				event_key: subscription.eventKey,
				payload_fields: subscription.payloadFields || null,
				required_field_count:
					subscription.compiledFilter?.required_field_count ||
					(filter ? Object.keys(filter).length : 0),
				terms: subscription.compiledFilter?.terms || compileEventFilterTerms(filter),
			};
		});
		await db.client.registerWorker({
			queueName: "default",
			taskSpecs: subscriptions.map((subscription) => ({
				key: subscription.taskKey,
				queue: "default",
				maxAttempts: subscription.maxAttempts || 3,
			})),
			cronSchedules: [],
			eventSubscriptions,
		});
	}

	async function claimEvent(
		db: TestDatabase,
		eventId: string,
		orchestratorId = crypto.randomUUID(),
	): Promise<string> {
		const claimed = await db.client.getExecutions({
			orchestratorId,
			queueName: INTERNAL_QUEUE,
			batchSize: 10,
			filterTaskKeys: [],
		});
		expect(claimed.some((execution) => execution.id === eventId)).toBe(true);
		return orchestratorId;
	}

	async function settleSource(
		db: TestDatabase,
		eventId: string,
		orchestratorId: string,
	): Promise<void> {
		await db.client.returnExecutions({
			count: 1,
			orchestratorId,
			completed: [
				{
					execution_id: eventId,
					orchestrator_id: orchestratorId,
					queue: INTERNAL_QUEUE,
					task_key: DISPATCH_TASK,
					status: "completed",
				},
			],
			failed: [],
			released: [],
			invokeChild: [],
			taskKeys: new Set([DISPATCH_TASK]),
		});
	}

	test("matches SQL numeric values without JavaScript precision loss", async () => {
		const db = await database();
		await registerSubscriptions(db, [
			{
				taskKey: "pipeline.numeric-destination",
				eventKey: "pipeline.numeric-boundary",
				filter: { value: [9007199254740992] },
			},
			{
				taskKey: "pipeline.numeric-range-destination",
				eventKey: "pipeline.numeric-boundary",
				filter: {
					value: [
						{
							$operator: "numeric_range",
							lower: 9007199254740992,
							lowerInclusive: false,
							upper: 9007199254740994,
							upperInclusive: false,
						},
					],
				},
			},
		]);

		const [rounded] = await db.sql<{ event_id: string }[]>`
			select pgconductor.emit_event(
				'pipeline.numeric-boundary',
				'{"value": 9007199254740993}'::jsonb
			) as event_id
		`;
		if (!rounded) throw new Error("expected rounded numeric event");
		const roundedId = rounded.event_id;
		const roundedOwner = await claimEvent(db, roundedId);
		await db.client.dispatchCustomEvents({ eventIds: [roundedId], orchestratorId: roundedOwner });

		const [exact] = await db.sql<{ event_id: string }[]>`
			select pgconductor.emit_event(
				'pipeline.numeric-boundary',
				'{"value": 9007199254740992}'::jsonb
			) as event_id
		`;
		if (!exact) throw new Error("expected exact numeric event");
		const exactId = exact.event_id;
		const exactOwner = await claimEvent(db, exactId);
		await db.client.dispatchCustomEvents({ eventIds: [exactId], orchestratorId: exactOwner });

		const destinations = await db.sql<{ parent_execution_id: string; task_keys: string[] }[]>`
			select parent_execution_id, array_agg(task_key order by task_key) as task_keys
			from pgconductor._private_executions
			where parent_execution_id in (${roundedId}::uuid, ${exactId}::uuid)
				and subscription_id is not null
			group by parent_execution_id
			order by parent_execution_id
		`;
		expect([...destinations]).toEqual(
			[
				{
					parent_execution_id: roundedId,
					task_keys: ["pipeline.numeric-range-destination"],
				},
				{
					parent_execution_id: exactId,
					task_keys: ["pipeline.numeric-destination"],
				},
			].sort((left, right) => left.parent_execution_id.localeCompare(right.parent_execution_id)),
		);
	}, 15_000);

	test("uses subscription identity rather than payload shape for direct invocations", async () => {
		const db = await database();
		const payload = z.object({
			event: z.string(),
			payload: z.object({ value: z.string() }),
		});
		const singleDefinition = defineTask({ name: "pipeline.direct-single", payload });
		const batchDefinition = defineTask({ name: "pipeline.direct-batch", payload });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([singleDefinition, batchDefinition]),
			context: {},
		});
		const received: unknown[] = [];
		const single = conductor.createTask(
			{ name: "pipeline.direct-single" },
			{ invocable: true },
			async (event) => {
				received.push(event);
			},
		);
		const batch = conductor.createTask(
			{ name: "pipeline.direct-batch", batch: { size: 10, timeoutMs: 10 } },
			{ invocable: true },
			async (events) => {
				received.push(...events);
			},
		);
		await conductor.invoke(
			{ name: "pipeline.direct-single" },
			{ event: "customer-action", payload: { value: "single" } },
		);
		await conductor.invoke(
			{ name: "pipeline.direct-batch" },
			{ event: "customer-action", payload: { value: "batch" } },
		);
		await Orchestrator.create({
			conductor,
			tasks: [single, batch],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		}).drain();

		expect(received).toEqual([
			{
				name: "pgconductor.invoke",
				payload: { event: "customer-action", payload: { value: "single" } },
			},
			{
				name: "pgconductor.invoke",
				payload: { event: "customer-action", payload: { value: "batch" } },
			},
		]);
	});

	test("stores the event only in its durable dispatch execution", async () => {
		const db = await database();
		const eventId = await db.client.emitEvent({
			eventKey: "pipeline.persisted",
			payload: { value: "ready" },
		});
		const [source] = await db.sql<
			{ id: string; queue: string; task_key: string; payload: Record<string, unknown> }[]
		>`
			select id, queue, task_key, payload
			from pgconductor._private_executions
			where id = ${eventId}::uuid and queue = ${INTERNAL_QUEUE}
		`;
		expect(source).toEqual({
			id: eventId,
			queue: INTERNAL_QUEUE,
			task_key: DISPATCH_TASK,
			payload: { eventKey: "pipeline.persisted", payload: { value: "ready" } },
		});
		const [schema] = await db.sql<{ event_table: string | null }[]>`
			select to_regclass('pgconductor._private_custom_events')::text as event_table
		`;
		expect(schema?.event_table).toBeNull();
	});

	test("validates event names and payload objects at the database boundary", async () => {
		const db = await database();
		await db.sql`
			do $$
			begin
				begin
					perform pgconductor.emit_event('', '{}'::jsonb);
					raise exception 'invalid event name was accepted';
				exception when others then
					if sqlerrm <> 'Event name must contain between 1 and 255 UTF-8 bytes' then
						raise;
					end if;
				end;

				begin
					perform pgconductor.emit_event('pipeline.invalid', '[]'::jsonb);
					raise exception 'invalid event payload was accepted';
				exception when others then
					if sqlerrm <> 'Event payload must be a JSON object' then
						raise;
					end if;
				end;

				begin
					perform pgconductor.emit_event('pipeline.invalid', null);
					raise exception 'null event payload was accepted';
				exception when others then
					if sqlerrm <> 'Event payload must be a JSON object' then
						raise;
					end if;
				end;
			end;
			$$
		`;
	}, 30_000);

	test("does not serialize same-key emitters for caller transaction lifetimes", async () => {
		const db = await database();
		const firstSql = postgres(db.url, { max: 1 });
		const secondSql = postgres(db.url, { max: 1 });
		let releaseFirst = () => {};
		let markFirstInserted = () => {};
		const release = new Promise<void>((resolve) => {
			releaseFirst = resolve;
		});
		const firstInserted = new Promise<void>((resolve) => {
			markFirstInserted = resolve;
		});
		const firstTransaction = firstSql.begin(async (transaction) => {
			await transaction`select pgconductor.emit_event('pipeline.concurrent', '{}'::jsonb)`;
			markFirstInserted();
			await release;
		});

		try {
			await firstInserted;
			let timeout: Timer | undefined;
			const [second] = await Promise.race([
				secondSql`select pgconductor.emit_event('pipeline.concurrent', '{}'::jsonb) as id`,
				new Promise<never>((_, reject) => {
					timeout = setTimeout(
						() => reject(new Error("same-key emission was transaction-serialized")),
						2_000,
					);
				}),
			]);
			if (timeout) clearTimeout(timeout);
			expect(second?.id).toBeString();
		} finally {
			releaseFirst();
			await firstTransaction;
			await Promise.all([firstSql.end(), secondSql.end()]);
		}
	});

	test("matches typed DNF predicates, distinguishes missing from null, and projects payload", async () => {
		const db = await database();
		const event = defineEvent({
			name: "pipeline.order",
			payload: z.object({
				status: z.enum(["paid", "trial", "cancelled"]),
				region: z.string(),
				attempt: z.number(),
				active: z.boolean(),
				coupon: z.string().nullable().optional(),
			}),
			filterable: ["status", "region", "attempt", "active", "coupon"],
		});
		const filteredDefinition = defineTask({ name: "pipeline.filtered", payload: z.object({}) });
		const nullDefinition = defineTask({ name: "pipeline.null", payload: z.object({}) });
		const allDefinition = defineTask({ name: "pipeline.all", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([filteredDefinition, nullDefinition, allDefinition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const received: { task: string; payload: Record<string, unknown> }[] = [];
		const filtered = conductor.createTask(
			{ name: "pipeline.filtered" },
			{
				event: "pipeline.order",
				filter: { status: ["paid", "trial"], region: ["us"], attempt: [1], active: [true] },
			},
			async (receivedEvent) => {
				received.push({ task: "filtered", payload: receivedEvent.payload });
			},
		);
		const explicitNull = conductor.createTask(
			{ name: "pipeline.null" },
			{ event: "pipeline.order", filter: { coupon: [null] } },
			async (receivedEvent) => {
				received.push({ task: "null", payload: receivedEvent.payload });
			},
		);
		const all = conductor.createTask(
			{ name: "pipeline.all" },
			{ event: "pipeline.order", fields: "status, region, coupon" },
			async (receivedEvent) => {
				received.push({ task: "all", payload: receivedEvent.payload });
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [filtered, explicitNull, all],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await conductor.emit("pipeline.order", {
			status: "paid",
			region: "us",
			attempt: 1,
			active: true,
			coupon: null,
		});
		await orchestrator.start();
		try {
			await conductor.emit("pipeline.order", {
				status: "trial",
				region: "us",
				attempt: 1,
				active: true,
			});
			await conductor.emit("pipeline.order", {
				status: "paid",
				region: "eu",
				attempt: 1,
				active: true,
				coupon: "SAVE",
			});
			await waitForCondition(() => received.length === 6);
			expect(received.filter((item) => item.task === "filtered")).toHaveLength(2);
			expect(received.filter((item) => item.task === "null")).toHaveLength(1);
			expect(received.filter((item) => item.task === "all")).toEqual([
				{ task: "all", payload: { status: "paid", region: "us", coupon: null } },
				{ task: "all", payload: { status: "trial", region: "us" } },
				{ task: "all", payload: { status: "paid", region: "eu", coupon: "SAVE" } },
			]);
		} finally {
			await orchestrator.stop();
		}
	});

	test("intersects every indexed filter field before returning subscriptions", async () => {
		const db = await database();
		await registerSubscriptions(
			db,
			Array.from({ length: 50 }, (_, index) => ({
				taskKey: `pipeline.inverted-${index}`,
				eventKey: "pipeline.inverted",
				filter: { status: ["paid"], tenantId: [`tenant-${index}`] },
			})),
		);

		const eventId = await db.client.emitEvent({
			eventKey: "pipeline.inverted",
			payload: { status: "paid", tenantId: "tenant-37" },
		});
		const owner = await claimEvent(db, eventId);
		await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId: owner });

		const destinations = await db.sql<{ task_key: string }[]>`
			select task_key
			from pgconductor._private_executions
			where parent_execution_id = ${eventId}::uuid and subscription_id is not null
		`;
		expect([...destinations]).toEqual([{ task_key: "pipeline.inverted-37" }]);
	});

	test("matches literal prefixes, numeric ranges, exists, and atomic anything-but", async () => {
		const db = await database();
		const compileFilter = (filter: Record<string, unknown[]>) => {
			const compiled = compileEventTrigger({ event: "pipeline.operators", filter }, [], true);
			if (compiled === null) throw new Error("expected an event trigger");
			return compiled;
		};
		await registerSubscriptions(db, [
			{
				taskKey: "pipeline.prefix",
				eventKey: "pipeline.operators",
				compiledFilter: compileFilter({ code: [{ prefix: "a%_\\" }] }),
			},
			{
				taskKey: "pipeline.range",
				eventKey: "pipeline.operators",
				compiledFilter: compileFilter({ amount: [{ numeric: [">=", 10, "<", 20] }] }),
			},
			{
				taskKey: "pipeline.missing",
				eventKey: "pipeline.operators",
				compiledFilter: compileFilter({ note: [{ exists: false }] }),
			},
			{
				taskKey: "pipeline.present",
				eventKey: "pipeline.operators",
				compiledFilter: compileFilter({ metadata: [{ exists: true }] }),
			},
			{
				taskKey: "pipeline.anything",
				eventKey: "pipeline.operators",
				compiledFilter: compileFilter({ status: [{ "anything-but": "blocked" }] }),
			},
			{
				taskKey: "pipeline.mixed-or",
				eventKey: "pipeline.operators",
				compiledFilter: compileFilter({ code: ["exact", { prefix: "a%_\\" }] }),
			},
		]);

		const eventId = await db.client.emitEvent({
			eventKey: "pipeline.operators",
			payload: { code: "a%_\\suffix", amount: 10, status: 42, metadata: { nested: true } },
		});
		const owner = await claimEvent(db, eventId);
		expect(
			await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId: owner }),
		).toEqual([eventId]);
		const destinations = await db.sql<{ task_key: string }[]>`
			select task_key from pgconductor._private_executions
			where parent_execution_id = ${eventId}::uuid and subscription_id is not null
			order by task_key
		`;
		expect(destinations.map(({ task_key }) => task_key)).toEqual([
			"pipeline.anything",
			"pipeline.missing",
			"pipeline.mixed-or",
			"pipeline.prefix",
			"pipeline.present",
			"pipeline.range",
		]);

		const negativeId = await db.client.emitEvent({
			eventKey: "pipeline.operators",
			payload: { code: "other", amount: 20, note: "present", status: "blocked" },
		});
		const negativeOwner = await claimEvent(db, negativeId);
		expect(
			await db.client.dispatchCustomEvents({
				eventIds: [negativeId],
				orchestratorId: negativeOwner,
			}),
		).toEqual([negativeId]);
		const [negative] = await db.sql<{ count: number }[]>`
			select count(*)::integer as count from pgconductor._private_executions
			where parent_execution_id = ${negativeId}::uuid and subscription_id is not null
		`;
		expect(negative?.count).toBe(0);
	});

	test("treats wrong scalar types as non-matches instead of cast errors", async () => {
		const db = await database();
		await registerSubscriptions(db, [
			{
				taskKey: "pipeline.residual",
				eventKey: "pipeline.residual",
				filter: { anchor: ["match"], count: [1], enabled: [true] },
			},
			{
				taskKey: "pipeline.anything-object",
				eventKey: "pipeline.residual",
				filter: {
					status: [{ $operator: "anything_but", value: "blocked" }],
				},
			},
		]);
		const eventId = await db.client.emitEvent({
			eventKey: "pipeline.residual",
			payload: {
				anchor: "match",
				count: "not-a-number",
				enabled: "not-a-boolean",
				status: { nested: true },
			},
		});
		const owner = await claimEvent(db, eventId);
		expect(
			await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId: owner }),
		).toEqual([eventId]);
		const [result] = await db.sql<{ count: number }[]>`
			select count(*)::integer as count from pgconductor._private_executions
			where parent_execution_id = ${eventId}::uuid and subscription_id is not null
		`;
		expect(result?.count).toBe(0);
	});

	test("commits destinations with a fan-out marker and freezes retries", async () => {
		const db = await database();
		await registerSubscriptions(db, [
			{ taskKey: "pipeline.first", eventKey: "pipeline.snapshot", filter: { kind: ["first"] } },
		]);
		const eventId = await db.client.emitEvent({
			eventKey: "pipeline.snapshot",
			payload: { kind: "first" },
		});
		const orchestratorId = await claimEvent(db, eventId);
		expect(await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId })).toEqual([
			eventId,
		]);

		await registerSubscriptions(db, [
			{ taskKey: "pipeline.second", eventKey: "pipeline.snapshot", filter: { kind: ["first"] } },
		]);
		expect(await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId })).toEqual([
			eventId,
		]);
		const destinations = await db.sql<
			{ task_key: string; parent_execution_id: string; subscription_id: string }[]
		>`
			select task_key, parent_execution_id, subscription_id
			from pgconductor._private_executions
			where parent_execution_id = ${eventId}::uuid and subscription_id is not null
		`;
		expect([...destinations]).toEqual([
			{
				task_key: "pipeline.first",
				parent_execution_id: eventId,
				subscription_id: expect.any(String),
			},
		]);
		const [marker] = await db.sql<{ count: number }[]>`
			select count(*)::integer as count
			from pgconductor._private_steps
			where execution_id = ${eventId}::uuid and key = ${FANOUT_STEP}
		`;
		expect(marker?.count).toBe(1);

		await settleSource(db, eventId, orchestratorId);
		const [settled] = await db.sql<
			{ source_exists: boolean; marker_exists: boolean; destination_exists: boolean }[]
		>`
			select
				exists(
					select 1 from pgconductor._private_executions
					where id = ${eventId}::uuid and queue = ${INTERNAL_QUEUE}
				) as source_exists,
				exists(
					select 1 from pgconductor._private_steps
					where execution_id = ${eventId}::uuid and key = ${FANOUT_STEP}
				) as marker_exists,
				exists(
					select 1 from pgconductor._private_executions
					where parent_execution_id = ${eventId}::uuid and subscription_id is not null
				) as destination_exists
		`;
		expect(settled).toEqual({
			source_exists: false,
			marker_exists: false,
			destination_exists: true,
		});
	});

	test("serializes concurrent dispatch and freezes the first committed subscription snapshot", async () => {
		const db = await database();
		await registerSubscriptions(db, [
			{ taskKey: "pipeline.concurrent-destination", eventKey: "pipeline.dispatch-race" },
		]);
		const eventId = await db.client.emitEvent({ eventKey: "pipeline.dispatch-race", payload: {} });
		const owner = await claimEvent(db, eventId);
		await db.sql`
			create function public.slow_event_destination() returns trigger language plpgsql as $$
			begin
				if new.subscription_id is not null then
					perform pg_sleep(0.2);
				end if;
				return new;
			end;
			$$
		`;
		await db.sql`
			create trigger slow_event_destination
			before insert on pgconductor._private_executions
			for each row execute function public.slow_event_destination()
		`;
		const firstSql = postgres(db.url, { max: 1 });
		const secondSql = postgres(db.url, { max: 1 });
		const firstClient = new DatabaseClient({ sql: firstSql, logger: new DefaultLogger() });
		const secondClient = new DatabaseClient({ sql: secondSql, logger: new DefaultLogger() });
		try {
			const first = firstClient.dispatchCustomEvents({
				eventIds: [eventId],
				orchestratorId: owner,
			});
			await new Promise((resolve) => setTimeout(resolve, 25));
			await registerSubscriptions(db, [
				{ taskKey: "pipeline.replacement-destination", eventKey: "pipeline.dispatch-race" },
			]);
			const second = secondClient.dispatchCustomEvents({
				eventIds: [eventId],
				orchestratorId: owner,
			});
			expect(await Promise.all([first, second])).toEqual([[eventId], [eventId]]);
		} finally {
			await Promise.all([firstSql.end(), secondSql.end()]);
			await db.sql`drop trigger slow_event_destination on pgconductor._private_executions`;
			await db.sql`drop function public.slow_event_destination()`;
		}
		const [counts] = await db.sql<
			{ destination_tasks: string[]; destinations: number; markers: number }[]
		>`
			select
				count(*) filter (
					where execution_id = ${eventId}::uuid and key = ${FANOUT_STEP}
				)::integer as markers,
				(
					select count(*)::integer from pgconductor._private_executions
					where parent_execution_id = ${eventId}::uuid and subscription_id is not null
				) as destinations,
				(
					select array_agg(task_key order by task_key) from pgconductor._private_executions
					where parent_execution_id = ${eventId}::uuid and subscription_id is not null
				) as destination_tasks
			from pgconductor._private_steps
		`;
		expect(counts).toEqual({
			destination_tasks: ["pipeline.concurrent-destination"],
			destinations: 1,
			markers: 1,
		});
	});

	test("marks zero-match fan-out complete", async () => {
		const db = await database();
		const eventId = await db.client.emitEvent({ eventKey: "pipeline.none", payload: {} });
		const orchestratorId = await claimEvent(db, eventId);
		expect(await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId })).toEqual([
			eventId,
		]);
		const [state] = await db.sql<{ destinations: number; markers: number }[]>`
			select
				count(*) filter (
					where execution_id = ${eventId}::uuid and key = ${FANOUT_STEP}
				)::integer as markers,
				(
					select count(*)::integer from pgconductor._private_executions
					where parent_execution_id = ${eventId}::uuid and subscription_id is not null
				) as destinations
			from pgconductor._private_steps
		`;
		expect(state).toEqual({ destinations: 0, markers: 1 });
	});

	test("rejects stale claims and rolls destination insertion back with the marker", async () => {
		const db = await database();
		await registerSubscriptions(db, [{ taskKey: "pipeline.atomic", eventKey: "pipeline.atomic" }]);
		const eventId = await db.client.emitEvent({ eventKey: "pipeline.atomic", payload: {} });
		const owner = await claimEvent(db, eventId);
		expect(
			await db.client.dispatchCustomEvents({
				eventIds: [eventId],
				orchestratorId: crypto.randomUUID(),
			}),
		).toEqual([]);

		await db.sql`
			create function public.fail_event_destination() returns trigger language plpgsql as $$
			begin
				if new.subscription_id is not null then
					raise exception 'event destination insert failed';
				end if;
				return new;
			end;
			$$
		`;
		await db.sql`
			create trigger fail_event_destination
			before insert on pgconductor._private_executions
			for each row execute function public.fail_event_destination()
		`;
		await expect(
			db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId: owner }),
		).rejects.toThrow("event destination insert failed");
		const [rolledBack] = await db.sql<{ destinations: number; markers: number }[]>`
			select
				count(*) filter (
					where execution_id = ${eventId}::uuid and key = ${FANOUT_STEP}
				)::integer as markers,
				(
					select count(*)::integer from pgconductor._private_executions
					where parent_execution_id = ${eventId}::uuid and subscription_id is not null
				) as destinations
			from pgconductor._private_steps
		`;
		expect(rolledBack).toEqual({ destinations: 0, markers: 0 });
		await db.sql`drop trigger fail_event_destination on pgconductor._private_executions`;
		await db.sql`drop function public.fail_event_destination()`;

		await db.sql`
			create function public.fail_event_marker() returns trigger language plpgsql as $$
			begin
				if new.key = 'pgconductor.internal.event-fanout.v1' then
					raise exception 'event marker insert failed';
				end if;
				return new;
			end;
			$$
		`;
		await db.sql`
			create trigger fail_event_marker
			before insert on pgconductor._private_steps
			for each row execute function public.fail_event_marker()
		`;
		await expect(
			db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId: owner }),
		).rejects.toThrow("event marker insert failed");
		const [markerRollback] = await db.sql<{ destinations: number; markers: number }[]>`
			select
				count(*) filter (
					where execution_id = ${eventId}::uuid and key = ${FANOUT_STEP}
				)::integer as markers,
				(
					select count(*)::integer from pgconductor._private_executions
					where parent_execution_id = ${eventId}::uuid and subscription_id is not null
				) as destinations
			from pgconductor._private_steps
		`;
		expect(markerRollback).toEqual({ destinations: 0, markers: 0 });
		await db.sql`drop trigger fail_event_marker on pgconductor._private_steps`;
		await db.sql`drop function public.fail_event_marker()`;

		expect(
			await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId: owner }),
		).toEqual([eventId]);
	});

	test("settles event destinations independently from dispatch sources", async () => {
		const db = await database();
		await registerSubscriptions(db, [
			{ taskKey: "pipeline.independent", eventKey: "pipeline.independent", maxAttempts: 1 },
		]);
		const eventId = await db.client.emitEvent({ eventKey: "pipeline.independent", payload: {} });
		const sourceOwner = await claimEvent(db, eventId);
		await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId: sourceOwner });
		const destinationOwner = crypto.randomUUID();
		const [failedDestination] = await db.client.getExecutions({
			orchestratorId: destinationOwner,
			queueName: "default",
			batchSize: 1,
			filterTaskKeys: [],
		});
		if (!failedDestination) throw new Error("destination was not claimed");
		await db.client.returnExecutions({
			count: 1,
			orchestratorId: destinationOwner,
			completed: [],
			failed: [
				{
					execution_id: failedDestination.id,
					orchestrator_id: destinationOwner,
					queue: failedDestination.queue,
					task_key: failedDestination.task_key,
					status: "failed",
					error: "destination failed",
				},
			],
			released: [],
			invokeChild: [],
			taskKeys: new Set([failedDestination.task_key]),
		});
		const [source] = await db.sql<{ failed_at: Date | null; locked_by: string | null }[]>`
			select failed_at, locked_by
			from pgconductor._private_executions
			where id = ${eventId}::uuid and queue = ${INTERNAL_QUEUE}
		`;
		expect(source).toEqual({ failed_at: null, locked_by: sourceOwner });

		const completedEventId = await db.client.emitEvent({
			eventKey: "pipeline.independent",
			payload: {},
		});
		const secondSourceOwner = await claimEvent(db, completedEventId, crypto.randomUUID());
		await db.client.dispatchCustomEvents({
			eventIds: [completedEventId],
			orchestratorId: secondSourceOwner,
		});
		const completionOwner = crypto.randomUUID();
		const [completedDestination] = await db.client.getExecutions({
			orchestratorId: completionOwner,
			queueName: "default",
			batchSize: 1,
			filterTaskKeys: [],
		});
		if (!completedDestination) throw new Error("second destination was not claimed");
		await db.client.returnExecutions({
			count: 1,
			orchestratorId: completionOwner,
			completed: [
				{
					execution_id: completedDestination.id,
					orchestrator_id: completionOwner,
					queue: completedDestination.queue,
					task_key: completedDestination.task_key,
					status: "completed",
				},
			],
			failed: [],
			released: [],
			invokeChild: [],
			taskKeys: new Set([completedDestination.task_key]),
		});
		const [destinationState] = await db.sql<
			{ completed_at: Date | null; failed_at: Date | null; subscription_id: string | null }[]
		>`
			select completed_at, failed_at, subscription_id
			from pgconductor._private_executions
			where id = ${completedDestination.id}::uuid and queue = 'default'
		`;
		expect(destinationState?.completed_at).not.toBeNull();
		expect(destinationState?.failed_at).toBeNull();
		expect(destinationState?.subscription_id).not.toBeNull();
	});

	test("serializes queue-scoped subscription replacement", async () => {
		const db = await database();
		const blockerSql = postgres(db.url, { max: 1 });
		const registrationSql = postgres(db.url, { max: 1 });
		const client = new DatabaseClient({ sql: registrationSql, logger: new DefaultLogger() });
		let registration: Promise<void> | undefined;
		try {
			await blockerSql.begin(async (transaction) => {
				await transaction`
					select 1 from pgconductor._private_queues
					where name = 'default' for update
				`;
				registration = client.registerWorker({
					queueName: "default",
					taskSpecs: [{ key: "pipeline.serialized", queue: "default", maxAttempts: 3 }],
					cronSchedules: [],
					eventSubscriptions: [
						{
							task_key: "pipeline.serialized",
							event_key: "pipeline.serialized",
							payload_fields: null,
							required_field_count: 0,
							terms: [],
						},
					],
				});
				await waitForCondition(async () => {
					const [waiting] = await db.sql<{ exists: boolean }[]>`
						select exists(
							select 1 from pg_stat_activity
							where datname = current_database()
								and cardinality(pg_blocking_pids(pid)) > 0
						) as exists
					`;
					return waiting?.exists === true;
				});
			});
			if (!registration) throw new Error("registration did not start");
			await registration;
		} finally {
			await Promise.all([blockerSql.end(), registrationSql.end()]);
		}

		const [count] = await db.sql<{ count: number }[]>`
			select count(*)::integer as count
			from pgconductor._private_custom_event_subscriptions
			where queue = 'default' and event_key = 'pipeline.serialized'
		`;
		expect(count?.count).toBe(1);
	}, 30_000);

	test("coordinates claims and recursively drains event fan-out", async () => {
		const db = await database();
		await registerSubscriptions(db, [{ taskKey: "pipeline.once", eventKey: "pipeline.once" }]);
		const eventId = await db.client.emitEvent({ eventKey: "pipeline.once", payload: {} });
		const sql = postgres(db.url, { max: 2 });
		const clients = [
			new DatabaseClient({ sql, logger: new DefaultLogger() }),
			new DatabaseClient({ sql, logger: new DefaultLogger() }),
		];
		try {
			const owners = [crypto.randomUUID(), crypto.randomUUID()];
			const claims = await Promise.all(
				clients.map((client, index) =>
					client.getExecutions({
						orchestratorId: owners[index] || "",
						queueName: INTERNAL_QUEUE,
						batchSize: 1,
						filterTaskKeys: [],
					}),
				),
			);
			expect(claims.flat()).toHaveLength(1);
			const winner = claims[0]?.length ? 0 : 1;
			expect(
				await clients[winner]?.dispatchCustomEvents({
					eventIds: [eventId],
					orchestratorId: owners[winner] || "",
				}),
			).toEqual([eventId]);
		} finally {
			await sql.end();
		}

		const event = defineEvent({
			name: "pipeline.drain-fanout",
			payload: z.object({ value: z.string() }),
		});
		const sourceDefinition = defineTask({ name: "pipeline.source", payload: z.object({}) });
		const destinationDefinition = defineTask({
			name: "pipeline.destination",
			queue: "destination",
			payload: z.object({}),
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([sourceDefinition, destinationDefinition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const received: string[] = [];
		const source = conductor.createTask(
			{ name: "pipeline.source" },
			{ invocable: true },
			async () => {
				await conductor.emit("pipeline.drain-fanout", { value: "done" });
			},
		);
		const destination = conductor.createTask(
			{ name: "pipeline.destination", queue: "destination" },
			{ event: "pipeline.drain-fanout" },
			async (receivedEvent) => {
				received.push(receivedEvent.payload.value);
			},
		);
		await conductor.invoke({ name: "pipeline.source" }, {});
		await Orchestrator.create({
			conductor,
			tasks: [source],
			workers: [conductor.createWorker({ queue: "destination", tasks: [destination] })],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		}).drain();
		expect(received).toEqual(["done"]);
	});

	test("rejects malformed filters and the reserved internal queue", async () => {
		const db = await database();
		const event = defineEvent({
			name: "pipeline.validation",
			payload: z.object({ status: z.string(), metadata: z.object({ source: z.string() }) }),
			filterable: ["status"],
		});
		const definition = defineTask({ name: "pipeline.validation-task", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		expect(() =>
			conductor.createTask(
				{ name: "pipeline.validation-task" },
				{ event: "pipeline.validation", filter: { status: [{ source: "api" }] } } as never,
				async () => {},
			),
		).toThrow(/unsupported operator/);
		expect(() => conductor.createWorker({ queue: INTERNAL_QUEUE, tasks: [] as never })).toThrow(
			/reserved for internal use/,
		);
	});
});
