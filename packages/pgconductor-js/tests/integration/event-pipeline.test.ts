import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import postgres from "postgres";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import {
	DatabaseClient,
	type EventSubscriptionSpec,
	type JsonValue,
} from "../../src/database-client";
import { defineEvent } from "../../src/event-definition";
import { DefaultLogger } from "../../src/lib/logger";
import { Orchestrator } from "../../src/orchestrator";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const INTERNAL_QUEUE = "pgconductor.internal";
const DISPATCH_TASK = "pgconductor.event-dispatch";

type CustomSubscription = {
	taskKey: string;
	eventKey: string;
	filter?: Record<string, JsonValue[]>;
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
		const eventSubscriptions: EventSubscriptionSpec[] = subscriptions.map((subscription) => ({
			task_key: subscription.taskKey,
			event_key: subscription.eventKey,
			payload_fields: subscription.payloadFields || null,
			filter: subscription.filter || null,
		}));
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

	test("uses delivery identity rather than payload shape for direct invocations", async () => {
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
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [single, batch],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.drain();

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

	test("emits a durable dispatch execution whose id is the event id", async () => {
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
			payload: {},
		});
		const [event] = await db.sql<
			{
				id: string;
				event_key: string;
				payload: Record<string, unknown>;
				dispatched_at: Date | null;
			}[]
		>`
			select id, event_key, payload, dispatched_at
			from pgconductor._private_custom_events
			where id = ${eventId}::uuid
		`;
		expect(event).toEqual({
			id: eventId,
			event_key: "pipeline.persisted",
			payload: { value: "ready" },
			dispatched_at: null,
		});
	});

	test("does not serialize same-key emitters for the lifetime of caller transactions", async () => {
		const db = await database();
		const firstSql = postgres(db.url, { max: 1 });
		const secondSql = postgres(db.url, { max: 1 });
		let releaseFirst!: () => void;
		let markFirstInserted!: () => void;
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
			const secondEmission = secondSql`
				select pgconductor.emit_event('pipeline.concurrent', '{}'::jsonb) as id
			`;
			const [second] = await Promise.race([
				secondEmission,
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

	test("retains the event log independently after its dispatch execution settles", async () => {
		const db = await database();
		const eventId = await db.client.emitEvent({ eventKey: "pipeline.retained", payload: {} });
		const orchestratorId = await claimEvent(db, eventId);
		expect(await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId })).toEqual([
			eventId,
		]);
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
		const [state] = await db.sql<{ source_exists: boolean; event_dispatched: boolean }[]>`
			select
				exists(select 1 from pgconductor._private_executions where id = ${eventId}::uuid and queue = ${INTERNAL_QUEUE}) as source_exists,
				exists(select 1 from pgconductor._private_custom_events where id = ${eventId}::uuid and dispatched_at is not null) as event_dispatched
		`;
		expect(state).toEqual({ source_exists: false, event_dispatched: true });
	});

	test("retention does not race active retries and removes terminal undispatched events", async () => {
		const db = await database();
		const oldTime = new Date(Date.now() - 10 * 24 * 60 * 60 * 1000);
		await db.client.setFakeTime({ date: oldTime });
		const dispatchedId = await db.client.emitEvent({ eventKey: "pipeline.cleanup", payload: {} });
		const owner = await claimEvent(db, dispatchedId);
		await db.client.dispatchCustomEvents({ eventIds: [dispatchedId], orchestratorId: owner });
		const undispatchedId = await db.client.emitEvent({
			eventKey: "pipeline.cleanup",
			payload: {},
		});
		await db.sql`
			update pgconductor._private_executions
			set failed_at = pgconductor._private_current_time(), locked_by = null, locked_at = null
			where id = ${undispatchedId}::uuid and queue = ${INTERNAL_QUEUE}
		`;
		await db.client.clearFakeTime();
		const before = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

		await db.client.removeCustomEvents(before, 10);
		let events = await db.sql<{ id: string }[]>`
			select id from pgconductor._private_custom_events
			where id in (${dispatchedId}::uuid, ${undispatchedId}::uuid)
			order by id
		`;
		expect(events.map((event) => event.id)).toEqual([dispatchedId]);

		await db.sql`
			update pgconductor._private_executions
			set failed_at = pgconductor._private_current_time(), locked_by = null, locked_at = null
			where id = ${dispatchedId}::uuid and queue = ${INTERNAL_QUEUE}
		`;
		await db.client.removeCustomEvents(before, 10);
		events = await db.sql<{ id: string }[]>`
			select id from pgconductor._private_custom_events
			where id in (${dispatchedId}::uuid, ${undispatchedId}::uuid)
		`;
		expect(events).toHaveLength(0);
	});

	test("matches typed scalar filters, distinguishes missing from null, and selects payload fields", async () => {
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
		// A persisted event can predate worker registration. The hidden dispatcher
		// must not claim it until all user subscriptions are registered.
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

	test("writes destinations and dispatch state atomically and reuses a frozen snapshot", async () => {
		const db = await database();
		await registerSubscriptions(db, [
			{ taskKey: "pipeline.first", eventKey: "pipeline.snapshot", filter: { kind: ["first"] } },
		]);
		const eventId = await db.client.emitEvent({
			eventKey: "pipeline.snapshot",
			payload: { kind: "first" },
		});
		const orchestratorId = await claimEvent(db, eventId);
		const first = await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId });
		expect(first).toEqual([eventId]);

		await registerSubscriptions(db, [
			{ taskKey: "pipeline.second", eventKey: "pipeline.snapshot", filter: { kind: ["first"] } },
		]);
		const second = await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId });
		expect(second).toEqual([eventId]);
		const destinations = await db.sql<{ task_key: string }[]>`
			select task_key
			from pgconductor._private_executions
			where source_event_id = ${eventId}::uuid
		`;
		expect([...destinations]).toEqual([{ task_key: "pipeline.first" }]);

		const noMatchId = await db.client.emitEvent({
			eventKey: "pipeline.none",
			payload: {},
		});
		const noMatchOwner = await claimEvent(db, noMatchId, crypto.randomUUID());
		expect(
			await db.client.dispatchCustomEvents({ eventIds: [noMatchId], orchestratorId: noMatchOwner }),
		).toEqual([noMatchId]);
		const [noMatch] = await db.sql<{ dispatched_at: Date | null }[]>`
			select dispatched_at
			from pgconductor._private_custom_events
			where id = ${noMatchId}::uuid
		`;
		expect(noMatch?.dispatched_at).not.toBeNull();
	});

	test("rejects stale claims and rolls destination insertion back with dispatch state", async () => {
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
				if new.source_event_id is not null then
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
		const [rolledBack] = await db.sql<{ destinations: string; dispatched_at: Date | null }[]>`
			select
				(select count(*)::text from pgconductor._private_executions where source_event_id = ${eventId}::uuid) as destinations,
				(select dispatched_at from pgconductor._private_custom_events where id = ${eventId}::uuid) as dispatched_at
		`;
		expect(rolledBack).toEqual({ destinations: "0", dispatched_at: null });
		await db.sql`drop trigger fail_event_destination on pgconductor._private_executions`;
		await db.sql`drop function public.fail_event_destination()`;
		expect(
			await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId: owner }),
		).toEqual([eventId]);
	});

	test("event destinations settle independently from their source", async () => {
		const db = await database();
		await registerSubscriptions(db, [
			{ taskKey: "pipeline.failure", eventKey: "pipeline.failure", maxAttempts: 1 },
		]);
		const eventId = await db.client.emitEvent({ eventKey: "pipeline.failure", payload: {} });
		const sourceOwner = await claimEvent(db, eventId);
		await db.client.dispatchCustomEvents({ eventIds: [eventId], orchestratorId: sourceOwner });
		const destinationOwner = crypto.randomUUID();
		const [destination] = await db.client.getExecutions({
			orchestratorId: destinationOwner,
			queueName: "default",
			batchSize: 1,
			filterTaskKeys: [],
		});
		expect(destination).toBeDefined();
		if (!destination) throw new Error("destination was not claimed");
		await db.client.returnExecutions({
			count: 1,
			orchestratorId: destinationOwner,
			completed: [],
			failed: [
				{
					execution_id: destination.id,
					orchestrator_id: destinationOwner,
					queue: destination.queue,
					task_key: destination.task_key,
					status: "failed",
					error: "destination failed",
				},
			],
			released: [],
			invokeChild: [],
			taskKeys: new Set([destination.task_key]),
		});
		const [source] = await db.sql<{ failed_at: Date | null; locked_by: string | null }[]>`
			select failed_at, locked_by
			from pgconductor._private_executions
			where id = ${eventId}::uuid and queue = ${INTERNAL_QUEUE}
		`;
		expect(source?.failed_at).toBeNull();
		expect(source?.locked_by).toBe(sourceOwner);
		const [failedDestination] = await db.sql<
			{ failed_at: Date | null; last_error: string | null }[]
		>`
			select failed_at, last_error
			from pgconductor._private_executions
			where id = ${destination.id}::uuid and queue = 'default'
		`;
		expect(failedDestination?.failed_at).not.toBeNull();
		expect(failedDestination?.last_error).toBe("destination failed");
	});

	test("serializes registrations that replace subscriptions for the same queue", async () => {
		const db = await database();
		const blockerSql = postgres(db.url, { max: 1 });
		const registrationSql = postgres(db.url, { max: 1 });
		const client = new DatabaseClient({ sql: registrationSql, logger: new DefaultLogger() });
		let registration: Promise<void> | undefined;
		try {
			await blockerSql.begin(async (transaction) => {
				await transaction`
					select 1
					from pgconductor._private_queues
					where name = 'default'
					for update
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
							filter: null,
						},
					],
				});
				await waitForCondition(async () => {
					const [waiting] = await db.sql<{ exists: boolean }[]>`
						select exists(
							select 1
							from pg_stat_activity
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

	test("coordinates concurrent dispatch claims and drains recursively emitted fanout", async () => {
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
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [source],
			workers: [conductor.createWorker({ queue: "destination", tasks: [destination] })],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.drain();
		expect(received).toEqual(["done"]);
	});

	test("runs global event maintenance for named-queue-only orchestrators", async () => {
		const db = await database();
		await db.client.setFakeTime({ date: new Date(Date.now() - 10 * 24 * 60 * 60 * 1000) });
		const oldEventId = await db.client.emitEvent({ eventKey: "pipeline.old", payload: {} });
		await db.sql`
			update pgconductor._private_executions
			set failed_at = pgconductor._private_current_time()
			where id = ${oldEventId}::uuid and queue = ${INTERNAL_QUEUE}
		`;
		await db.client.clearFakeTime();
		const definition = defineTask({
			name: "pipeline.named-only",
			queue: "pipeline.named",
			payload: z.object({}),
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const task = conductor.createTask(
			{ name: "pipeline.named-only", queue: "pipeline.named" },
			{ invocable: true },
			async () => {},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			workers: [conductor.createWorker({ queue: "pipeline.named", tasks: [task] })],
		});
		await orchestrator.start();
		try {
			const [maintenance] = await db.sql<{ task: boolean; schedule: boolean }[]>`
				select
					exists(
						select 1 from pgconductor._private_tasks
						where queue = ${INTERNAL_QUEUE} and key = 'pgconductor.maintenance'
					) as task,
					exists(
						select 1 from pgconductor._private_executions
						where queue = ${INTERNAL_QUEUE}
							and task_key = 'pgconductor.maintenance'
							and cron_expression is not null
					) as schedule
			`;
			expect(maintenance).toEqual({ task: true, schedule: true });
			await db.client.invoke({
				task_key: "pgconductor.maintenance",
				queue: INTERNAL_QUEUE,
				payload: {},
			});
			await waitForCondition(async () => {
				const [event] = await db.sql<{ exists: boolean }[]>`
					select exists(
						select 1 from pgconductor._private_custom_events where id = ${oldEventId}::uuid
					) as exists
				`;
				return event?.exists === false;
			});
		} finally {
			await orchestrator.stop();
		}
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
				{ event: "pipeline.validation", filter: { status: [{ source: "api" }] } } as any,
				async () => {},
			),
		).toThrow(/scalar values/);
		expect(() =>
			conductor.createWorker({
				queue: INTERNAL_QUEUE,
				tasks: [] as never,
			}),
		).toThrow(/reserved for internal use/);
	});
});
