import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { DatabaseClient } from "../../src/database-client";
import { DefaultLogger } from "../../src/lib/logger";
import { Orchestrator } from "../../src/orchestrator";
import { defineEvent } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";
import postgres from "postgres";

describe("event pipeline", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

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

	async function subscription(
		db: TestDatabase,
		taskKey: string,
		eventKey: string,
		filter?: Record<string, string[]>,
	): Promise<void> {
		await db.sql`
			insert into pgconductor._private_tasks (key, queue)
			values (${taskKey}, 'default')
			on conflict (queue, key) do nothing
		`;
		await db.sql`
			insert into pgconductor._private_event_subscriptions
				(task_key, queue, event_key, filter)
			values (${taskKey}, 'default', ${eventKey}, ${filter ? db.sql.json(filter) : null})
		`;
	}

	test("matches one and multiple allowed values across all filter fields", async () => {
		const db = await database();
		const event = defineEvent({
			name: "pipeline.order",
			payload: z.object({ status: z.enum(["paid", "trial", "cancelled"]), region: z.string() }),
			filterable: ["status", "region"],
		});
		const taskDefinition = defineTask({ name: "pipeline-order-task", payload: z.object({}) });
		const received: string[] = [];
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const task = conductor.createTask(
			{ name: "pipeline-order-task" },
			{ event: "pipeline.order", filter: { status: ["paid", "trial"], region: ["us"] } },
			async (receivedEvent) => {
				received.push(receivedEvent.payload.status);
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		try {
			await conductor.emit("pipeline.order", { status: "paid", region: "us" });
			await conductor.emit("pipeline.order", { status: "trial", region: "us" });
			await conductor.emit("pipeline.order", { status: "paid", region: "eu" });
			await conductor.emit("pipeline.order", { status: "cancelled", region: "us" });
			await waitForCondition(async () => received.length === 2);
			expect(received.sort()).toEqual(["paid", "trial"]);
		} finally {
			await orchestrator.stop();
		}
	});

	test("rejects undeclared filter fields at runtime", async () => {
		const db = await database();
		const event = defineEvent({
			name: "pipeline.runtime-filter",
			payload: z.object({ status: z.string(), secret: z.string() }),
			filterable: ["status"],
		});
		const taskDefinition = defineTask({ name: "pipeline-runtime-task", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		expect(() =>
			conductor.createTask(
				{ name: "pipeline-runtime-task" },
				{ event: "pipeline.runtime-filter", filter: { secret: ["nope"] } } as never,
				async () => {},
			),
		).toThrow(/undeclared field/);
	});

	test("fans one event out to multiple subscriptions and ignores nonmatches", async () => {
		const db = await database();
		await subscription(db, "pipeline.email", "pipeline.fanout", { kind: ["email"] });
		await subscription(db, "pipeline.audit", "pipeline.fanout", { kind: ["audit"] });
		await db.client.emitEvent({ eventKey: "pipeline.fanout", payload: { kind: "email" } });
		await db.client.emitEvent({ eventKey: "pipeline.fanout", payload: { kind: "other" } });
		expect(await db.client.processEvents({ batchSize: 10 })).toBe(2);
		const rows = await db.sql<{ task_key: string; count: string }[]>`
			select task_key, count(*)::text as count
			from pgconductor._private_executions group by task_key order by task_key
		`;
		expect([...rows]).toEqual([{ task_key: "pipeline.email", count: "1" }]);
	});

	test("processes multiple event keys in bounded batches", async () => {
		const db = await database();
		await subscription(db, "pipeline.one", "pipeline.one");
		await subscription(db, "pipeline.two", "pipeline.two");
		await db.client.emitEvent({ eventKey: "pipeline.one", payload: { n: 1 } });
		await db.client.emitEvent({ eventKey: "pipeline.two", payload: { n: 2 } });
		await db.client.emitEvent({ eventKey: "pipeline.one", payload: { n: 3 } });
		expect(await db.client.processEvents({ batchSize: 2 })).toBe(2);
		const [pending] = await db.sql<{ count: string }[]>`
			select count(*)::text as count from pgconductor._private_custom_events where processed_at is null
		`;
		expect(pending?.count).toBe("1");
		expect(await db.client.processEvents({ batchSize: 2 })).toBe(1);
		const [executions] = await db.sql<{ count: string }[]>`
			select count(*)::text as count from pgconductor._private_executions
			where event_id is not null
		`;
		expect(executions?.count).toBe("3");
	});

	test("is idempotent under repeated and concurrent processing", async () => {
		const db = await database();
		await subscription(db, "pipeline.once", "pipeline.once");
		await db.client.emitEvent({ eventKey: "pipeline.once", payload: {} });
		const sql = postgres(db.url, { max: 2 });
		const clients = [
			new DatabaseClient({ sql, logger: new DefaultLogger() }),
			new DatabaseClient({ sql, logger: new DefaultLogger() }),
		];
		try {
			const counts = await Promise.all(
				clients.map((client) => client.processEvents({ batchSize: 1 })),
			);
			expect(counts.reduce((sum, count) => sum + count, 0)).toBe(1);
			expect(await db.client.processEvents({ batchSize: 1 })).toBe(0);
			const [deliveries] = await db.sql<{ count: string }[]>`
				select count(*)::text as count from pgconductor._private_event_deliveries
			`;
			expect(deliveries?.count).toBe("1");
		} finally {
			await sql.end();
		}
	});

	test("drains a fanout emitted by one queue into two queues", async () => {
		const db = await database();
		const event = defineEvent({
			name: "pipeline.drain-fanout",
			payload: z.object({ value: z.string() }),
		});
		const sourceDefinition = defineTask({ name: "pipeline.source", payload: z.object({}) });
		const leftDefinition = defineTask({
			name: "pipeline.left",
			queue: "left",
			payload: z.object({}),
		});
		const rightDefinition = defineTask({
			name: "pipeline.right",
			queue: "right",
			payload: z.object({}),
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([sourceDefinition, leftDefinition, rightDefinition]),
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
		const left = conductor.createTask(
			{ name: "pipeline.left", queue: "left" },
			{ event: "pipeline.drain-fanout" },
			async (receivedEvent) => {
				received.push(`left:${receivedEvent.payload.value}`);
			},
		);
		const right = conductor.createTask(
			{ name: "pipeline.right", queue: "right" },
			{ event: "pipeline.drain-fanout" },
			async (receivedEvent) => {
				received.push(`right:${receivedEvent.payload.value}`);
			},
		);
		await conductor.invoke({ name: "pipeline.source" }, {});

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [source],
			workers: [
				conductor.createWorker({ queue: "left", tasks: [left] }),
				conductor.createWorker({ queue: "right", tasks: [right] }),
			],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.drain();
		expect(received.sort()).toEqual(["left:done", "right:done"]);
	});

	test("rejects duplicate workers for one queue", async () => {
		const db = await database();
		const definition = defineTask({ name: "pipeline.duplicate" });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const task = conductor.createTask(
			{ name: "pipeline.duplicate" },
			{ invocable: true },
			async () => {},
		);
		const worker = conductor.createWorker({ queue: "default", tasks: [task] });
		expect(() => Orchestrator.create({ conductor, tasks: [task], workers: [worker] })).toThrow(
			/multiple workers for queue/,
		);
	});

	test("rolls back a failed processing transaction and retries", async () => {
		const db = await database();
		await db.client.emitEvent({ eventKey: "pipeline.retry", payload: {} });
		await db.sql`
			create function public.fail_event_execution() returns trigger language plpgsql as $$
			begin raise exception 'event execution insert failed'; end; $$
		`;
		await db.sql`
			create trigger fail_event_execution after insert on pgconductor._private_executions
			execute function public.fail_event_execution()
		`;
		await expect(db.client.processEvents({ batchSize: 1 })).rejects.toThrow(
			"event execution insert failed",
		);
		const [rolledBack] = await db.sql<{ processed_at: Date | null }[]>`
			select processed_at from pgconductor._private_custom_events
		`;
		expect(rolledBack?.processed_at).toBeNull();
		await db.sql`drop trigger fail_event_execution on pgconductor._private_executions`;
		await db.sql`drop function public.fail_event_execution()`;
		expect(await db.client.processEvents({ batchSize: 1 })).toBe(1);
	});
});
