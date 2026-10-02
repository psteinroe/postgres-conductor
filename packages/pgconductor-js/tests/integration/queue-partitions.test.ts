import { afterAll, afterEach, beforeAll, describe, expect, mock, test } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineEvent } from "../../src/event-definition";
import { Orchestrator } from "../../src/orchestrator";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

describe("queue partitions", () => {
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

	async function executeOnEachQueue(db: TestDatabase, queues: string[]) {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(queues.map((queue) => defineTask({ name: "work", queue }))),
			context: {},
		});
		const executed: string[] = [];
		const orchestrator = Orchestrator.create({
			conductor,
			workers: queues.map((queue) =>
				conductor.createWorker({
					queue,
					tasks: [
						conductor.createTask({ name: "work", queue }, { invocable: true }, async () => {
							executed.push(queue);
						}),
					],
					config: { pollIntervalMs: 10, flushIntervalMs: 10 },
				}),
			),
		});
		await orchestrator.start();
		for (const queue of queues) {
			await conductor.invoke({ name: "work", queue }, {});
		}
		await waitForCondition(() => executed.length === queues.length);
		await orchestrator.stop();
		expect(executed.toSorted()).toEqual(queues.toSorted());
	}

	test("queues that differ only in '-' and '_' get separate partitions", async () => {
		const db = await pool.child();
		databases.push(db);

		await executeOnEachQueue(db, ["a-b", "a_b"]);
	}, 30_000);

	test("long queue names with a shared prefix get separate partitions", async () => {
		const db = await pool.child();
		databases.push(db);
		const prefix = "a".repeat(70);

		await executeOnEachQueue(db, [`${prefix}-first`, `${prefix}-second`]);
	}, 30_000);

	test("drop_queue drops only the partition of the given queue", async () => {
		const db = await pool.child();
		databases.push(db);

		await executeOnEachQueue(db, ["a-b", "a_b"]);
		await db.sql`
			insert into pgconductor._private_steps (key, execution_id, queue)
			select 'step', id, queue from pgconductor._private_executions
		`;
		await db.sql`select pgconductor.drop_queue('a-b')`;

		const rows = await db.sql<{ queue: string }[]>`
			select distinct queue from pgconductor._private_executions
		`;
		expect(rows.map((row) => row.queue)).toEqual(["a_b"]);
	}, 30_000);

	test("drop_queue removes its subscriptions so other subscribers still receive events", async () => {
		const db = await pool.child();
		databases.push(db);
		const event = defineEvent({ name: "thing.happened", payload: z.object({}) });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "on-thing", queue: "doomed", payload: z.object({}) }),
				defineTask({ name: "on-thing", payload: z.object({}) }),
			]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const handler = mock(async () => {});
		const config = { pollIntervalMs: 10, flushIntervalMs: 10 };
		const orchestrator = Orchestrator.create({
			conductor,
			workers: [
				conductor.createWorker({
					queue: "doomed",
					tasks: [
						conductor.createTask(
							{ name: "on-thing", queue: "doomed" },
							{ event: "thing.happened" },
							async () => {},
						),
					],
					config,
				}),
				conductor.createWorker({
					queue: "default",
					tasks: [conductor.createTask({ name: "on-thing" }, { event: "thing.happened" }, handler)],
					config,
				}),
			],
		});
		await orchestrator.start();
		await db.sql`select pgconductor.drop_queue('doomed')`;

		await conductor.emit("thing.happened", {});
		await waitForCondition(() => handler.mock.calls.length === 1);
		await orchestrator.stop();

		const rows = await db.sql<{ source: string }[]>`
			select 'task' as source from pgconductor._private_tasks where queue = 'doomed'
			union all
			select 'subscription' from pgconductor._private_custom_event_subscriptions where queue = 'doomed'
		`;
		expect(rows.map((row) => row.source)).toEqual([]);
	}, 30_000);

	test("drop_queue clears dead-letter destinations in the dropped queue", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "charge" }),
				defineTask({ name: "failed-charge", queue: "doomed" }),
			]),
			context: {},
		});
		const destination = conductor.createTask(
			{ name: "failed-charge", queue: "doomed" },
			{ invocable: true },
			async () => {},
		);
		const source = conductor.createTask(
			{ name: "charge", maxAttempts: 1, deadLetter: { queue: "doomed", task: destination } },
			{ invocable: true },
			async () => {
				throw new Error("card declined");
			},
		);
		const config = { pollIntervalMs: 10, flushIntervalMs: 10 };
		const orchestrator = Orchestrator.create({
			conductor,
			workers: [
				conductor.createWorker({ queue: "doomed", tasks: [destination], config }),
				conductor.createWorker({ queue: "default", tasks: [source], config }),
			],
		});
		await orchestrator.start();
		await db.sql`select pgconductor.drop_queue('doomed')`;

		await conductor.invoke({ name: "charge" }, {});
		await waitForCondition(async () => {
			const rows = await db.sql`
				select 1 from pgconductor._private_executions
				where task_key = 'charge' and failed_at is not null
			`;
			return rows.length === 1;
		});
		await orchestrator.stop();
	}, 30_000);
});
