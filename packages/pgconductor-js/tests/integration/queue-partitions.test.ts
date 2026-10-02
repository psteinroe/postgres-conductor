import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
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
});
