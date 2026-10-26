import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import postgres from "postgres";
import { Conductor } from "../../src/conductor";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

// Steps, signals and wait subscriptions have no foreign key into executions; every path that
// deletes executions deletes them
describe("execution data", () => {
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
		await db.client.registerWorker({
			queueName: "q",
			taskSpecs: [
				{ key: "kept", queue: "q" },
				{
					key: "removed",
					queue: "q",
					maxAttempts: 1,
					removeOnCompleteDays: 0,
					removeOnFailDays: 0,
				},
				{ key: "retained", queue: "q", removeOnCompleteDays: 1 },
			],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		return db;
	}

	async function invoke(db: TestDatabase, taskKey: string, dedupeKey?: string): Promise<string> {
		const id = await db.client.invoke({ task_key: taskKey, queue: "q", dedupe_key: dedupeKey });
		if (!id) throw new Error("expected execution");
		return id;
	}

	async function claim(db: TestDatabase, orchestratorId: string, taskKey: string) {
		const [execution] = (
			await db.client.getExecutions({
				orchestratorId,
				queueName: "q",
				batchSize: 1,
				taskKeys: [taskKey],
			})
		).executions;
		if (!execution) throw new Error("expected claim");
		return execution;
	}

	async function store(db: TestDatabase, id: string) {
		const [{ queue }] = await db.sql<[{ queue: string }]>`
			select queue from pgconductor._private_executions where id = ${id}::uuid
		`;
		await db.sql`
			insert into pgconductor._private_steps (execution_id, queue, key, result)
			values (${id}::uuid, ${queue}, 'step', '1')
		`;
		await db.sql`
			insert into pgconductor._private_execution_signals (execution_id, queue, id, name)
			values (${id}::uuid, ${queue}, 's1', 'ping')
		`;
		await db.sql`
			insert into pgconductor._private_custom_event_subscriptions
				(event_key, task_key, queue, required_field_count, kind, execution_id, step_key)
			values ('never', 'kept', ${queue}, 0, 'execution_wait', ${id}::uuid, 'wait')
		`;
	}

	async function stored(db: TestDatabase, id: string): Promise<number[]> {
		const [row] = await db.sql<[{ steps: number; signals: number; waits: number }]>`
			select
				(select count(*)::int from pgconductor._private_steps where execution_id = ${id}::uuid) as steps,
				(select count(*)::int from pgconductor._private_execution_signals where execution_id = ${id}::uuid) as signals,
				(select count(*)::int from pgconductor._private_custom_event_subscriptions where execution_id = ${id}::uuid) as waits
		`;
		return [row.steps, row.signals, row.waits];
	}

	async function exists(db: TestDatabase, id: string): Promise<boolean> {
		const rows = await db.sql`select 1 from pgconductor._private_executions where id = ${id}::uuid`;
		return rows.length > 0;
	}

	// another execution keeps its rows
	async function bystander(db: TestDatabase): Promise<string> {
		const id = await invoke(db, "retained");
		await store(db, id);
		return id;
	}

	test("retention", async () => {
		const db = await database();
		const other = await bystander(db);
		const id = await invoke(db, "retained");
		await store(db, id);
		await db.sql`
			update pgconductor._private_executions
			set completed_at = now() - interval '2 days'
			where id = ${id}::uuid
		`;

		await db.client.removeExecutions({ queueName: "q", batchSize: 10 });

		expect(await exists(db, id)).toBe(false);
		expect(await stored(db, id)).toEqual([0, 0, 0]);
		expect(await stored(db, other)).toEqual([1, 1, 1]);
	});

	test.each(["completed", "permanently_failed"] as const)(
		"settlement that removes a %s execution",
		async (status) => {
			const db = await database();
			const other = await bystander(db);
			const orchestratorId = await db.registerOrchestrator();
			const id = await invoke(db, "removed");
			const execution = await claim(db, orchestratorId, "removed");
			await store(db, id);

			await db.client.returnExecutions({
				orchestratorId,
				queueName: "q",
				results: [
					{
						execution_id: id,
						task_key: "removed",
						claim_token: execution.claim_token,
						status,
						error: "boom",
					},
				],
			});

			expect(await exists(db, id)).toBe(false);
			expect(await stored(db, id)).toEqual([0, 0, 0]);
			expect(await stored(db, other)).toEqual([1, 1, 1]);
		},
	);

	test("settlement removes rows stored while it waited for the execution", async () => {
		const db = await database();
		const orchestratorId = await db.registerOrchestrator();
		const id = await invoke(db, "removed");
		const execution = await claim(db, orchestratorId, "removed");

		// like dispatch storing an event for a running wait, after the settlement's snapshot
		const sql = postgres(db.url, { max: 2 });
		const locked = new Deferred<void>();
		const commit = new Deferred<void>();
		const tx = sql.begin(async (tx) => {
			await tx`select 1 from pgconductor._private_executions where id = ${id}::uuid for update`;
			await tx`
				insert into pgconductor._private_steps (execution_id, queue, key, result)
				values (${id}::uuid, 'q', 'event', '1')
			`;
			locked.resolve();
			await commit.promise;
		});
		await locked.promise;

		const settled = db.client.returnExecutions({
			orchestratorId,
			queueName: "q",
			results: [
				{
					execution_id: id,
					task_key: "removed",
					claim_token: execution.claim_token,
					status: "completed",
				},
			],
		});
		await waitForCondition(async () => {
			const [row] = await sql<[{ waiting: boolean }]>`
				select exists (
					select 1 from pg_stat_activity
					where wait_event_type = 'Lock' and query like '%result_data as%'
				) as waiting
			`;
			return row.waiting;
		});
		commit.resolve();
		await tx;
		await settled;
		await sql.end();

		expect(await exists(db, id)).toBe(false);
		expect(await stored(db, id)).toEqual([0, 0, 0]);
	});

	test("cancelling a pending execution that is removed on failure", async () => {
		const db = await database();
		const other = await bystander(db);
		const id = await invoke(db, "removed");
		await store(db, id);

		await db.client.cancelExecution(id);

		expect(await exists(db, id)).toBe(false);
		expect(await stored(db, id)).toEqual([0, 0, 0]);
		expect(await stored(db, other)).toEqual([1, 1, 1]);
	});

	test("superseding a running execution that is removed on failure", async () => {
		const db = await database();
		const other = await bystander(db);
		const orchestratorId = await db.registerOrchestrator();
		const id = await invoke(db, "removed", "key");
		await claim(db, orchestratorId, "removed");
		await store(db, id);

		await invoke(db, "removed", "key");

		expect(await exists(db, id)).toBe(false);
		expect(await stored(db, id)).toEqual([0, 0, 0]);
		expect(await stored(db, other)).toEqual([1, 1, 1]);
	});

	test("a timed out wait of a superseded execution stores no step", async () => {
		const db = await database();
		const orchestratorId = await db.registerOrchestrator();
		const id = await invoke(db, "removed", "key");
		const execution = await claim(db, orchestratorId, "removed");
		await db.sql`
			update pgconductor._private_executions
			set waiting_on_execution_id = gen_random_uuid(), waiting_step_key = 'child'
			where id = ${id}::uuid
		`;

		const sql = postgres(db.url, { max: 2 });
		const superseded = new Deferred<void>();
		const commit = new Deferred<void>();
		const tx = sql.begin(async (tx) => {
			await tx`select pgconductor.invoke(p_task_key := 'removed', p_queue := 'q', p_dedupe_key := 'key')`;
			superseded.resolve();
			await commit.promise;
		});
		await superseded.promise;

		const cleared = db.client.clearWaitingState({
			executionId: id,
			queue: "q",
			orchestratorId,
			claimToken: execution.claim_token,
		});
		await waitForCondition(async () => {
			const [row] = await sql<[{ waiting: boolean }]>`
				select exists (
					select 1 from pg_stat_activity
					where wait_event_type = 'Lock' and query like '%timed_out as%'
				) as waiting
			`;
			return row.waiting;
		});
		commit.resolve();
		await tx;
		expect(await cleared).toBe(false);
		await sql.end();

		expect(await exists(db, id)).toBe(false);
		expect(await stored(db, id)).toEqual([0, 0, 0]);
	});

	test("failing a waiting parent that is removed on failure", async () => {
		const db = await database();
		const other = await bystander(db);
		const orchestratorId = await db.registerOrchestrator();
		const id = await invoke(db, "removed");
		const parent = await claim(db, orchestratorId, "removed");
		await db.client.returnExecutions({
			orchestratorId,
			queueName: "q",
			results: [
				{
					execution_id: id,
					task_key: "removed",
					claim_token: parent.claim_token,
					status: "invoke_child",
					step_key: "child",
					timeout_ms: "infinity",
					child_task_name: "kept",
					child_task_queue: "q",
					child_payload: {},
				},
			],
		});
		await store(db, id);
		const child = await claim(db, orchestratorId, "kept");

		await db.client.returnExecutions({
			orchestratorId,
			queueName: "q",
			results: [
				{
					execution_id: child.id,
					task_key: "kept",
					claim_token: child.claim_token,
					status: "permanently_failed",
					error: "boom",
				},
			],
		});

		expect(await exists(db, id)).toBe(false);
		expect(await stored(db, id)).toEqual([0, 0, 0]);
		expect(await stored(db, other)).toEqual([1, 1, 1]);
	});

	test("replacing and removing a dynamic schedule", async () => {
		const db = await database();
		const other = await bystander(db);
		const occurrence = async () => {
			const [row] = await db.sql<[{ id: string }]>`
				select id from pgconductor._private_executions where cron_expression is not null
			`;
			return row.id;
		};
		const schedule = (runAt: Date) =>
			db.client.scheduleCronExecution({
				spec: { task_key: "kept", queue: "q", run_at: runAt, cron_expression: "0 * * * *" },
				scheduleName: "hourly",
			});

		await schedule(new Date(Date.now() + 3_600_000));
		const first = await occurrence();
		await store(db, first);
		await schedule(new Date(Date.now() + 7_200_000));
		const second = await occurrence();
		await store(db, second);
		await db.client.unscheduleCronExecution({
			taskKey: "kept",
			queue: "q",
			scheduleName: "hourly",
		});

		expect(second).not.toBe(first);
		expect(await stored(db, first)).toEqual([0, 0, 0]);
		expect(await stored(db, second)).toEqual([0, 0, 0]);
		expect(await stored(db, other)).toEqual([1, 1, 1]);
	});

	test("registration that removes a static schedule", async () => {
		const db = await database();
		const other = await bystander(db);
		const runAt = new Date(Date.now() + 3_600_000);
		const register = (
			cronSchedules: Parameters<typeof db.client.registerWorker>[0]["cronSchedules"],
		) =>
			db.client.registerWorker({
				queueName: "q",
				taskSpecs: [{ key: "kept", queue: "q" }],
				cronSchedules,
				eventSubscriptions: [],
			});
		await register([
			{
				task_key: "kept",
				queue: "q",
				run_at: runAt,
				cron_expression: "0 * * * *",
				dedupe_key: `scheduled::hourly::${Math.floor(runAt.getTime() / 1000)}`,
			},
		]);
		const [{ id }] = await db.sql<[{ id: string }]>`
			select id from pgconductor._private_executions where cron_expression is not null
		`;
		await store(db, id);

		await register([]);

		expect(await exists(db, id)).toBe(false);
		expect(await stored(db, id)).toEqual([0, 0, 0]);
		expect(await stored(db, other)).toEqual([1, 1, 1]);
	});

	test("dropping a queue", async () => {
		const db = await database();
		const other = await bystander(db);
		await db.client.registerWorker({
			queueName: "doomed",
			taskSpecs: [{ key: "kept", queue: "doomed" }],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const id = await db.client.invoke({ task_key: "kept", queue: "doomed" });
		if (!id) throw new Error("expected execution");
		await store(db, id);

		await db.sql`select pgconductor.drop_queue('doomed')`;

		expect(await stored(db, id)).toEqual([0, 0, 0]);
		expect(await stored(db, other)).toEqual([1, 1, 1]);
	});
});
