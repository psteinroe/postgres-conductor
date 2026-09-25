import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import postgres from "postgres";
import { Conductor } from "../../src/conductor";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";

describe("execution foundations", () => {
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
		const database = await pool.child();
		databases.push(database);
		const conductor = Conductor.create({ sql: database.sql, context: {} });
		await conductor.ensureInstalled();
		return database;
	}

	function grouped(
		result: Parameters<TestDatabase["client"]["returnExecutions"]>[0]["completed"][number],
	) {
		return {
			count: 1,
			orchestratorId: result.orchestrator_id,
			completed: [result],
			failed: [],
			released: [],
			invokeChild: [],
			taskKeys: new Set([result.task_key]),
		};
	}

	test("registers and executes the same task key independently in two queues", async () => {
		const db = await database();

		await db.client.registerWorker({
			queueName: "queue-a",
			taskSpecs: [
				{
					key: "same-task",
					queue: "queue-a",
					maxAttempts: 2,
					removeOnCompleteDays: 0,
				},
			],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		await db.client.registerWorker({
			queueName: "queue-b",
			taskSpecs: [
				{
					key: "same-task",
					queue: "queue-b",
					maxAttempts: 7,
					removeOnCompleteDays: 1,
				},
			],
			cronSchedules: [],
			eventSubscriptions: [],
		});

		const tasks = await db.sql<
			{ queue: string; key: string; max_attempts: number; remove_on_complete_days: number | null }[]
		>`
			select queue, key, max_attempts, remove_on_complete_days
			from pgconductor._private_tasks
			where key = 'same-task'
			order by queue
		`;
		expect([...tasks]).toEqual([
			{ queue: "queue-a", key: "same-task", max_attempts: 2, remove_on_complete_days: 0 },
			{ queue: "queue-b", key: "same-task", max_attempts: 7, remove_on_complete_days: 1 },
		]);

		const firstId = await db.client.invoke({ task_key: "same-task", queue: "queue-a" });
		const secondId = await db.client.invoke({ task_key: "same-task", queue: "queue-b" });
		expect(firstId).not.toBeNull();
		expect(secondId).not.toBeNull();

		const first = (
			await db.client.getExecutions({
				orchestratorId: crypto.randomUUID(),
				queueName: "queue-a",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		const second = (
			await db.client.getExecutions({
				orchestratorId: crypto.randomUUID(),
				queueName: "queue-b",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		expect(first?.queue).toBe("queue-a");
		expect(second?.queue).toBe("queue-b");
		if (!first || !second) throw new Error("expected both executions to be claimed");

		await db.client.returnExecutions(
			grouped({
				execution_id: first.id,
				queue: first.queue,
				task_key: first.task_key,
				orchestrator_id: first.locked_by,
				status: "completed",
			}),
		);
		await db.client.returnExecutions(
			grouped({
				execution_id: second.id,
				queue: second.queue,
				task_key: second.task_key,
				orchestrator_id: second.locked_by,
				status: "completed",
			}),
		);

		const remaining = await db.sql<{ queue: string; completed_at: Date | null }[]>`
			select queue, completed_at
			from pgconductor._private_executions
			where task_key = 'same-task'
			order by queue
		`;
		expect([...remaining]).toHaveLength(1);
		expect(remaining[0]?.queue).toBe("queue-b");
		expect(remaining[0]?.completed_at).not.toBeNull();
	});

	test("retention cleanup scopes duplicate execution IDs to the requested queue", async () => {
		const db = await database();

		for (const queue of ["queue-a", "queue-b"]) {
			await db.client.registerWorker({
				queueName: queue,
				taskSpecs: [{ key: "retained-task", queue, removeOnCompleteDays: 1 }],
				cronSchedules: [],
				eventSubscriptions: [],
			});
		}

		const executionId = await db.client.invoke({ task_key: "retained-task", queue: "queue-a" });
		const duplicateId = await db.client.invoke({ task_key: "retained-task", queue: "queue-b" });
		if (!executionId || !duplicateId) throw new Error("expected both executions");

		await db.sql`
			update pgconductor._private_executions
			set id = ${executionId}::uuid,
				completed_at = pgconductor._private_current_time() - interval '2 days'
			where id = ${duplicateId}::uuid and queue = 'queue-b'
		`;
		await db.sql`
			update pgconductor._private_executions
			set completed_at = pgconductor._private_current_time() - interval '2 days'
			where id = ${executionId}::uuid and queue = 'queue-a'
		`;

		await db.client.removeExecutions({ queueName: "queue-a", batchSize: 1 });

		const remaining = await db.sql<{ queue: string }[]>`
			select queue
			from pgconductor._private_executions
			where id = ${executionId}::uuid
			order by queue
		`;
		expect([...remaining]).toEqual([{ queue: "queue-b" }]);
	});

	test("retention cleanup removes the oldest terminal execution first", async () => {
		const db = await database();

		await db.client.registerWorker({
			queueName: "retention-order",
			taskSpecs: [
				{
					key: "retained-task",
					queue: "retention-order",
					removeOnCompleteDays: 1,
					removeOnFailDays: 1,
				},
			],
			cronSchedules: [],
			eventSubscriptions: [],
		});

		const newerId = await db.client.invoke({
			task_key: "retained-task",
			queue: "retention-order",
		});
		const olderId = await db.client.invoke({
			task_key: "retained-task",
			queue: "retention-order",
		});
		if (!newerId || !olderId) throw new Error("expected both executions");

		await db.sql`
			update pgconductor._private_executions
			set failed_at = pgconductor._private_current_time() - interval '2 days'
			where id = ${newerId}::uuid and queue = 'retention-order'
		`;
		await db.sql`
			update pgconductor._private_executions
			set completed_at = pgconductor._private_current_time() - interval '3 days'
			where id = ${olderId}::uuid and queue = 'retention-order'
		`;

		await db.client.removeExecutions({ queueName: "retention-order", batchSize: 1 });

		const remaining = await db.sql<{ id: string }[]>`
			select id
			from pgconductor._private_executions
			where queue = 'retention-order'
		`;
		expect([...remaining]).toEqual([{ id: newerId }]);
	});

	test("retention cleanup skips locked executions without ending the cleanup loop", async () => {
		const db = await database();

		await db.client.registerWorker({
			queueName: "retention-lock",
			taskSpecs: [{ key: "retained-task", queue: "retention-lock", removeOnCompleteDays: 1 }],
			cronSchedules: [],
			eventSubscriptions: [],
		});

		const oldestId = await db.client.invoke({
			task_key: "retained-task",
			queue: "retention-lock",
		});
		const nextId = await db.client.invoke({
			task_key: "retained-task",
			queue: "retention-lock",
		});
		if (!oldestId || !nextId) throw new Error("expected both executions");

		await db.sql`
			update pgconductor._private_executions
			set completed_at = case id
				when ${oldestId}::uuid then pgconductor._private_current_time() - interval '3 days'
				else pgconductor._private_current_time() - interval '2 days'
			end
			where id = any(${[oldestId, nextId]}::uuid[]) and queue = 'retention-lock'
		`;

		const blockerSql = postgres(db.url, { max: 1 });
		let releaseLock = () => {};
		let markLocked = () => {};
		const lockRelease = new Promise<void>((resolve) => {
			releaseLock = resolve;
		});
		const locked = new Promise<void>((resolve) => {
			markLocked = resolve;
		});
		const blocker = blockerSql.begin(async (transaction) => {
			await transaction`
				select id
				from pgconductor._private_executions
				where id = ${oldestId}::uuid and queue = 'retention-lock'
				for update
			`;
			markLocked();
			await lockRelease;
		});

		try {
			await locked;
			const hasMore = await db.client.removeExecutions({
				queueName: "retention-lock",
				batchSize: 1,
			});
			expect(hasMore).toBe(true);

			const remaining = await db.sql<{ id: string }[]>`
				select id
				from pgconductor._private_executions
				where queue = 'retention-lock'
			`;
			expect([...remaining]).toEqual([{ id: oldestId }]);
		} finally {
			releaseLock();
			await blocker;
			await blockerSql.end();
		}
	});

	test("retains a parent when its permanently failed child is configured for removal", async () => {
		const db = await database();
		await db.client.registerWorker({
			queueName: "parent-retention",
			taskSpecs: [
				{ key: "parent", queue: "parent-retention", removeOnFailDays: 1 },
				{ key: "child", queue: "parent-retention", maxAttempts: 1, removeOnFailDays: 0 },
			],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const parentId = await db.client.invoke({ task_key: "parent", queue: "parent-retention" });
		if (!parentId) throw new Error("expected parent execution");
		const parent = (
			await db.client.getExecutions({
				orchestratorId: crypto.randomUUID(),
				queueName: "parent-retention",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		if (!parent) throw new Error("expected parent claim");

		await db.client.returnExecutions({
			count: 1,
			orchestratorId: parent.locked_by,
			completed: [],
			failed: [],
			released: [],
			invokeChild: [
				{
					execution_id: parent.id,
					queue: parent.queue,
					orchestrator_id: parent.locked_by,
					task_key: parent.task_key,
					status: "invoke_child",
					timeout_ms: 5000,
					step_key: "child-step",
					child_task_name: "child",
					child_task_queue: "parent-retention",
					child_payload: null,
				},
			],
			taskKeys: new Set([parent.task_key]),
		});

		const childId = (
			await db.sql<{ id: string }[]>`
				select id from pgconductor._private_executions
				where parent_execution_id = ${parentId}::uuid
			`
		)[0]?.id;
		if (!childId) throw new Error("expected child execution");
		const child = (
			await db.client.getExecutions({
				orchestratorId: crypto.randomUUID(),
				queueName: "parent-retention",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		if (!child) throw new Error("expected child claim");

		await db.client.returnExecutions({
			count: 1,
			orchestratorId: child.locked_by,
			completed: [],
			failed: [
				{
					execution_id: child.id,
					queue: child.queue,
					orchestrator_id: child.locked_by,
					task_key: child.task_key,
					status: "permanently_failed",
					error: "child failed",
				},
			],
			released: [],
			invokeChild: [],
			taskKeys: new Set([child.task_key]),
		});

		const retained = await db.sql<
			{ id: string; failed_at: Date | null; last_error: string | null }[]
		>`
			select id, failed_at, last_error from pgconductor._private_executions
			where id = ${parentId}::uuid
		`;
		expect(retained[0]?.id).toBe(parentId);
		expect(retained[0]?.failed_at).not.toBeNull();
		expect(retained[0]?.last_error).toContain("Child execution failed");
		expect(
			await db.sql`select 1 from pgconductor._private_executions where id = ${childId}::uuid`,
		).toHaveLength(0);
	});

	test("propagates a permanently failed child across queues to its parent", async () => {
		const db = await database();
		await db.client.registerWorker({
			queueName: "parent-queue",
			taskSpecs: [{ key: "parent", queue: "parent-queue", removeOnFailDays: 1 }],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		await db.client.registerWorker({
			queueName: "child-queue",
			taskSpecs: [{ key: "child", queue: "child-queue", maxAttempts: 1, removeOnFailDays: 1 }],
			cronSchedules: [],
			eventSubscriptions: [],
		});

		const parentId = await db.client.invoke({ task_key: "parent", queue: "parent-queue" });
		if (!parentId) throw new Error("expected parent execution");
		const parent = (
			await db.client.getExecutions({
				orchestratorId: crypto.randomUUID(),
				queueName: "parent-queue",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		if (!parent) throw new Error("expected parent claim");

		await db.client.returnExecutions({
			count: 1,
			orchestratorId: parent.locked_by,
			completed: [],
			failed: [],
			released: [],
			invokeChild: [
				{
					execution_id: parent.id,
					queue: parent.queue,
					orchestrator_id: parent.locked_by,
					task_key: parent.task_key,
					status: "invoke_child",
					timeout_ms: "infinity",
					step_key: "child-step",
					child_task_name: "child",
					child_task_queue: "child-queue",
					child_payload: null,
				},
			],
			taskKeys: new Set([parent.task_key]),
		});

		const child = (
			await db.client.getExecutions({
				orchestratorId: crypto.randomUUID(),
				queueName: "child-queue",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		if (!child) throw new Error("expected child claim");
		await db.client.returnExecutions({
			count: 1,
			orchestratorId: child.locked_by,
			completed: [],
			failed: [
				{
					execution_id: child.id,
					queue: child.queue,
					orchestrator_id: child.locked_by,
					task_key: child.task_key,
					status: "permanently_failed",
					error: "child failed",
				},
			],
			released: [],
			invokeChild: [],
			taskKeys: new Set([child.task_key]),
		});

		const outcome = await db.sql<{ failed_at: Date | null; last_error: string | null }[]>`
			select failed_at, last_error
			from pgconductor._private_executions
			where id = ${parentId}::uuid
		`;
		expect(outcome[0]?.failed_at).not.toBeNull();
		expect(outcome[0]?.last_error).toBe("Child execution failed: child failed");
	});

	test("cancellation fences a buffered completion and does not retry it", async () => {
		const db = await database();
		await db.client.registerWorker({
			queueName: "cancel-buffered",
			taskSpecs: [{ key: "cancelled", queue: "cancel-buffered", maxAttempts: 5 }],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const orchestratorId = crypto.randomUUID();
		await db.client.orchestratorHeartbeat({ orchestratorId, version: "test", migrationNumber: 1 });
		const executionId = await db.client.invoke({ task_key: "cancelled", queue: "cancel-buffered" });
		if (!executionId) throw new Error("expected execution");
		const claimed = (
			await db.client.getExecutions({
				orchestratorId,
				queueName: "cancel-buffered",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		if (!claimed) throw new Error("expected claim");
		await db.client.cancelExecution(executionId, { reason: "cancelled before flush" });
		await db.client.returnExecutions(
			grouped({
				execution_id: claimed.id,
				queue: claimed.queue,
				task_key: claimed.task_key,
				orchestrator_id: claimed.locked_by,
				status: "completed",
			}),
		);

		const outcome = await db.sql<
			{
				failed_at: Date | null;
				completed_at: Date | null;
				locked_by: string | null;
				attempts: number;
				last_error: string | null;
			}[]
		>`
			select failed_at, completed_at, locked_by, attempts, last_error
			from pgconductor._private_executions
			where id = ${executionId}::uuid
		`;
		expect(outcome[0]?.failed_at).not.toBeNull();
		expect(outcome[0]?.completed_at).toBeNull();
		expect(outcome[0]?.locked_by).toBeNull();
		expect(outcome[0]?.attempts).toBe(1);
		expect(outcome[0]?.last_error).toBe("cancelled before flush");
	});

	test("cancelling a waiting parent permanently fails its pending child", async () => {
		const db = await database();
		await db.client.registerWorker({
			queueName: "cascade-parent",
			taskSpecs: [{ key: "parent", queue: "cascade-parent" }],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		await db.client.registerWorker({
			queueName: "cascade-child",
			taskSpecs: [{ key: "child", queue: "cascade-child" }],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const parentId = await db.client.invoke({ task_key: "parent", queue: "cascade-parent" });
		if (!parentId) throw new Error("expected parent execution");
		const parent = (
			await db.client.getExecutions({
				orchestratorId: crypto.randomUUID(),
				queueName: "cascade-parent",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		if (!parent) throw new Error("expected parent claim");
		await db.client.returnExecutions({
			count: 1,
			orchestratorId: parent.locked_by,
			completed: [],
			failed: [],
			released: [],
			invokeChild: [
				{
					execution_id: parent.id,
					queue: parent.queue,
					orchestrator_id: parent.locked_by,
					task_key: parent.task_key,
					status: "invoke_child",
					timeout_ms: "infinity",
					step_key: "child-step",
					child_task_name: "child",
					child_task_queue: "cascade-child",
					child_payload: null,
				},
			],
			taskKeys: new Set([parent.task_key]),
		});
		const childId = (
			await db.sql<{ id: string }[]>`
			select id from pgconductor._private_executions where parent_execution_id = ${parentId}::uuid
		`
		)[0]?.id;
		if (!childId) throw new Error("expected child execution");
		await db.client.cancelExecution(parentId);
		const outcome = await db.sql<
			{
				id: string;
				failed_at: Date | null;
				waiting_on_execution_id: string | null;
			}[]
		>`
			select id, failed_at, waiting_on_execution_id
			from pgconductor._private_executions
			where id in (${parentId}::uuid, ${childId}::uuid)
			order by id
		`;
		expect(outcome).toHaveLength(2);
		expect(outcome.every((execution) => execution.failed_at !== null)).toBe(true);
		expect(
			outcome.find((execution) => execution.id === parentId)?.waiting_on_execution_id,
		).toBeNull();
	});

	test("orders equal-priority executions by created_at and id", async () => {
		const db = await database();
		await db.client.registerWorker({
			queueName: "enqueue-order",
			taskSpecs: [{ key: "ordered", queue: "enqueue-order" }],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const ids = await db.client.invokeBatch([
			{ task_key: "ordered", queue: "enqueue-order", priority: 0 },
			{ task_key: "ordered", queue: "enqueue-order", priority: 0 },
			{ task_key: "ordered", queue: "enqueue-order", priority: 0 },
		]);
		const expected = await db.sql<{ id: string }[]>`
			select id
			from pgconductor._private_executions
			where id = any(${db.sql.array(ids)}::uuid[])
			order by priority, run_at, created_at, id
		`;
		const claimed = await db.client.getExecutions({
			orchestratorId: crypto.randomUUID(),
			queueName: "enqueue-order",
			batchSize: 3,
			filterTaskKeys: [],
		});
		expect(claimed.map((execution) => execution.id)).toEqual(
			expected.map((execution) => execution.id),
		);
	});

	test("fences stale completion, failure, and release results after recovery and re-claim", async () => {
		const db = await database();
		await db.client.registerWorker({
			queueName: "fenced",
			taskSpecs: [{ key: "fenced-task", queue: "fenced", maxAttempts: 3 }],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const executionId = await db.client.invoke({ task_key: "fenced-task", queue: "fenced" });
		if (!executionId) throw new Error("expected execution id");

		const oldOrchestrator = crypto.randomUUID();
		await db.client.orchestratorHeartbeat({
			orchestratorId: oldOrchestrator,
			version: "test",
			migrationNumber: 1,
		});
		const oldClaim = (
			await db.client.getExecutions({
				orchestratorId: oldOrchestrator,
				queueName: "fenced",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		if (!oldClaim) throw new Error("expected old claim");

		await db.sql`
			update pgconductor._private_orchestrators
			set last_heartbeat_at = now() - interval '1 hour'
			where id = ${oldOrchestrator}::uuid
		`;
		await db.client.recoverStaleOrchestrators({ maxAge: "1 second" });

		const newOrchestrator = crypto.randomUUID();
		const currentClaim = (
			await db.client.getExecutions({
				orchestratorId: newOrchestrator,
				queueName: "fenced",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		if (!currentClaim) throw new Error("expected recovered execution to be re-claimed");

		const staleBase = {
			execution_id: executionId,
			queue: "fenced",
			task_key: "fenced-task",
			orchestrator_id: oldClaim.locked_by,
		};
		await db.client.returnExecutions({
			count: 3,
			orchestratorId: oldOrchestrator,
			completed: [{ ...staleBase, status: "completed" }],
			failed: [{ ...staleBase, status: "failed", error: "stale" }],
			released: [{ ...staleBase, status: "released", reschedule_in_ms: 0 }],
			invokeChild: [],
			taskKeys: new Set(["fenced-task"]),
		});

		const untouched = await db.sql<
			{
				completed_at: Date | null;
				failed_at: Date | null;
				locked_by: string;
			}[]
		>`
			select completed_at, failed_at, locked_by
			from pgconductor._private_executions
			where id = ${executionId}::uuid
		`;
		expect(untouched[0]?.completed_at).toBeNull();
		expect(untouched[0]?.failed_at).toBeNull();
		expect(untouched[0]?.locked_by).toBe(newOrchestrator);

		await db.client.returnExecutions(
			grouped({
				execution_id: currentClaim.id,
				queue: currentClaim.queue,
				task_key: currentClaim.task_key,
				orchestrator_id: currentClaim.locked_by,
				status: "completed",
			}),
		);
		const settled = await db.sql<{ completed_at: Date | null; locked_by: string | null }[]>`
			select completed_at, locked_by
			from pgconductor._private_executions
			where id = ${executionId}::uuid
		`;
		expect(settled[0]?.completed_at).not.toBeNull();
		expect(settled[0]?.locked_by).toBeNull();
	});
});
