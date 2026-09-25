import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

describe("dead-letter queues (Postgres integration)", () => {
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

	test("retries, then delivers the final failure with payload and metadata", async () => {
		const db = await pool.child();
		databases.push(db);
		const payloadSchema = z.object({ value: z.string() });
		const sourceDefinition = defineTask({ name: "charge", payload: payloadSchema });
		const destinationDefinition = defineTask({
			name: "alternate-failure",
			queue: "dlq",
			payload: payloadSchema,
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([sourceDefinition, destinationDefinition]),
			context: {},
		});
		const seen: Array<{
			value: string;
			sourceTask: string;
			attempts: number;
			error: string | null;
		}> = [];
		const destination = conductor.createTask(
			{ name: "alternate-failure", queue: "dlq" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name === "pgconductor.invoke") {
					if (!ctx.deadLetter) throw new Error("missing dead-letter metadata");
					seen.push({
						value: event.payload.value,
						sourceTask: ctx.deadLetter.sourceTaskKey,
						attempts: ctx.deadLetter.attempts,
						error: ctx.deadLetter.error,
					});
				}
			},
		);
		const source = conductor.createTask(
			{
				name: "charge",
				maxAttempts: 2,
				removeOnFail: true,
				deadLetter: { queue: "dlq", task: destination },
			},
			{ invocable: true },
			async () => {
				throw new Error("card declined");
			},
		);
		const sourceOrchestrator = Orchestrator.create({
			conductor,
			tasks: [source],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await sourceOrchestrator.start();
		await conductor.invoke({ name: "charge" }, { value: "order-42" });
		await waitForCondition(async () => {
			const rows = await db.sql<{ attempts: number; released: boolean }[]>`
				select attempts, locked_by is null as released
				from pgconductor._private_executions
				where task_key = 'charge'
			`;
			return rows[0]?.attempts === 1 && rows[0].released;
		});
		const [retry] = await db.sql<{ run_at: Date }[]>`
			select run_at from pgconductor._private_executions
			where task_key = 'charge'
		`;
		if (!retry) throw new Error("expected persisted retry");
		await db.client.setFakeTime({ date: new Date(retry.run_at.getTime() + 1) });
		await waitForCondition(async () => {
			const rows = await db.sql<{ count: string }[]>`
				select count(*)::text as count from pgconductor._private_executions
				where queue = 'dlq'
				and dead_letter_source_task_key = 'charge'
			`;
			return rows[0]?.count === "1";
		});
		const sourceRows = await db.sql<{ id: string }[]>`
			select id from pgconductor._private_executions where task_key = 'charge'
		`;
		expect(sourceRows).toHaveLength(0);
		await sourceOrchestrator.stop();

		// The source registration created the destination partition, but the destination
		// worker was intentionally started later.
		const destinationOrchestrator = Orchestrator.create({
			conductor,
			workers: [
				conductor.createWorker({
					queue: "dlq",
					tasks: [destination],
					config: { pollIntervalMs: 10, flushIntervalMs: 10 },
				}),
			],
		});
		await destinationOrchestrator.start();
		await waitForCondition(async () => seen.length === 1);
		await destinationOrchestrator.stop();
		expect(seen).toEqual([
			{ value: "order-42", sourceTask: "charge", attempts: 2, error: "card declined" },
		]);
	}, 60_000);

	test("ignores duplicate settlements and wrong worker identity", async () => {
		const db = await pool.child();
		databases.push(db);
		const payloadSchema = z.object({ value: z.string() });
		const sourceDefinition = defineTask({ name: "settle-source", payload: payloadSchema });
		const destinationDefinition = defineTask({
			name: "settle-destination",
			queue: "dlq",
			payload: payloadSchema,
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([sourceDefinition, destinationDefinition]),
			context: {},
		});
		await conductor.ensureInstalled();
		await db.client.registerWorker({
			queueName: "default",
			taskSpecs: [
				{
					key: "settle-source",
					queue: "default",
					maxAttempts: 1,
					deadLetterQueue: "dlq",
					deadLetterTaskKey: "settle-destination",
				},
			],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const executionId = await db.client.invoke({
			task_key: "settle-source",
			queue: "default",
			payload: { value: "x" },
		});
		const orchestratorId = crypto.randomUUID();
		const claimed = await db.client.getExecutions({
			orchestratorId,
			queueName: "default",
			batchSize: 1,
			filterTaskKeys: [],
		});
		expect(claimed).toHaveLength(1);
		const execution = claimed[0]!;
		const result = {
			execution_id: execution.id,
			queue: execution.queue,
			task_key: execution.task_key,
			status: "failed" as const,
			orchestrator_id: execution.locked_by,
			error: "settlement failure",
		};
		await db.client.returnExecutions({
			count: 1,
			orchestratorId,
			completed: [],
			failed: [{ ...result }],
			released: [],
			invokeChild: [],
			taskKeys: new Set([execution.task_key]),
		});
		await db.client.returnExecutions({
			count: 1,
			orchestratorId,
			completed: [],
			failed: [{ ...result }],
			released: [],
			invokeChild: [],
			taskKeys: new Set([execution.task_key]),
		});
		const rows = await db.sql<{ count: string; source: string | null }[]>`
			select count(*)::text as count, min(dead_letter_source_execution_id::text) as source
			from pgconductor._private_executions where queue = 'dlq'
		`;
		expect(rows[0]).toEqual({ count: "1", source: executionId });

		// A result from a different worker is fenced out as well.
		await db.client.returnExecutions({
			count: 1,
			orchestratorId: crypto.randomUUID(),
			completed: [],
			failed: [{ ...result, orchestrator_id: crypto.randomUUID() }],
			released: [],
			invokeChild: [],
			taskKeys: new Set([execution.task_key]),
		});
		const count = await db.sql<{ count: string }[]>`
			select count(*)::text as count from pgconductor._private_executions where queue = 'dlq'
		`;
		expect(count[0]?.count).toBe("1");
	}, 15000);

	test("cancellation never delivers to the DLQ", async () => {
		const db = await pool.child();
		databases.push(db);
		const sourceDefinition = defineTask({ name: "cancel-source", payload: z.object({}) });
		const destinationDefinition = defineTask({
			name: "cancel-destination",
			queue: "dlq",
			payload: z.object({}),
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([sourceDefinition, destinationDefinition]),
			context: {},
		});
		await conductor.ensureInstalled();
		await db.client.registerWorker({
			queueName: "default",
			taskSpecs: [
				{
					key: "cancel-source",
					queue: "default",
					maxAttempts: 1,
					deadLetterQueue: "dlq",
					deadLetterTaskKey: "cancel-destination",
				},
			],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const id = (await db.client.invoke({
			task_key: "cancel-source",
			queue: "default",
			payload: {},
		}))!;
		expect(await db.client.cancelExecution(id)).toBe(true);
		const rows = await db.sql<{ failed_at: Date | null; cancelled: boolean }[]>`
			select failed_at, cancelled from pgconductor._private_executions where id = ${id}::uuid
		`;
		expect(rows[0]?.failed_at).not.toBeNull();
		expect(rows[0]?.cancelled).toBe(false);
		const destinationRows = await db.sql<{ count: string }[]>`
			select count(*)::text as count from pgconductor._private_executions where queue = 'dlq'
		`;
		expect(destinationRows[0]?.count).toBe("0");
	}, 15000);

	test("removes a claimed cancellation without delivering to the DLQ", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({ sql: db.sql, context: {} });
		await conductor.ensureInstalled();
		await db.client.registerWorker({
			queueName: "default",
			taskSpecs: [
				{
					key: "cancelled-running-source",
					queue: "default",
					maxAttempts: 1,
					removeOnFailDays: 0,
					deadLetterQueue: "dlq",
					deadLetterTaskKey: "cancelled-running-destination",
				},
			],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const executionId = await db.client.invoke({
			task_key: "cancelled-running-source",
			queue: "default",
			payload: {},
		});
		const orchestratorId = crypto.randomUUID();
		await db.client.orchestratorHeartbeat({
			orchestratorId,
			version: "test",
			migrationNumber: 1,
		});
		const [execution] = await db.client.getExecutions({
			orchestratorId,
			queueName: "default",
			batchSize: 1,
			filterTaskKeys: [],
		});
		if (!execution || !executionId) throw new Error("expected claimed execution");

		expect(await db.client.cancelExecution(executionId)).toBe(true);
		await db.client.returnExecutions({
			count: 1,
			orchestratorId,
			completed: [],
			failed: [
				{
					execution_id: execution.id,
					queue: execution.queue,
					task_key: execution.task_key,
					orchestrator_id: execution.locked_by,
					status: "failed",
					error: "Task was cancelled",
				},
			],
			released: [],
			invokeChild: [],
			taskKeys: new Set([execution.task_key]),
		});

		const [counts] = await db.sql<{ source: string; destination: string }[]>`
			select
				count(*) filter (where queue = 'default')::text as source,
				count(*) filter (where queue = 'dlq')::text as destination
			from pgconductor._private_executions
		`;
		expect(counts).toEqual({ source: "0", destination: "0" });
	}, 15000);

	test("removes a parent failed by a claimed child cancellation without delivering to the DLQ", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({ sql: db.sql, context: {} });
		await conductor.ensureInstalled();
		await db.client.registerWorker({
			queueName: "default",
			taskSpecs: [
				{
					key: "cancelled-child-parent",
					queue: "default",
					removeOnFailDays: 0,
					deadLetterQueue: "dlq",
					deadLetterTaskKey: "cancelled-parent-destination",
				},
				{
					key: "cancelled-child",
					queue: "default",
					maxAttempts: 1,
					removeOnFailDays: 0,
				},
			],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const parentId = await db.client.invoke({
			task_key: "cancelled-child-parent",
			queue: "default",
			payload: {},
		});
		const parent = (
			await db.client.getExecutions({
				orchestratorId: crypto.randomUUID(),
				queueName: "default",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		if (!parent || !parentId) throw new Error("expected claimed parent execution");

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
					task_key: parent.task_key,
					orchestrator_id: parent.locked_by,
					status: "invoke_child",
					timeout_ms: "infinity",
					step_key: "child-step",
					child_task_name: "cancelled-child",
					child_task_queue: "default",
					child_payload: {},
				},
			],
			taskKeys: new Set([parent.task_key]),
		});
		const childOrchestratorId = crypto.randomUUID();
		await db.client.orchestratorHeartbeat({
			orchestratorId: childOrchestratorId,
			version: "test",
			migrationNumber: 1,
		});
		const child = (
			await db.client.getExecutions({
				orchestratorId: childOrchestratorId,
				queueName: "default",
				batchSize: 1,
				filterTaskKeys: [],
			})
		)[0];
		if (!child) throw new Error("expected claimed child execution");

		expect(await db.client.cancelExecution(child.id)).toBe(true);
		await db.client.returnExecutions({
			count: 1,
			orchestratorId: childOrchestratorId,
			completed: [],
			failed: [
				{
					execution_id: child.id,
					queue: child.queue,
					task_key: child.task_key,
					orchestrator_id: child.locked_by,
					status: "failed",
					error: "Task was cancelled",
				},
			],
			released: [],
			invokeChild: [],
			taskKeys: new Set([child.task_key]),
		});

		const [counts] = await db.sql<{ source: string; destination: string }[]>`
			select
				count(*) filter (where queue = 'default')::text as source,
				count(*) filter (where queue = 'dlq')::text as destination
			from pgconductor._private_executions
			where id = ${parentId}::uuid or queue = 'dlq'
		`;
		expect(counts).toEqual({ source: "0", destination: "0" });
	}, 15000);

	test("rejects direct self-targets in the database but permits cross-queue identity", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({ sql: db.sql, context: {} });
		await conductor.ensureInstalled();
		await expect(
			db.client.registerWorker({
				queueName: "default",
				taskSpecs: [
					{
						key: "self",
						queue: "default",
						maxAttempts: 1,
						deadLetterQueue: "default",
					},
				],
				cronSchedules: [],
				eventSubscriptions: [],
			}),
		).rejects.toThrow();

		await expect(
			db.client.registerWorker({
				queueName: "default",
				taskSpecs: [
					{
						key: "same-name",
						queue: "default",
						maxAttempts: 1,
						deadLetterQueue: "other-queue",
						deadLetterTaskKey: "same-name",
					},
				],
				cronSchedules: [],
				eventSubscriptions: [],
			}),
		).resolves.toBeUndefined();
	}, 15000);

	test("rolls back the source settlement when destination insertion fails", async () => {
		const db = await pool.child();
		databases.push(db);
		const payloadSchema = z.object({ value: z.string() });
		const sourceDefinition = defineTask({ name: "rollback-source", payload: payloadSchema });
		const destinationDefinition = defineTask({
			name: "rollback-destination",
			queue: "dlq",
			payload: payloadSchema,
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([sourceDefinition, destinationDefinition]),
			context: {},
		});
		await conductor.ensureInstalled();
		await db.client.registerWorker({
			queueName: "default",
			taskSpecs: [
				{
					key: "rollback-source",
					queue: "default",
					maxAttempts: 1,
					deadLetterQueue: "dlq",
					deadLetterTaskKey: "rollback-destination",
				},
			],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const id = await db.client.invoke({
			task_key: "rollback-source",
			queue: "default",
			payload: { value: "rollback" },
		});
		const claimed = await db.client.getExecutions({
			orchestratorId: crypto.randomUUID(),
			queueName: "default",
			batchSize: 1,
			filterTaskKeys: [],
		});
		const execution = claimed[0]!;
		if (!execution.locked_by) throw new Error("execution was not claimed");
		const lockedBy = execution.locked_by;
		await db.sql.unsafe(`
			create function public.fail_dlq_insert() returns trigger language plpgsql as $$
			begin raise exception 'forced destination failure'; end;
			$$;
			create trigger fail_dlq_insert before insert on pgconductor.executions_dlq
			for each row execute function public.fail_dlq_insert();
		`);
		const settlement = {
			execution_id: execution.id,
			queue: execution.queue,
			task_key: execution.task_key,
			status: "failed" as const,
			orchestrator_id: lockedBy,
			error: "rollback failure",
		};
		await expect(
			db.client.returnExecutions({
				count: 1,
				orchestratorId: lockedBy,
				completed: [],
				failed: [settlement],
				released: [],
				invokeChild: [],
				taskKeys: new Set([execution.task_key]),
			}),
		).rejects.toThrow("forced destination failure");
		const afterRollback = await db.sql<
			{ failed_at: Date | null; locked_by: string | null; attempts: number }[]
		>`select failed_at, locked_by, attempts from pgconductor._private_executions where id = ${id}::uuid`;
		expect(afterRollback[0]?.failed_at).toBeNull();
		expect(afterRollback[0]?.locked_by).toBe(lockedBy);
		expect(afterRollback[0]?.attempts).toBe(1);
		await db.sql.unsafe(
			`drop trigger fail_dlq_insert on pgconductor.executions_dlq; drop function public.fail_dlq_insert();`,
		);
		await db.client.returnExecutions({
			count: 1,
			orchestratorId: lockedBy,
			completed: [],
			failed: [settlement],
			released: [],
			invokeChild: [],
			taskKeys: new Set([execution.task_key]),
		});
		const finalRows = await db.sql<{ source: string | null; source_failed: string }[]>`
			select dead_letter_source_execution_id::text as source, (select failed_at is not null from pgconductor._private_executions where id = ${id}::uuid)::text as source_failed
			from pgconductor._private_executions where queue = 'dlq'
		`;
		expect(Array.from(finalRows)).toEqual([{ source: id, source_failed: "true" }]);
	}, 20000);
});
