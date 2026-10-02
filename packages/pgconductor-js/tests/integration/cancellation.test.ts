import postgres from "postgres";
import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";
import { waitForCondition } from "../test-utils";

describe("Cancellation Support", () => {
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

	test("cancel pending execution sets failed_at", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinition = defineTask({
			name: "slow-task",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		conductor.createTask({ name: "slow-task" }, { invocable: true }, async (event, _ctx) => {
			if (event.name === "pgconductor.invoke") {
				await new Promise((r) => setTimeout(r, 10000));
				return { result: "done" };
			}
			throw new Error("Unexpected event type");
		});

		// Initialize schema
		await conductor.ensureInstalled();

		// Invoke task (execution stays pending since no orchestrator running)
		await conductor.invoke({ name: "slow-task" }, {});

		// Wait a bit to ensure execution is created
		await new Promise((r) => setTimeout(r, 100));

		// Get execution ID
		const executions = await db.sql<{ id: string }[]>`
			select id from pgconductor._private_executions where task_key = 'slow-task'
		`;
		expect(executions.length).toBe(1);
		const executionId = executions[0]!.id;

		// Cancel the pending execution
		const cancelled = await db.client.cancelExecution(executionId);
		expect(cancelled).toBe(true);

		// Verify execution is failed
		const failedExecution = await db.sql<
			{
				failed_at: Date | null;
				last_error: string | null;
				cancelled: boolean;
			}[]
		>`
			select failed_at, last_error, cancelled
			from pgconductor._private_executions
			where id = ${executionId}::uuid
		`;

		expect(failedExecution[0]?.failed_at).not.toBeNull();
		expect(failedExecution[0]?.last_error).toBe("Cancelled by user");
		expect(failedExecution[0]?.cancelled).toBe(false); // Not set for pending
	}, 10000);

	test("cancel running execution sets cancelled flag and signals orchestrator", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinition = defineTask({
			name: "long-running-task",
			payload: z.object({}),
			returns: z.object({ completed: z.boolean() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const longRunningTask = conductor.createTask(
			{ name: "long-running-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					await new Promise((r) => setTimeout(r, 10000));
					return { completed: true };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [longRunningTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Invoke task
		await conductor.invoke({ name: "long-running-task" }, {});

		// Wait for task to start executing
		await new Promise((r) => setTimeout(r, 200));

		// Get execution ID
		const executions = await db.sql<{ id: string; locked_by: string | null }[]>`
			select id, locked_by
			from pgconductor._private_executions
			where task_key = 'long-running-task'
		`;
		expect(executions.length).toBe(1);
		expect(executions[0]!.locked_by).not.toBeNull(); // Ensure it's running

		const executionId = executions[0]!.id;

		// Cancel the running execution
		const cancelled = await db.client.cancelExecution(executionId);
		expect(cancelled).toBe(true);

		// Verify execution has cancelled flag set
		const cancelledExecution = await db.sql<
			{
				cancelled: boolean;
				failed_at: Date | null;
			}[]
		>`
			select cancelled, failed_at
			from pgconductor._private_executions
			where id = ${executionId}::uuid
		`;

		expect(cancelledExecution[0]?.cancelled).toBe(true);
		expect(cancelledExecution[0]?.failed_at).toBeNull(); // Not failed yet

		// Verify signal was created
		const signals = await db.sql<
			{
				type: string;
				execution_id: string;
			}[]
		>`
			select type, execution_id
			from pgconductor._private_orchestrator_signals
		`;

		expect(signals.length).toBe(1);
		expect(signals[0]?.type).toBe("cancel_execution");
		expect(signals[0]?.execution_id).toBe(executionId);

		await orchestrator.stop();
	}, 15000);

	test("cancelled execution doesn't re-execute on retry", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDefinition = defineTask({
			name: "retry-task",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const taskFn = mock(() => {
			return { result: "success" };
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			context: {},
		});

		const retryTask = conductor.createTask(
			{ name: "retry-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					return taskFn();
				}
				throw new Error("Unexpected event type");
			},
		);

		// Initialize schema
		await conductor.ensureInstalled();

		// Create execution but don't start orchestrator
		await conductor.invoke({ name: "retry-task" }, {});

		await new Promise((r) => setTimeout(r, 100));

		// Get execution ID
		const executions = await db.sql<{ id: string }[]>`
			select id from pgconductor._private_executions where task_key = 'retry-task'
		`;
		const executionId = executions[0]!.id;

		// Cancel it
		await db.client.cancelExecution(executionId);

		// Now start orchestrator - should see cancelled flag and not execute
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [retryTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Wait for orchestrator to process
		await new Promise((r) => setTimeout(r, 300));

		await orchestrator.stop();

		// Task should not have been called
		expect(taskFn).toHaveBeenCalledTimes(0);

		// Execution should be permanently failed
		const finalExecution = await db.sql<
			{
				failed_at: Date | null;
				last_error: string | null;
			}[]
		>`
			select failed_at, last_error
			from pgconductor._private_executions
			where id = ${executionId}::uuid
		`;

		expect(finalExecution[0]?.failed_at).not.toBeNull();
		expect(finalExecution[0]?.last_error).toBe("Cancelled by user");
	}, 10000);

	test("cancelExecution returns false for non-existent or completed executions", async () => {
		const db = await pool.child();
		databases.push(db);

		const dummyTask = defineTask({
			name: "dummy-task",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([dummyTask]),
			context: {},
		});

		conductor.createTask({ name: "dummy-task" }, { invocable: true }, async () => {});

		// Initialize schema
		await conductor.ensureInstalled();

		const fakeId = "00000000-0000-0000-0000-000000000000";
		const cancelled = await db.client.cancelExecution(fakeId);

		expect(cancelled).toBe(false);
	}, 5000);

	test("cancel parent execution waiting on child", async () => {
		const db = await pool.child();
		databases.push(db);

		const parentDef = defineTask({
			name: "parent-task",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const childDef = defineTask({
			name: "child-task",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDef, childDef]),
			context: {},
		});

		const parentTask = conductor.createTask(
			{ name: "parent-task" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name === "pgconductor.invoke") {
					const childResult = await ctx.invoke("invoke-child", { name: "child-task" }, {});
					return { result: `parent got: ${childResult.result}` };
				}
				throw new Error("Unexpected event");
			},
		);

		const childTask = conductor.createTask(
			{ name: "child-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					await new Promise((r) => setTimeout(r, 10000));
					return { result: "child done" };
				}
				throw new Error("Unexpected event");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parentTask, childTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Invoke parent
		await conductor.invoke({ name: "parent-task" }, {});

		// Wait for parent to start and invoke child
		await new Promise((r) => setTimeout(r, 300));

		// Get parent execution
		const parentExecs = await db.sql<{ id: string; waiting_on_execution_id: string | null }[]>`
			select id, waiting_on_execution_id
			from pgconductor._private_executions
			where task_key = 'parent-task'
		`;

		expect(parentExecs.length).toBe(1);
		expect(parentExecs[0]!.waiting_on_execution_id).not.toBeNull();

		const parentId = parentExecs[0]!.id;

		// Cancel parent while it's waiting
		const cancelled = await db.client.cancelExecution(parentId);
		expect(cancelled).toBe(true);

		// Verify parent is failed
		const failedParent = await db.sql<{ failed_at: Date | null; last_error: string | null }[]>`
			select failed_at, last_error
			from pgconductor._private_executions
			where id = ${parentId}::uuid
		`;

		expect(failedParent[0]!.failed_at).not.toBeNull();
		expect(failedParent[0]!.last_error).toBe("Cancelled by user");

		await orchestrator.stop();
	}, 15000);

	test("cancel child that parent is waiting on", async () => {
		const db = await pool.child();
		databases.push(db);

		const parentDef = defineTask({
			name: "parent-task-2",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const childDef = defineTask({
			name: "child-task-2",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDef, childDef]),
			context: {},
		});

		const parentTask = conductor.createTask(
			{ name: "parent-task-2" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name === "pgconductor.invoke") {
					const childResult = await ctx.invoke("invoke-child-2", { name: "child-task-2" }, {});
					return { result: `parent got: ${childResult.result}` };
				}
				throw new Error("Unexpected event");
			},
		);

		const childTask = conductor.createTask(
			{ name: "child-task-2" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					await new Promise((r) => setTimeout(r, 10000));
					return { result: "child done" };
				}
				throw new Error("Unexpected event");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parentTask, childTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Invoke parent
		await conductor.invoke({ name: "parent-task-2" }, {});

		// Wait for child to be created and start running
		await new Promise((r) => setTimeout(r, 300));

		// Get child execution
		const childExecs = await db.sql<{ id: string; locked_by: string | null }[]>`
			select id, locked_by
			from pgconductor._private_executions
			where task_key = 'child-task-2'
		`;

		expect(childExecs.length).toBe(1);
		expect(childExecs[0]!.locked_by).not.toBeNull();

		const childId = childExecs[0]!.id;

		// Cancel child while it's running
		const cancelled = await db.client.cancelExecution(childId);
		expect(cancelled).toBe(true);

		// Verify child has cancelled flag
		const cancelledChild = await db.sql<{ cancelled: boolean }[]>`
			select cancelled
			from pgconductor._private_executions
			where id = ${childId}::uuid
		`;

		expect(cancelledChild[0]!.cancelled).toBe(true);

		await orchestrator.stop();
	}, 15000);

	test("cancel execution in released state (failed, scheduled to retry)", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({
			name: "failing-task",
			payload: z.object({}),
		});

		let attemptCount = 0;

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const failingTask = conductor.createTask(
			{ name: "failing-task", maxAttempts: 5 },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					attemptCount++;
					throw new Error("Intentional failure");
				}
				throw new Error("Unexpected event");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [failingTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		// Invoke task
		await conductor.invoke({ name: "failing-task" }, {});

		// Wait for first attempt to fail and be released
		await new Promise((r) => setTimeout(r, 500));

		await orchestrator.stop();

		// Verify it failed once and is scheduled to retry
		expect(attemptCount).toBe(1);

		const executions = await db.sql<
			{
				id: string;
				attempts: number;
				failed_at: Date | null;
				run_at: Date;
			}[]
		>`
			select id, attempts, failed_at, run_at
			from pgconductor._private_executions
			where task_key = 'failing-task'
		`;

		expect(executions.length).toBe(1);
		expect(executions[0]!.attempts).toBe(1);
		expect(executions[0]!.failed_at).toBeNull();

		const execId = executions[0]!.id;

		// Cancel it while it's in released state
		const cancelled = await db.client.cancelExecution(execId);
		expect(cancelled).toBe(true);

		// Verify it's now failed
		const failedExec = await db.sql<{ failed_at: Date | null; last_error: string | null }[]>`
			select failed_at, last_error
			from pgconductor._private_executions
			where id = ${execId}::uuid
		`;

		expect(failedExec[0]!.failed_at).not.toBeNull();
		expect(failedExec[0]!.last_error).toBe("Cancelled by user");
	}, 10000);

	test("multiple cancellations are idempotent", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({
			name: "idempotent-task",
			payload: z.object({}),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		conductor.createTask({ name: "idempotent-task" }, { invocable: true }, async (event, _ctx) => {
			if (event.name === "pgconductor.invoke") {
				await new Promise((r) => setTimeout(r, 10000));
			}
		});

		// Initialize schema
		await conductor.ensureInstalled();

		// Invoke task (stays pending)
		await conductor.invoke({ name: "idempotent-task" }, {});

		await new Promise((r) => setTimeout(r, 100));

		const executions = await db.sql<{ id: string }[]>`
			select id from pgconductor._private_executions where task_key = 'idempotent-task'
		`;

		const execId = executions[0]!.id;

		// Cancel multiple times
		const cancelled1 = await db.client.cancelExecution(execId);
		const cancelled2 = await db.client.cancelExecution(execId);
		const cancelled3 = await db.client.cancelExecution(execId);

		expect(cancelled1).toBe(true);
		expect(cancelled2).toBe(false);
		expect(cancelled3).toBe(false);

		// Verify execution failed only once
		const finalExec = await db.sql<
			{
				failed_at: Date | null;
				last_error: string | null;
			}[]
		>`
			select failed_at, last_error
			from pgconductor._private_executions
			where id = ${execId}::uuid
		`;

		expect(finalExec[0]!.failed_at).not.toBeNull();
		expect(finalExec[0]!.last_error).toBe("Cancelled by user");
	}, 10000);

	test("cancelled execution fails when orchestrator dies", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({
			name: "long-task",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const longTask = conductor.createTask(
			{ name: "long-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					await new Promise((r) => setTimeout(r, 10000));
					return { result: "done" };
				}
				throw new Error("Unexpected event");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [longTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		await conductor.invoke({ name: "long-task" }, {});

		await new Promise((r) => setTimeout(r, 300));

		const executions = await db.sql<
			{
				id: string;
				locked_by: string | null;
			}[]
		>`
			select id, locked_by
			from pgconductor._private_executions
			where task_key = 'long-task'
		`;

		expect(executions.length).toBe(1);
		expect(executions[0]!.locked_by).not.toBeNull();

		const execId = executions[0]!.id;
		const orchestratorId = executions[0]!.locked_by as string;

		const cancelled = await db.client.cancelExecution(execId);
		expect(cancelled).toBe(true);

		const cancelledExec = await db.sql<
			{
				cancelled: boolean;
				failed_at: Date | null;
			}[]
		>`
			select cancelled, failed_at
			from pgconductor._private_executions
			where id = ${execId}::uuid
		`;

		expect(cancelledExec[0]!.cancelled).toBe(true);
		expect(cancelledExec[0]!.failed_at).toBeNull();

		await db.client.orchestratorShutdown({ orchestratorId });

		const failedExec = await db.sql<
			{
				failed_at: Date | null;
				last_error: string | null;
				locked_by: string | null;
			}[]
		>`
			select failed_at, last_error, locked_by
			from pgconductor._private_executions
			where id = ${execId}::uuid
		`;

		expect(failedExec[0]!.failed_at).not.toBeNull();
		expect(failedExec[0]!.last_error).toBe("Cancelled by user");
		expect(failedExec[0]!.locked_by).toBeNull();

		await orchestrator.stop();
	}, 15000);

	test("cancel execution with custom reason", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({
			name: "custom-reason-task",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "custom-reason-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					await new Promise((r) => setTimeout(r, 10000));
					return { result: "done" };
				}
				throw new Error("Unexpected event");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		await conductor.invoke({ name: "custom-reason-task" }, {});

		await new Promise((r) => setTimeout(r, 300));

		const executions = await db.sql<{ id: string }[]>`
			select id
			from pgconductor._private_executions
			where task_key = 'custom-reason-task'
		`;

		const execId = executions[0]!.id;

		const cancelled = await db.client.cancelExecution(execId, {
			reason: "User requested immediate shutdown",
		});
		expect(cancelled).toBe(true);

		await new Promise((r) => setTimeout(r, 200));

		const failedExec = await db.sql<
			{
				last_error: string | null;
			}[]
		>`
			select last_error
			from pgconductor._private_executions
			where id = ${execId}::uuid
		`;

		expect(failedExec[0]!.last_error).toBe("User requested immediate shutdown");

		await orchestrator.stop();
	}, 15000);

	test("cancel from conductor API", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({
			name: "conductor-cancel-task",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "conductor-cancel-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					await new Promise((r) => setTimeout(r, 10000));
					return { result: "done" };
				}
				throw new Error("Unexpected event");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		const execId = await conductor.invoke({ name: "conductor-cancel-task" }, {});

		await new Promise((r) => setTimeout(r, 300));

		const cancelled = await conductor.cancel(execId, { reason: "Cancelled from conductor" });
		expect(cancelled).toBe(true);

		await new Promise((r) => setTimeout(r, 200));

		const failedExec = await db.sql<
			{
				last_error: string | null;
			}[]
		>`
			select last_error
			from pgconductor._private_executions
			where id = ${execId}::uuid
		`;

		expect(failedExec[0]!.last_error).toBe("Cancelled from conductor");

		await orchestrator.stop();
	}, 15000);

	test("cancel from task context", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({
			name: "context-cancel-task",
			payload: z.object({ execIdToCancel: z.string() }),
			returns: z.object({ result: z.string() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		let capturedResult: boolean | null = null;

		const contextCancelTask = conductor.createTask(
			{ name: "context-cancel-task" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name === "pgconductor.invoke") {
					capturedResult = await ctx.cancel(event.payload.execIdToCancel, {
						reason: "Cancelled from task context",
					});
					return { result: `cancelled: ${capturedResult}` };
				}
				throw new Error("Unexpected event");
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [contextCancelTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();

		const pendingExecId = await db.client.invoke({
			task_key: "some-task",
			queue: "default",
			payload: {},
		});

		await conductor.invoke({ name: "context-cancel-task" }, { execIdToCancel: pendingExecId! });

		await new Promise((r) => setTimeout(r, 300));

		if (capturedResult !== true) {
			throw new Error(`Expected capturedResult to be true, got ${capturedResult}`);
		}

		const failedExec = await db.sql<
			{
				last_error: string | null;
			}[]
		>`
			select last_error
			from pgconductor._private_executions
			where id = ${pendingExecId}::uuid
		`;

		expect(failedExec[0]!.last_error).toBe("Cancelled from task context");

		await orchestrator.stop();
	}, 15000);

	async function installSchema(db: TestDatabase) {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "workflow-task", payload: z.object({}) })]),
			context: {},
		});
		await conductor.ensureInstalled();
	}

	async function insertExecution(
		db: TestDatabase,
		{ waitingOn, lockedBy }: { waitingOn?: string; lockedBy?: string } = {},
	) {
		const [execution] = await db.sql<[{ id: string }]>`
			insert into pgconductor._private_executions
				(task_key, payload, run_at, waiting_on_execution_id, waiting_step_key, locked_by, locked_at)
			values (
				'workflow-task', '{}'::jsonb,
				${waitingOn ? "infinity" : "now"}::text::timestamptz,
				${waitingOn || null}::uuid, ${waitingOn ? "child" : null},
				${lockedBy || null}::uuid, ${lockedBy ? "now" : null}::text::timestamptz
			)
			returning id
		`;
		if (waitingOn) {
			await db.sql`
				update pgconductor._private_executions
				set parent_execution_id = ${execution.id}::uuid
				where id = ${waitingOn}::uuid
			`;
		}
		return execution.id;
	}

	async function executionState(db: TestDatabase, id: string) {
		const [execution] = await db.sql<
			[
				{
					failed: boolean;
					cancelled: boolean;
					last_error: string | null;
					waiting_on_execution_id: string | null;
				},
			]
		>`
			select failed_at is not null as failed, cancelled, last_error, waiting_on_execution_id
			from pgconductor._private_executions
			where id = ${id}::uuid
		`;
		return execution;
	}

	test("cancelling a pending child fails the workflow waiting on it", async () => {
		const db = await pool.child();
		databases.push(db);
		await installSchema(db);

		const child = await insertExecution(db);
		const parent = await insertExecution(db, { waitingOn: child });
		const root = await insertExecution(db, { waitingOn: parent });

		expect(await db.client.cancelExecution(child)).toBe(true);

		expect(await executionState(db, child)).toEqual({
			failed: true,
			cancelled: false,
			last_error: "Cancelled by user",
			waiting_on_execution_id: null,
		});
		for (const id of [parent, root]) {
			expect(await executionState(db, id)).toEqual({
				failed: true,
				cancelled: false,
				last_error: "Child execution failed: Cancelled by user",
				waiting_on_execution_id: null,
			});
		}
	}, 15000);

	test("cancelling a waiting workflow cancels every execution it waits on", async () => {
		const db = await pool.child();
		databases.push(db);
		await installSchema(db);

		const [orchestrator] = await db.sql<[{ id: string }]>`
			insert into pgconductor._private_orchestrators default values returning id
		`;
		const child = await insertExecution(db, { lockedBy: orchestrator.id });
		const parent = await insertExecution(db, { waitingOn: child });
		const root = await insertExecution(db, { waitingOn: parent });

		expect(await db.client.cancelExecution(root)).toBe(true);

		expect(await executionState(db, child)).toEqual({
			failed: false,
			cancelled: true,
			last_error: "Cancelled by user",
			waiting_on_execution_id: null,
		});
		for (const id of [root, parent]) {
			expect(await executionState(db, id)).toEqual({
				failed: true,
				cancelled: false,
				last_error: "Cancelled by user",
				waiting_on_execution_id: null,
			});
		}
		const signals = await db.sql<{ orchestrator_id: string; execution_id: string }[]>`
			select orchestrator_id, execution_id
			from pgconductor._private_orchestrator_signals
			where type = 'cancel_execution'
		`;
		expect(signals.map((s) => [s.orchestrator_id, s.execution_id])).toEqual([
			[orchestrator.id, child],
		]);
	}, 15000);

	test("cancellation locks a child before its waiting parent", async () => {
		const db = await pool.child();
		databases.push(db);
		await installSchema(db);

		const [orchestrator] = await db.sql<[{ id: string }]>`
			insert into pgconductor._private_orchestrators default values returning id
		`;
		const child = await insertExecution(db, { lockedBy: orchestrator.id });
		const parent = await insertExecution(db, { waitingOn: child });

		const worker = postgres(db.url, { max: 1 });
		const canceller = postgres(db.url, { max: 1 });
		try {
			// Returning a child result locks the child, then the parent it wakes.
			await worker`begin`;
			await worker`select 1 from pgconductor._private_executions where id = ${child}::uuid for update`;
			const cancellation = canceller<[{ cancelled: boolean }]>`
				select pgconductor.cancel_execution(${parent}::uuid) as cancelled
			`.execute();
			await waitForCondition(async () => {
				const [blocked] = await db.sql<[{ count: number }]>`
					select count(*)::int as count from pg_stat_activity
					where datname = current_database() and wait_event_type = 'Lock'
				`;
				return blocked.count === 1;
			});
			await worker`select 1 from pgconductor._private_executions where id = ${parent}::uuid for update`;
			await worker`commit`;
			const [{ cancelled }] = await cancellation;
			expect(cancelled).toBe(true);
		} finally {
			await Promise.all([worker.end(), canceller.end()]);
		}

		expect((await executionState(db, child)).cancelled).toBe(true);
		expect((await executionState(db, parent)).failed).toBe(true);
	}, 15000);

	test("cancelling a pending execution honors remove_on_fail_days = 0", async () => {
		const db = await pool.child();
		databases.push(db);
		await installSchema(db);
		await db.sql`
			insert into pgconductor._private_tasks (key, queue, remove_on_fail_days)
			values ('workflow-task', 'default', 0)
		`;

		const child = await insertExecution(db);
		await insertExecution(db, { waitingOn: child });

		expect(await db.client.cancelExecution(child)).toBe(true);

		const [{ count }] = await db.sql<[{ count: number }]>`
			select count(*)::int as count from pgconductor._private_executions
		`;
		expect(count).toBe(0);
	}, 15000);

	test("superseding a running execution honors remove_on_fail_days = 0", async () => {
		const db = await pool.child();
		databases.push(db);
		await installSchema(db);
		await db.sql`
			insert into pgconductor._private_tasks (key, queue, remove_on_fail_days)
			values ('workflow-task', 'default', 0)
		`;
		const [orchestrator] = await db.sql<[{ id: string }]>`
			insert into pgconductor._private_orchestrators default values returning id
		`;
		const spec = { task_key: "workflow-task", queue: "default", payload: {}, dedupe_key: "key" };

		const first = await db.client.invoke(spec);
		await db.sql`update pgconductor._private_executions set locked_by = ${orchestrator.id}::uuid, locked_at = now()`;
		const [second] = await db.client.invokeBatch([spec]);
		await db.sql`update pgconductor._private_executions set locked_by = ${orchestrator.id}::uuid, locked_at = now()`;
		const third = await db.client.invoke(spec);

		expect(new Set([first, second, third]).size).toBe(3);
		const remaining = await db.sql<{ is_third: boolean }[]>`
			select id = ${third}::uuid as is_third from pgconductor._private_executions
		`;
		expect(remaining.map((r) => r.is_third)).toEqual([true]);
	}, 15000);
});
