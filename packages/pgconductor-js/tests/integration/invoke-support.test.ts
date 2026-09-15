import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach, mock } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import { waitForCondition } from "../test-utils";

describe("Invoke Support", () => {
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

	test("invoke() calls child task and returns result", async () => {
		const db = await pool.child();
		databases.push(db);

		const parentDefinition = defineTask({
			name: "parent-task",
			payload: z.object({ value: z.number() }),
			returns: z.object({ result: z.number() }),
		});

		const childDefinition = defineTask({
			name: "child-task",
			payload: z.object({ input: z.number() }),
			returns: z.object({ output: z.number() }),
		});

		const childFn = mock((n: number) => n * 2);
		const childCompleted = new Deferred<void>();

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const childTask = conductor.createTask(
			{ name: "child-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					const result = childFn(event.payload.input);
					childCompleted.resolve();
					return { output: result };
				}
				throw new Error("Unexpected event type");
			},
		);

		const parentTask = conductor.createTask(
			{ name: "parent-task" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name === "pgconductor.invoke") {
					const childResult = await ctx.invoke(
						"invoke-child",
						{ name: "child-task" },
						{ input: event.payload.value },
					);
					return { result: childResult.output };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			conductor,
			tasks: [parentTask, childTask],
		});

		await orchestrator.start();

		await conductor.invoke({ name: "parent-task" }, { value: 5 });
		await childCompleted.promise;
		await orchestrator.stop();

		expect(childFn).toHaveBeenCalledTimes(1);
		expect(childFn).toHaveBeenCalledWith(5);
	}, 30000);

	test("invoke() with timeout throws error", async () => {
		const db = await pool.child();
		databases.push(db);

		const parentDefinition = defineTask({
			name: "timeout-parent",
			payload: z.object({}),
			returns: z.object({ success: z.boolean() }),
		});

		const childDefinition = defineTask({
			name: "slow-child",
			payload: z.object({}),
			returns: z.object({ completed: z.boolean() }),
		});

		const parentError = mock((err: Error) => err.message);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const slowChildTask = conductor.createTask(
			{ name: "slow-child" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.sleep("long-sleep", 5000);
				return { completed: true };
			},
		);

		const timeoutParentTask = conductor.createTask(
			{ name: "timeout-parent" },
			{ invocable: true },
			async (_event, ctx) => {
				try {
					await ctx.invoke("invoke-slow", { name: "slow-child" }, {}, 1000);
					return { success: true };
				} catch (err) {
					parentError(err as Error);
					throw err;
				}
			},
		);

		const startTime = new Date("2024-01-01T00:00:00Z");
		await db.client.setFakeTime({ date: startTime });

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [timeoutParentTask, slowChildTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		await conductor.invoke({ name: "timeout-parent" }, {});
		await waitForCondition(async () => {
			const [child] = await db.sql<{ released: boolean; sleeping: boolean }[]>`
				select
					locked_by is null as released,
					exists (
						select 1 from pgconductor._private_steps s
						where s.execution_id = e.id and s.key = 'long-sleep'
					) as sleeping
				from pgconductor._private_executions e
				where task_key = 'slow-child'
			`;
			return child?.released === true && child.sleeping;
		});

		// Advance time past timeout (1 second) but before sleep completes (5 seconds)
		const afterTimeout = new Date(startTime.getTime() + 1500);
		await db.client.setFakeTime({ date: afterTimeout });
		await waitForCondition(async () => parentError.mock.calls.length === 1);
		await orchestrator.stop();

		await db.client.clearFakeTime();

		expect(parentError).toHaveBeenCalledTimes(1);
		const errorMsg = parentError.mock.results[0]?.value;
		expect(errorMsg).toContain("timed out after 1000ms");
	}, 30000);

	test("invoke() caches child result on retry", async () => {
		const db = await pool.child();
		databases.push(db);

		const parentDefinition = defineTask({
			name: "retry-parent",
			payload: z.object({ value: z.number() }),
			returns: z.object({ result: z.number() }),
		});

		const childDefinition = defineTask({
			name: "once-child",
			payload: z.object({ input: z.number() }),
			returns: z.object({ output: z.number() }),
		});

		const childFn = mock((n: number) => n * 3);
		const parentCompleted = new Deferred<void>();

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const onceChildTask = conductor.createTask(
			{ name: "once-child" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					const result = childFn(event.payload.input);
					return { output: result };
				}
				throw new Error("Unexpected event type");
			},
		);

		let parentAttempts = 0;
		const retryParentTask = conductor.createTask(
			{ name: "retry-parent", maxAttempts: 3 },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name === "pgconductor.invoke") {
					parentAttempts++;
					const childResult = await ctx.invoke(
						"invoke-once",
						{ name: "once-child" },
						{ input: event.payload.value },
					);

					// Fail on first attempt, succeed on retry
					if (parentAttempts === 1) {
						throw new Error("First attempt fails");
					}

					parentCompleted.resolve();
					return { result: childResult.output };
				}
				throw new Error("Unexpected event type");
			},
		);

		const orchestrator = Orchestrator.create({
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			conductor,
			tasks: [retryParentTask, onceChildTask],
		});

		await orchestrator.start();

		await conductor.invoke({ name: "retry-parent" }, { value: 7 });
		await parentCompleted.promise;
		await orchestrator.stop();

		expect(childFn).toHaveBeenCalledTimes(1);
		expect(childFn).toHaveBeenCalledWith(7);
	}, 30000);

	test("invoke() without timeout waits indefinitely", async () => {
		const db = await pool.child();
		databases.push(db);

		const parentDefinition = defineTask({
			name: "patient-parent",
			payload: z.object({}),
			returns: z.object({ childResult: z.string() }),
		});

		const childDefinition = defineTask({
			name: "eventual-child",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const childFn = mock(() => "completed");
		const childCompleted = new Deferred<void>();

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const eventualChildTask = conductor.createTask(
			{ name: "eventual-child" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.sleep("moderate-sleep", 2000);
				const result = childFn();
				childCompleted.resolve();
				return { result };
			},
		);

		const patientParentTask = conductor.createTask(
			{ name: "patient-parent" },
			{ invocable: true },
			async (_event, ctx) => {
				const childResult = await ctx.invoke("invoke-eventual", { name: "eventual-child" }, {});
				return { childResult: childResult.result };
			},
		);

		const orchestrator = Orchestrator.create({
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			conductor,
			tasks: [patientParentTask, eventualChildTask],
		});

		await orchestrator.start();

		await conductor.invoke({ name: "patient-parent" }, {});
		await childCompleted.promise;
		await orchestrator.stop();

		expect(childFn).toHaveBeenCalledTimes(1);
	}, 40000);

	test("invoke() cascade failure when child fails permanently", async () => {
		const db = await pool.child();
		databases.push(db);

		const parentDefinition = defineTask({
			name: "cascade-parent",
			payload: z.object({}),
			returns: z.object({ success: z.boolean() }),
		});

		const childDefinition = defineTask({
			name: "failing-child",
			payload: z.object({}),
			returns: z.object({ result: z.string() }),
		});

		const childFn = mock(() => {
			throw new Error("Child always fails");
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const failingChildTask = conductor.createTask(
			{ name: "failing-child", maxAttempts: 2 },
			{ invocable: true },
			async (_event, _ctx) => {
				childFn();
				return { result: "never-reached" };
			},
		);

		const cascadeParentTask = conductor.createTask(
			{ name: "cascade-parent" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.invoke("invoke-failing", { name: "failing-child" }, {});
				return { success: true };
			},
		);

		const startTime = new Date("2024-01-01T00:00:00Z");
		await db.client.setFakeTime({ date: startTime });

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [cascadeParentTask, failingChildTask],
			defaultWorker: {
				pollIntervalMs: 100,
				flushIntervalMs: 100,
			},
		});

		await orchestrator.start();

		await conductor.invoke({ name: "cascade-parent" }, {});

		await waitForCondition(async () => {
			const [child] = await db.sql<{ attempts: number; released: boolean }[]>`
				select attempts, locked_by is null as released
				from pgconductor._private_executions
				where task_key = 'failing-child'
			`;
			return child?.attempts === 1 && child.released;
		});
		expect(childFn).toHaveBeenCalledTimes(1);

		const [retry] = await db.sql<{ run_at: Date }[]>`
			select run_at from pgconductor._private_executions
			where task_key = 'failing-child'
		`;
		if (!retry) throw new Error("expected persisted child retry");
		await db.client.setFakeTime({ date: new Date(retry.run_at.getTime() + 1) });

		await waitForCondition(async () => {
			const [parent] = await db.sql<{ failed: boolean }[]>`
				select failed_at is not null as failed
				from pgconductor._private_executions
				where task_key = 'cascade-parent'
			`;
			return parent?.failed === true;
		});
		await orchestrator.stop();

		await db.client.clearFakeTime();

		expect(childFn).toHaveBeenCalledTimes(2);

		const failedParents = await db.sql<Array<{ last_error: string; task_key: string }>>`
			SELECT last_error, task_key
			FROM pgconductor._private_executions
			WHERE task_key = 'cascade-parent' AND failed_at IS NOT NULL
		`;

		expect(failedParents.length).toBe(1);
		expect(failedParents[0]?.last_error).toContain("Child execution failed");
	}, 5000);

	test("invoke() works across queues", async () => {
		const db = await pool.child();
		databases.push(db);

		const parentDefinition = defineTask({
			queue: "parent-queue",
			name: "parent-task",
			payload: z.object({ value: z.number() }),
			returns: z.object({ result: z.number() }),
		});

		const childDefinition = defineTask({
			queue: "child-queue",
			name: "child-task",
			payload: z.object({ input: z.number() }),
			returns: z.object({ output: z.number() }),
		});

		const childFn = mock((n: number) => n * 2);
		const childCalled = new Deferred<void>();

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		const parentTask = conductor.createTask(
			{ queue: "parent-queue", name: "parent-task" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name === "pgconductor.invoke") {
					const childResult = await ctx.invoke(
						"invoke-child",
						{ queue: "child-queue", name: "child-task" },
						{ input: event.payload.value },
					);
					return { result: childResult.output };
				}
				throw new Error("Unexpected event type");
			},
		);

		const parentWorker = conductor.createWorker({
			queue: "parent-queue",
			tasks: [parentTask],
		});

		const childTask = conductor.createTask(
			{ queue: "child-queue", name: "child-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					const result = childFn(event.payload.input);
					childCalled.resolve();
					return { output: result };
				}
				throw new Error("Unexpected event type");
			},
		);

		const childWorker = conductor.createWorker({
			queue: "child-queue",
			tasks: [childTask],
		});

		const orchestrator = Orchestrator.create({
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			conductor,
			workers: [parentWorker, childWorker],
		});

		await orchestrator.start();

		await conductor.invoke({ queue: "parent-queue", name: "parent-task" }, { value: 5 });
		await childCalled.promise;
		await orchestrator.stop();

		expect(childFn).toHaveBeenCalledTimes(1);
		expect(childFn).toHaveBeenCalledWith(5);
	}, 30000);

	test("invoke() timeout fails pending child immediately", async () => {
		const db = await pool.child();
		databases.push(db);

		const parentDefinition = defineTask({
			name: "timeout-parent-2",
			payload: z.object({}),
			returns: z.object({ success: z.boolean() }),
		});

		const childDefinition = defineTask({
			name: "slow-child-2",
			queue: "pending-child-queue",
			payload: z.object({}),
			returns: z.object({ completed: z.boolean() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});

		// Parent with 1 second timeout - let error throw
		const timeoutParentTask = conductor.createTask(
			{ name: "timeout-parent-2" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.invoke(
					"invoke-slow",
					{ name: "slow-child-2", queue: "pending-child-queue" },
					{},
					1000,
				);
				return { success: true };
			},
		);

		await conductor.ensureInstalled();
		await db.sql`
			insert into pgconductor._private_queues (name)
			values ('pending-child-queue')
			on conflict (name) do nothing
		`;

		const startTime = new Date("2024-01-01T00:00:00Z");
		await db.client.setFakeTime({ date: startTime });

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [timeoutParentTask],
			defaultWorker: {
				pollIntervalMs: 50,
				flushIntervalMs: 50,
			},
		});

		await orchestrator.start();

		await conductor.invoke({ name: "timeout-parent-2" }, {});

		// Wait for the parent to persist the unclaimed child execution.
		await waitForCondition(async () => {
			const [child] = await db.sql<{ pending: boolean }[]>`
				select locked_by is null as pending
				from pgconductor._private_executions
				where task_key = 'slow-child-2'
					and queue = 'pending-child-queue'
			`;
			return child?.pending === true;
		});

		// Advance time past timeout while the child is still pending.
		const afterTimeout = new Date(startTime.getTime() + 1500);
		await db.client.setFakeTime({ date: afterTimeout });
		await waitForCondition(async () => {
			const [child] = await db.sql<{ failed: boolean }[]>`
				select failed_at is not null as failed
				from pgconductor._private_executions
				where task_key = 'slow-child-2'
					and queue = 'pending-child-queue'
			`;
			return child?.failed === true;
		});
		await orchestrator.stop();
		await db.client.clearFakeTime();

		// Check that child was failed (not cancelled, since it wasn't locked)
		const children = await db.sql<
			{
				cancelled: boolean;
				failed_at: Date | null;
				last_error: string | null;
			}[]
		>`
			select cancelled, failed_at, last_error
			from pgconductor._private_executions
			where task_key = 'slow-child-2'
				and queue = 'pending-child-queue'
		`;

		expect(children.length).toBe(1);
		expect(children[0]?.cancelled).toBe(false); // Not cancelled since it was pending
		expect(children[0]?.failed_at).not.toBeNull();
		expect(children[0]?.last_error).toContain("parent timed out");
	}, 10000);

	test("dedupe_key replaces unlocked execution with new values", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({
			name: "dedupe-task",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		await conductor.ensureInstalled();

		// First invocation with dedupe_key
		const id1 = await conductor.invoke(
			{ name: "dedupe-task" },
			{ value: 1 },
			{ dedupe_key: "unique-key-1" },
		);

		// Second invocation with same dedupe_key should replace (update)
		const id2 = await conductor.invoke(
			{ name: "dedupe-task" },
			{ value: 2 },
			{ dedupe_key: "unique-key-1", priority: 10 },
		);

		// Should return same ID
		expect(id2).toBe(id1);

		// Check database - should have only one execution with updated values
		const executions = await db.sql<
			{
				id: string;
				payload: { value: number };
				priority: number;
			}[]
		>`
			select id, payload, priority
			from pgconductor._private_executions
			where task_key = 'dedupe-task'
		`;

		expect(executions.length).toBe(1);
		expect(executions[0]?.id).toBe(id1);
		expect(executions[0]?.payload.value).toBe(2);
		expect(executions[0]?.priority).toBe(10);
	}, 10000);

	test("dedupe_key creates new execution when existing one is locked", async () => {
		const db = await pool.child();
		databases.push(db);

		let runCount = 0;

		const taskDef = defineTask({
			name: "locked-dedupe-task",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "locked-dedupe-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					runCount++;
					// Simulate slow task
					await new Promise((r) => setTimeout(r, 500));
				}
			},
		);

		const orchestrator = Orchestrator.create({
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			conductor,
			tasks: [task],
		});

		await orchestrator.start();

		// First invocation
		const id1 = await conductor.invoke(
			{ name: "locked-dedupe-task" },
			{ value: 1 },
			{ dedupe_key: "locked-key" },
		);

		await waitForCondition(async () => {
			const [execution] = await db.sql<{ locked: boolean }[]>`
				select locked_by is not null as locked
				from pgconductor._private_executions
				where id = ${id1}
			`;
			return execution?.locked === true;
		});

		// Second invocation while first is locked - should create NEW execution
		const id2 = await conductor.invoke(
			{ name: "locked-dedupe-task" },
			{ value: 2 },
			{ dedupe_key: "locked-key" },
		);

		// Should be different IDs
		expect(id2).not.toBe(id1);

		await waitForCondition(async () => {
			const [execution] = await db.sql<{ completed: boolean }[]>`
				select completed_at is not null as completed
				from pgconductor._private_executions
				where id = ${id2}
			`;
			return execution?.completed === true;
		});
		await orchestrator.stop();

		// First execution should be marked as failed (superseded)
		const exec1 = await db.sql<
			{
				id: string;
				failed_at: Date | null;
				last_error: string | null;
				dedupe_key: string | null;
			}[]
		>`
			select id, failed_at, last_error, dedupe_key
			from pgconductor._private_executions
			where id = ${id1}
		`;

		expect(exec1.length).toBe(1);
		expect(exec1[0]?.failed_at).not.toBeNull();
		expect(exec1[0]?.last_error).toBe("superseded by reinvoke");
		expect(exec1[0]?.dedupe_key).toBeNull();

		// Second execution should have completed successfully
		const exec2 = await db.sql<
			{
				id: string;
				completed_at: Date | null;
				payload: { value: number };
			}[]
		>`
			select id, completed_at, payload
			from pgconductor._private_executions
			where id = ${id2}
		`;

		expect(exec2.length).toBe(1);
		expect(exec2[0]?.completed_at).not.toBeNull();
		expect(exec2[0]?.payload.value).toBe(2);

		// Should have run twice (first was interrupted, second completed)
		expect(runCount).toBe(2);
	}, 10000);

	test("dedupe_key debounce pattern - multiple rapid invocations", async () => {
		const db = await pool.child();
		databases.push(db);

		let executionCount = 0;
		let lastValue = 0;

		const taskDef = defineTask({
			name: "debounce-task",
			payload: z.object({ value: z.number() }),
		});

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const task = conductor.createTask(
			{ name: "debounce-task" },
			{ invocable: true },
			async (event, _ctx) => {
				if (event.name === "pgconductor.invoke") {
					executionCount++;
					lastValue = event.payload.value;
				}
			},
		);

		const startTime = new Date("2024-01-01T00:00:00Z");
		await db.client.setFakeTime({ date: startTime });

		const orchestrator = Orchestrator.create({
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
			conductor,
			tasks: [task],
		});

		await orchestrator.start();

		// Rapid invocations with same dedupe_key and delayed run_at
		const futureTime = new Date(startTime.getTime() + 1000);

		const id1 = await conductor.invoke(
			{ name: "debounce-task" },
			{ value: 1 },
			{ dedupe_key: "debounce-1", run_at: futureTime },
		);

		const id2 = await conductor.invoke(
			{ name: "debounce-task" },
			{ value: 2 },
			{ dedupe_key: "debounce-1", run_at: futureTime },
		);

		const id3 = await conductor.invoke(
			{ name: "debounce-task" },
			{ value: 3 },
			{ dedupe_key: "debounce-1", run_at: futureTime },
		);

		// All should return same ID (execution was replaced)
		expect(id2).toBe(id1);
		expect(id3).toBe(id1);

		// Should have only one execution in database
		const executions = await db.sql<{ count: number }[]>`
			select count(*)::int as count
			from pgconductor._private_executions
			where task_key = 'debounce-task'
		`;
		expect(executions[0]?.count).toBe(1);

		await db.client.setFakeTime({ date: futureTime });
		await waitForCondition(async () => executionCount === 1);
		await orchestrator.stop();
		await db.client.clearFakeTime();

		// Should have executed only once with the last value
		expect(executionCount).toBe(1);
		expect(lastValue).toBe(3);
	}, 10000);
});
