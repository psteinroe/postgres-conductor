import { test, expect, describe, beforeAll, afterAll, afterEach } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";

describe("Task-Level Concurrency", () => {
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

	test("concurrency=1 enforces serial execution", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({ name: "serial-task" });

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		const executionOrder: number[] = [];
		const blockers = new Map<number, Deferred<void>>();

		// create blockers for each execution
		for (let i = 0; i < 5; i++) {
			blockers.set(i, new Deferred<void>());
		}

		let executionIndex = 0;

		const serialTask = conductor.createTask(
			{ name: "serial-task", concurrency: 1 },
			{ invocable: true },
			async () => {
				const myIndex = executionIndex++;
				executionOrder.push(myIndex);

				// wait for blocker to be released
				await blockers.get(myIndex)?.promise;
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [serialTask],
			defaultWorker: {
				pollIntervalMs: 50,
				flushIntervalMs: 50,
				flushBatchSize: 1,
				concurrency: 10,
			},
		});

		await orchestrator.start();

		// queue 5 tasks
		for (let i = 0; i < 5; i++) {
			await conductor.invoke({ name: "serial-task" }, {});
		}

		// wait for first task to start
		await new Promise((r) => setTimeout(r, 200));
		expect(executionOrder).toEqual([0]);

		// release first task, second should start
		blockers.get(0)?.resolve();
		await new Promise((r) => setTimeout(r, 500));
		expect(executionOrder).toEqual([0, 1]);

		// release remaining tasks
		for (let i = 1; i < 5; i++) {
			blockers.get(i)?.resolve();
			await new Promise((r) => setTimeout(r, 200));
		}

		await orchestrator.stop();
		await orchestrator.stopped;

		// all tasks executed in order
		expect(executionOrder).toEqual([0, 1, 2, 3, 4]);
	}, 30000);

	test("concurrency=4 allows up to 4 parallel executions", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({ name: "parallel-task" });

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		let runningCount = 0;
		let maxRunningCount = 0;
		const blocker = new Deferred<void>();

		const parallelTask = conductor.createTask(
			{ name: "parallel-task", concurrency: 4 },
			{ invocable: true },
			async () => {
				runningCount++;
				maxRunningCount = Math.max(maxRunningCount, runningCount);

				// wait for blocker
				await blocker.promise;

				runningCount--;
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parallelTask],
			defaultWorker: {
				pollIntervalMs: 50,
				flushIntervalMs: 50,
				fetchBatchSize: 10,
				concurrency: 10,
			},
		});

		await orchestrator.start();

		// queue 10 tasks
		for (let i = 0; i < 10; i++) {
			await conductor.invoke({ name: "parallel-task" }, {});
		}

		// wait for tasks to start (should be 4 max)
		await new Promise((r) => setTimeout(r, 500));
		expect(maxRunningCount).toBeGreaterThan(0);
		expect(maxRunningCount).toBeLessThanOrEqual(4);

		// release all tasks
		blocker.resolve();

		await orchestrator.stop();
		await orchestrator.stopped;
	}, 30000);

	test("task without concurrency limit runs unlimited", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({ name: "unlimited-task" });

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		let runningCount = 0;
		let maxRunningCount = 0;
		const blocker = new Deferred<void>();

		const unlimitedTask = conductor.createTask(
			{ name: "unlimited-task" }, // no concurrency set
			{ invocable: true },
			async () => {
				runningCount++;
				maxRunningCount = Math.max(maxRunningCount, runningCount);

				await blocker.promise;

				runningCount--;
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [unlimitedTask],
			defaultWorker: {
				pollIntervalMs: 50,
				flushIntervalMs: 50,
				fetchBatchSize: 10,
				concurrency: 10,
			},
		});

		await orchestrator.start();

		// queue 10 tasks
		for (let i = 0; i < 10; i++) {
			await conductor.invoke({ name: "unlimited-task" }, {});
		}

		// wait for tasks to start (should be limited by worker concurrency=10)
		await new Promise((r) => setTimeout(r, 500));
		expect(maxRunningCount).toBeGreaterThan(0);
		expect(maxRunningCount).toBeLessThanOrEqual(10);

		blocker.resolve();

		await orchestrator.stop();
		await orchestrator.stopped;
	}, 30000);

	test("high concurrency uses slot groups correctly", async () => {
		const db = await pool.child();
		databases.push(db);

		const taskDef = defineTask({ name: "high-concurrency-task" });

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDef]),
			context: {},
		});

		let completed = 0;

		const highConcurrencyTask = conductor.createTask(
			{ name: "high-concurrency-task", concurrency: 500 },
			{ invocable: true },
			async () => {
				completed++;
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [highConcurrencyTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50, concurrency: 50 },
		});

		await orchestrator.start();

		// queue 100 tasks
		for (let i = 0; i < 100; i++) {
			await conductor.invoke({ name: "high-concurrency-task" }, {});
		}

		// wait for completion
		await new Promise((r) => setTimeout(r, 5000));

		await orchestrator.stop();
		await orchestrator.stopped;

		// extra wait to ensure all flushes complete
		await new Promise((r) => setTimeout(r, 1000));

		// all tasks should complete
		expect(completed).toBe(100);

		// verify slots were created (one row per slot, concurrency=500 → 500 rows)
		const slots = await db.sql`
			select * from pgconductor._private_concurrency_slots
			where task_key = 'high-concurrency-task'
		`;

		expect(slots.length).toBe(500); // one row per slot
		expect(slots.every((s) => s.capacity === 1)).toBe(true); // each slot has capacity=1

		// verify all slots are released (used = 0)
		for (const slot of slots) {
			expect(slot.used).toBe(0);
		}
	}, 30000);

	test("mixed queue with concurrency and without", async () => {
		const db = await pool.child();
		databases.push(db);

		const limitedDef = defineTask({ name: "limited-task" });
		const unlimitedDef = defineTask({ name: "unlimited-task" });

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([limitedDef, unlimitedDef]),
			context: {},
		});

		let limitedRunning = 0;
		let limitedMaxRunning = 0;
		let unlimitedRunning = 0;
		let unlimitedMaxRunning = 0;

		const limitedBlocker = new Deferred<void>();
		const unlimitedBlocker = new Deferred<void>();

		const limitedTask = conductor.createTask(
			{ name: "limited-task", concurrency: 2 },
			{ invocable: true },
			async () => {
				limitedRunning++;
				limitedMaxRunning = Math.max(limitedMaxRunning, limitedRunning);
				await limitedBlocker.promise;
				limitedRunning--;
			},
		);

		const unlimitedTask = conductor.createTask(
			{ name: "unlimited-task" },
			{ invocable: true },
			async () => {
				unlimitedRunning++;
				unlimitedMaxRunning = Math.max(unlimitedMaxRunning, unlimitedRunning);
				await unlimitedBlocker.promise;
				unlimitedRunning--;
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [limitedTask, unlimitedTask],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50, concurrency: 10 },
		});

		await orchestrator.start();

		// queue 5 of each
		for (let i = 0; i < 5; i++) {
			await conductor.invoke({ name: "limited-task" }, {});
			await conductor.invoke({ name: "unlimited-task" }, {});
		}

		// wait for tasks to start
		await new Promise((r) => setTimeout(r, 500));

		// limited should have max 2, unlimited can use remaining worker slots
		expect(limitedMaxRunning).toBeGreaterThan(0);
		expect(limitedMaxRunning).toBeLessThanOrEqual(2);
		expect(unlimitedMaxRunning).toBeGreaterThan(0);

		limitedBlocker.resolve();
		unlimitedBlocker.resolve();

		await orchestrator.stop();
		await orchestrator.stopped;
	}, 30000);
});
