import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

async function eventually(check: () => Promise<boolean>, timeoutMs = 10_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await check()) return;
		await sleep(10);
	}
	throw new Error("condition was not met before the test timeout");
}

async function owner(db: TestDatabase, taskKey: string, queue = "default") {
	const rows = await db.sql<{ execution_id: string }[]>`
		select execution_id
		from pgconductor._private_fifo_owners
		where task_key = ${taskKey} and queue = ${queue}
	`;
	return rows[0]?.execution_id;
}

const payloadDefinition = <const Name extends string>(name: Name) =>
	defineTask({ name, payload: z.object({ id: z.number() }) });

describe("FIFO task execution", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60_000);

	afterEach(async () => {
		await Promise.all(databases.map((db) => db.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	test("executes runnable work strictly in enqueue order with concurrent workers", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = payloadDefinition("fifo-order");
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const order: number[] = [];
		const starts = Array.from({ length: 5 }, () => new Deferred<void>());
		const releases = Array.from({ length: 5 }, () => new Deferred<void>());
		const task = conductor.createTask(
			{ name: "fifo-order", fifo: true },
			{ invocable: true },
			async (event) => {
				const id = event.payload.id;
				order.push(id);
				starts[id]?.resolve();
				await releases[id]!.promise;
			},
		);
		await conductor.ensureInstalled();
		for (let id = 0; id < 5; id++) await conductor.invoke({ name: "fifo-order" }, { id });

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { concurrency: 5, fetchBatchSize: 2, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await starts[0]!.promise;
		expect(order).toEqual([0]);
		for (let id = 0; id < 5; id++) {
			releases[id]!.resolve();
			if (id < 4) {
				await starts[id + 1]!.promise;
				expect(order).toEqual(Array.from({ length: id + 2 }, (_, i) => i));
			}
		}
		await orchestrator.stop();
	}, 30_000);

	test("does not let a future, never-started execution block ready work", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = payloadDefinition("fifo-future");
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const readyStarted = new Deferred<void>();
		const readyRelease = new Deferred<void>();
		const order: number[] = [];
		const task = conductor.createTask(
			{ name: "fifo-future", fifo: true },
			{ invocable: true },
			async (event) => {
				order.push(event.payload.id);
				if (event.payload.id === 2) {
					readyStarted.resolve();
					await readyRelease.promise;
				}
			},
		);
		await conductor.ensureInstalled();
		const future = await conductor.invoke(
			{ name: "fifo-future" },
			{ id: 1 },
			{ run_at: new Date(Date.now() + 3_600_000) },
		);
		await conductor.invoke({ name: "fifo-future" }, { id: 2 });
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { concurrency: 3, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await readyStarted.promise;
		expect(order).toEqual([2]);
		expect(await owner(db, "fifo-future")).not.toBe(future);
		readyRelease.resolve();
		await orchestrator.stop();
	}, 30_000);

	test("ignores priority within a FIFO lane", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = payloadDefinition("fifo-priority");
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const starts = [new Deferred<void>(), new Deferred<void>()];
		const releases = [new Deferred<void>(), new Deferred<void>()];
		const order: number[] = [];
		const task = conductor.createTask(
			{ name: "fifo-priority", fifo: true },
			{ invocable: true },
			async (event) => {
				const id = event.payload.id;
				order.push(id);
				starts[id]!.resolve();
				await releases[id]!.promise;
			},
		);
		await conductor.ensureInstalled();
		await conductor.invoke({ name: "fifo-priority" }, { id: 0 }, { priority: 100 });
		await conductor.invoke({ name: "fifo-priority" }, { id: 1 }, { priority: -100 });
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { concurrency: 2, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await starts[0]!.promise;
		expect(order).toEqual([0]);
		releases[0]!.resolve();
		await starts[1]!.promise;
		expect(order).toEqual([0, 1]);
		releases[1]!.resolve();
		await orchestrator.stop();
	}, 30_000);

	test("retains its owner over retry backoff", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = payloadDefinition("fifo-retry");
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const firstFailed = new Deferred<void>();
		const retryStarted = new Deferred<void>();
		const retryRelease = new Deferred<void>();
		const successorStarted = new Deferred<void>();
		let calls = 0;
		const task = conductor.createTask(
			{ name: "fifo-retry", fifo: true },
			{ invocable: true },
			async (event) => {
				if (event.payload.id === 0) {
					if (++calls === 1) {
						firstFailed.resolve();
						throw new Error("retry me");
					}
					retryStarted.resolve();
					await retryRelease.promise;
				} else successorStarted.resolve();
			},
		);
		await conductor.ensureInstalled();
		const now = new Date("2024-01-01T00:00:00Z");
		await db.client.setFakeTime({ date: now });
		const first = await conductor.invoke({ name: "fifo-retry" }, { id: 0 });
		const successor = await conductor.invoke({ name: "fifo-retry" }, { id: 1 });
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { concurrency: 2, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await firstFailed.promise;
		await eventually(
			async () =>
				(await owner(db, "fifo-retry")) === first &&
				(
					await db.sql<
						{ locked_at: Date | null }[]
					>`select locked_at from pgconductor._private_executions where id = ${first}::uuid`
				)[0]?.locked_at === null,
		);
		expect(successorStarted.isSettled).toBe(false);
		const retryAt = (
			await db.sql<
				{ run_at: Date }[]
			>`select run_at from pgconductor._private_executions where id = ${first}::uuid`
		)[0]!.run_at;
		await db.client.setFakeTime({ date: new Date(retryAt.getTime() + 1) });
		await retryStarted.promise;
		expect(await owner(db, "fifo-retry")).toBe(first);
		retryRelease.resolve();
		await successorStarted.promise;
		expect(await owner(db, "fifo-retry")).toBe(successor);
		await orchestrator.stop();
	}, 30_000);

	test("retains its owner over ctx.sleep", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = payloadDefinition("fifo-sleep");
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const sleeping = new Deferred<void>();
		const successorStarted = new Deferred<void>();
		const task = conductor.createTask(
			{ name: "fifo-sleep", fifo: true },
			{ invocable: true },
			async (event, ctx) => {
				if (event.payload.id === 0) {
					sleeping.resolve();
					await ctx.sleep("wait", 3_600_000);
				} else successorStarted.resolve();
			},
		);
		await conductor.ensureInstalled();
		await db.client.setFakeTime({ date: new Date("2024-01-01T00:00:00Z") });
		const first = await conductor.invoke({ name: "fifo-sleep" }, { id: 0 });
		const successor = await conductor.invoke({ name: "fifo-sleep" }, { id: 1 });
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { concurrency: 2, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await sleeping.promise;
		await eventually(async () => (await owner(db, "fifo-sleep")) === first);
		expect(successorStarted.isSettled).toBe(false);
		expect(await owner(db, "fifo-sleep")).toBe(first);
		await orchestrator.stop();
	}, 30_000);

	test("retains its owner while waiting on a child", async () => {
		const db = await pool.child();
		databases.push(db);
		const parentDefinition = defineTask({ name: "fifo-parent" });
		const childDefinition = defineTask({ name: "fifo-child" });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			context: {},
		});
		const childStarted = new Deferred<void>();
		const childRelease = new Deferred<void>();
		const parentResumed = new Deferred<void>();
		const successorStarted = new Deferred<void>();
		const child = conductor.createTask({ name: "fifo-child" }, { invocable: true }, async () => {
			childStarted.resolve();
			await childRelease.promise;
		});
		const parent = conductor.createTask(
			{ name: "fifo-parent", fifo: true },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.invoke("child", { name: "fifo-child" }, {});
				parentResumed.resolve();
			},
		);
		await conductor.ensureInstalled();
		const first = await conductor.invoke({ name: "fifo-parent" }, {});
		const successor = await conductor.invoke({ name: "fifo-parent" }, {});
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parent, child],
			defaultWorker: { concurrency: 3, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await childStarted.promise;
		await eventually(
			async () =>
				(await owner(db, "fifo-parent")) === first &&
				(
					await db.sql<
						{ waiting_on_execution_id: string | null }[]
					>`select waiting_on_execution_id from pgconductor._private_executions where id = ${first}::uuid`
				)[0]?.waiting_on_execution_id !== null,
		);
		expect(successorStarted.isSettled).toBe(false);
		childRelease.resolve();
		await parentResumed.promise;
		await eventually(async () => (await owner(db, "fifo-parent")) === successor);
		await orchestrator.stop();
	}, 30_000);

	test("releases its owner on cancellation and permanent failure", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = payloadDefinition("fifo-release");
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const sleeping = new Deferred<void>();
		const cancelledSuccessor = new Deferred<void>();
		const task = conductor.createTask(
			{ name: "fifo-release", fifo: true, maxAttempts: 1 },
			{ invocable: true },
			async (event, ctx) => {
				if (event.payload.id === 0) {
					sleeping.resolve();
					await ctx.sleep("cancel-me", 3_600_000);
				} else if (event.payload.id === 1) cancelledSuccessor.resolve();
				else throw new Error("permanent failure");
			},
		);
		await conductor.ensureInstalled();
		await db.client.setFakeTime({ date: new Date("2024-01-01T00:00:00Z") });
		const first = await conductor.invoke({ name: "fifo-release" }, { id: 0 });
		const second = await conductor.invoke({ name: "fifo-release" }, { id: 1 });
		const third = await conductor.invoke({ name: "fifo-release" }, { id: 2 });
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { concurrency: 3, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await sleeping.promise;
		await eventually(async () => (await owner(db, "fifo-release")) === first);
		await db.client.cancelExecution(first);
		await cancelledSuccessor.promise;
		expect(await owner(db, "fifo-release")).toBe(second);
		await eventually(
			async () =>
				(
					await db.sql<
						{ failed_at: Date | null }[]
					>`select failed_at from pgconductor._private_executions where id = ${first}::uuid`
				)[0]?.failed_at !== null,
		);
		await eventually(
			async () =>
				(
					await db.sql<
						{ failed_at: Date | null }[]
					>`select failed_at from pgconductor._private_executions where id = ${third}::uuid`
				)[0]?.failed_at !== null,
		);
		await eventually(async () => (await owner(db, "fifo-release")) === undefined);
		await orchestrator.stop();
	}, 30_000);

	test("stale recovery leaves the owner to be resumed before its successor", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = payloadDefinition("fifo-stale");
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const starts: Deferred<void>[] = [new Deferred(), new Deferred()];
		const secondWorkerRelease = new Deferred<void>();
		const successorStarted = new Deferred<void>();
		let startCount = 0;
		const task = conductor.createTask(
			{ name: "fifo-stale", fifo: true },
			{ invocable: true },
			async (event) => {
				if (event.payload.id === 0) {
					const index = startCount++;
					starts[index]!.resolve();
					if (index === 1) await secondWorkerRelease.promise;
				} else successorStarted.resolve();
			},
		);
		await conductor.ensureInstalled();
		const first = await conductor.invoke({ name: "fifo-stale" }, { id: 0 });
		const successor = await conductor.invoke({ name: "fifo-stale" }, { id: 1 });
		const orchestrator1 = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { concurrency: 2, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator1.start();
		await starts[0]!.promise;
		const oldOrchestrator = (
			await db.sql<
				{ locked_by: string }[]
			>`select locked_by from pgconductor._private_executions where id = ${first}::uuid`
		)[0]!.locked_by;
		await db.client.setFakeTime({ date: new Date("2027-01-01T00:00:00Z") });
		await db.client.recoverStaleOrchestrators({ maxAge: "0 milliseconds" });
		expect(await owner(db, "fifo-stale")).toBe(first);
		expect(
			(
				await db.sql<
					{ locked_by: string | null }[]
				>`select locked_by from pgconductor._private_executions where id = ${first}::uuid`
			)[0]?.locked_by,
		).toBeNull();
		await orchestrator1.stop();
		const orchestrator2 = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { concurrency: 2, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator2.start();
		await starts[1]!.promise;
		expect(await owner(db, "fifo-stale")).toBe(first);
		expect(successorStarted.isSettled).toBe(false);
		expect(oldOrchestrator).toBeTruthy();
		secondWorkerRelease.resolve();
		await successorStarted.promise;
		expect(await owner(db, "fifo-stale")).toBe(successor);
		await orchestrator2.stop();
	}, 30_000);

	test("allows the same task key in different queues to proceed independently", async () => {
		const db = await pool.child();
		databases.push(db);
		const a = defineTask({ name: "same-key", queue: "queue-a" });
		const b = defineTask({ name: "same-key", queue: "queue-b" });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([a, b]),
			context: {},
		});
		const startedA = new Deferred<void>();
		const startedB = new Deferred<void>();
		const release = new Deferred<void>();
		const taskA = conductor.createTask(
			{ name: "same-key", queue: "queue-a", fifo: true },
			{ invocable: true },
			async () => {
				startedA.resolve();
				await release.promise;
			},
		);
		const taskB = conductor.createTask(
			{ name: "same-key", queue: "queue-b", fifo: true },
			{ invocable: true },
			async () => {
				startedB.resolve();
				await release.promise;
			},
		);
		const workerA = conductor.createWorker({
			queue: "queue-a",
			tasks: [taskA],
			config: { concurrency: 1, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		const workerB = conductor.createWorker({
			queue: "queue-b",
			tasks: [taskB],
			config: { concurrency: 1, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await conductor.ensureInstalled();
		await conductor.invoke({ name: "same-key", queue: "queue-a" }, {});
		await conductor.invoke({ name: "same-key", queue: "queue-b" }, {});
		const orchestrator = Orchestrator.create({ conductor, workers: [workerA, workerB] });
		await orchestrator.start();
		await Promise.all([startedA.promise, startedB.promise]);
		release.resolve();
		await orchestrator.stop();
	}, 30_000);

	test("allows different FIFO task keys to proceed independently", async () => {
		const db = await pool.child();
		databases.push(db);
		const a = defineTask({ name: "fifo-a" });
		const b = defineTask({ name: "fifo-b" });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([a, b]),
			context: {},
		});
		const startedA = new Deferred<void>();
		const startedB = new Deferred<void>();
		const release = new Deferred<void>();
		const taskA = conductor.createTask(
			{ name: "fifo-a", fifo: true },
			{ invocable: true },
			async () => {
				startedA.resolve();
				await release.promise;
			},
		);
		const taskB = conductor.createTask(
			{ name: "fifo-b", fifo: true },
			{ invocable: true },
			async () => {
				startedB.resolve();
				await release.promise;
			},
		);
		await conductor.ensureInstalled();
		await conductor.invoke({ name: "fifo-a" }, {});
		await conductor.invoke({ name: "fifo-b" }, {});
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [taskA, taskB],
			defaultWorker: { concurrency: 2, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await Promise.all([startedA.promise, startedB.promise]);
		release.resolve();
		await orchestrator.stop();
	}, 30_000);

	test("dedupe supersession transfers the lane instead of stranding its owner", async () => {
		const db = await pool.child();
		databases.push(db);
		const definition = payloadDefinition("fifo-dedupe");
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const firstStarted = new Deferred<void>();
		const replacementStarted = new Deferred<void>();
		const releaseFirst = new Deferred<void>();
		const releaseReplacement = new Deferred<void>();
		const task = conductor.createTask(
			{ name: "fifo-dedupe", fifo: true },
			{ invocable: true },
			async (event) => {
				if (event.payload.id === 0) {
					firstStarted.resolve();
					await releaseFirst.promise;
				} else {
					replacementStarted.resolve();
					await releaseReplacement.promise;
				}
			},
		);
		await conductor.ensureInstalled();
		const first = await conductor.invoke(
			{ name: "fifo-dedupe" },
			{ id: 0 },
			{ dedupe_key: "same" },
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { concurrency: 2, pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await firstStarted.promise;
		const replacement = await conductor.invoke(
			{ name: "fifo-dedupe" },
			{ id: 1 },
			{ dedupe_key: "same" },
		);
		await replacementStarted.promise;
		expect(await owner(db, "fifo-dedupe")).toBe(replacement);
		expect(
			(
				await db.sql<
					{ failed_at: Date | null }[]
				>`select failed_at from pgconductor._private_executions where id = ${first}::uuid`
			)[0]?.failed_at,
		).not.toBeNull();
		releaseReplacement.resolve();
		releaseFirst.resolve();
		await eventually(async () => (await owner(db, "fifo-dedupe")) === undefined);
		await orchestrator.stop();
	}, 30_000);
});
