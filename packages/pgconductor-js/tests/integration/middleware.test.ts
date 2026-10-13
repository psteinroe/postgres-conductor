import { AsyncLocalStorage } from "node:async_hooks";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineEvent } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import type { MiddlewareExecution } from "../../src/middleware";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const approval = defineEvent({ name: "middleware.approval", payload: z.object({}) });
const parentDefinition = defineTask({ name: "middleware.parent", payload: z.object({}) });
const childDefinition = defineTask({ name: "middleware.child", payload: z.object({}) });
const batchDefinition = defineTask({ name: "middleware.batch", payload: z.object({}) });

describe("Middleware", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const orchestrators: Orchestrator[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		await Promise.allSettled(orchestrators.map((orchestrator) => orchestrator.stop()));
		orchestrators.length = 0;
		await Promise.all(databases.map((db) => db.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	async function database() {
		const db = await pool.child();
		databases.push(db);
		await Conductor.create({ sql: db.sql, context: {} }).ensureInstalled();
		return db;
	}

	async function completed(db: TestDatabase, taskKey: string) {
		await waitForCondition(async () => {
			const [row] = await db.sql<{ done: boolean }[]>`
				select exists (
					select 1 from pgconductor._private_executions
					where task_key = ${taskKey} and completed_at is not null
				) as done
			`;
			return row?.done === true;
		});
	}

	test("runs once per attempt, including after sleep, event and child resumes", async () => {
		const db = await database();
		const runs: MiddlewareExecution[] = [];

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			events: EventSchemas.fromSchema([approval]),
			context: {},
			middleware: [
				async ({ execution, ctx }, next) => {
					runs.push(execution);
					return next(ctx);
				},
			],
		});
		const parent = conductor.createTask(
			{ name: "middleware.parent" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.sleep("nap", 1);
				await ctx.waitForEvent("approval", { event: approval });
				await ctx.invoke("child", { name: "middleware.child" }, {});
			},
		);
		const child = conductor.createTask(
			{ name: "middleware.child" },
			{ invocable: true },
			async () => {},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parent, child],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		orchestrators.push(orchestrator);
		await orchestrator.start();

		const parentId = await conductor.invoke(
			{ name: "middleware.parent" },
			{},
			{ metadata: { tenant: "acme" } },
		);
		await waitForCondition(async () => {
			const [row] = await db.sql<{ waiting: boolean }[]>`
				select exists (
					select 1 from pgconductor._private_custom_event_subscriptions
					where kind = 'execution_wait'
				) as waiting
			`;
			return row?.waiting === true;
		});
		await conductor.emit("middleware.approval", {});
		await completed(db, "middleware.parent");

		expect(
			runs.map(({ task_key, resumed, parent_execution_id }) => ({
				task_key,
				resumed,
				parent_execution_id,
			})),
		).toEqual([
			{ task_key: "middleware.parent", resumed: false, parent_execution_id: null },
			{ task_key: "middleware.parent", resumed: true, parent_execution_id: null },
			{ task_key: "middleware.parent", resumed: true, parent_execution_id: null },
			{ task_key: "middleware.child", resumed: false, parent_execution_id: parentId },
			{ task_key: "middleware.parent", resumed: true, parent_execution_id: null },
		]);
		expect(runs.map((run) => run.metadata)).toEqual(Array(5).fill({ tenant: "acme" }));
		expect(runs.filter((run) => run.task_key === "middleware.parent")[0]).toMatchObject({
			id: parentId,
			queue: "default",
			attempt: 1,
		});
	}, 30000);

	test("adds context and wraps every attempt", async () => {
		const db = await database();
		const storage = new AsyncLocalStorage<string>();
		const seen: { store: string | undefined; label: string }[] = [];
		const finished: string[] = [];

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition]),
			context: {},
			middleware: [
				async ({ execution, ctx }, next) => {
					try {
						return await storage.run(execution.id, () =>
							next({ ...ctx, label: execution.resumed ? "resumed" : "started" }),
						);
					} finally {
						finished.push(execution.id);
					}
				},
			],
		});
		const parent = conductor.createTask(
			{ name: "middleware.parent" },
			{ invocable: true },
			async (_event, ctx) => {
				seen.push({ store: storage.getStore(), label: ctx.label });
				await ctx.sleep("nap", 1);
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parent],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		orchestrators.push(orchestrator);
		await orchestrator.start();

		const id = await conductor.invoke({ name: "middleware.parent" }, {});
		await completed(db, "middleware.parent");

		expect(seen).toEqual([
			{ store: id, label: "started" },
			{ store: id, label: "resumed" },
		]);
		expect(finished).toEqual([id, id]);
	}, 30000);

	test("a middleware error fails the attempt and retries", async () => {
		const db = await database();
		const attempts: number[] = [];
		let handled = 0;

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition]),
			context: {},
			middleware: [
				async ({ execution, ctx }, next) => {
					attempts.push(execution.attempt);
					if (execution.attempt === 1) throw new Error("middleware failed");
					return next(ctx);
				},
			],
		});
		const parent = conductor.createTask(
			{ name: "middleware.parent" },
			{ invocable: true },
			async () => {
				handled++;
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parent],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		orchestrators.push(orchestrator);
		await orchestrator.start();

		await conductor.invoke({ name: "middleware.parent" }, {});
		let retryAt: Date | undefined;
		await waitForCondition(async () => {
			const [row] = await db.sql<{ last_error: string | null; run_at: Date }[]>`
				select last_error, run_at from pgconductor._private_executions
				where task_key = 'middleware.parent' and locked_by is null
			`;
			retryAt = row?.run_at;
			return row?.last_error === "middleware failed";
		});
		expect(handled).toBe(0);

		await db.client.setFakeTime({ date: new Date((retryAt?.getTime() || 0) + 1) });
		await completed(db, "middleware.parent");
		await db.client.clearFakeTime();

		expect(attempts).toEqual([1, 2]);
		expect(handled).toBe(1);
	}, 30000);

	test("a middleware that swallows errors does not break suspension", async () => {
		const db = await database();
		const caught: unknown[] = [];
		let handled = 0;

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([parentDefinition]),
			context: {},
			middleware: [
				async ({ ctx }, next) => {
					try {
						return await next(ctx);
					} catch (error) {
						caught.push(error);
						return undefined as never;
					}
				},
			],
		});
		const parent = conductor.createTask(
			{ name: "middleware.parent" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.sleep("nap", 1);
				handled++;
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parent],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		orchestrators.push(orchestrator);
		await orchestrator.start();

		await conductor.invoke({ name: "middleware.parent" }, {});
		await completed(db, "middleware.parent");

		expect(caught).toEqual([]);
		expect(handled).toBe(1);
	}, 30000);

	test("skips internal and batch tasks", async () => {
		const db = await database();
		const runs: string[] = [];
		let batched = 0;

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([batchDefinition]),
			events: EventSchemas.fromSchema([approval]),
			context: {},
			middleware: [
				async ({ execution, ctx }, next) => {
					runs.push(execution.task_key);
					return next(ctx);
				},
			],
		});
		const batch = conductor.createTask(
			{ name: "middleware.batch", batch: { size: 2, timeoutMs: 10 } },
			{ invocable: true },
			async (events) => {
				batched += events.length;
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [batch],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		orchestrators.push(orchestrator);
		await orchestrator.start();

		await conductor.invoke({ name: "middleware.batch" }, {});
		await conductor.emit("middleware.approval", {});
		// @ts-expect-error - maintenance task is not in conductor's task registry
		await conductor.invoke({ name: "pgconductor.maintenance" }, {});
		await completed(db, "middleware.batch");
		await completed(db, "pgconductor.maintenance");
		await waitForCondition(async () => {
			const [row] = await db.sql<{ pending: number }[]>`
				select count(*)::int as pending from pgconductor._private_executions
				where task_key = 'pgconductor.event-dispatch' and completed_at is null
			`;
			return row?.pending === 0;
		});

		expect(batched).toBe(1);
		expect(runs).toEqual([]);
	}, 30000);
});
