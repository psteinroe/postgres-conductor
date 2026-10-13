import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { loseFirstResponse, waitForCondition } from "../test-utils";

const metadataSchema = z.object({ tenant: z.string(), replyTo: z.string().optional() });

describe("ctx.start (Postgres integration)", () => {
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

	async function child() {
		const db = await pool.child();
		databases.push(db);
		return db;
	}

	const config = { pollIntervalMs: 10, flushIntervalMs: 10 };

	test("the starter completes without waiting for the started execution", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "starter", returns: z.object({ id: z.string() }) }),
				defineTask({
					name: "work",
					payload: z.object({ value: z.number() }),
					returns: z.object({ doubled: z.number() }),
				}),
			]),
			context: {},
		});
		const release = new Deferred<void>();
		const work = conductor.createTask({ name: "work" }, { invocable: true }, async (event) => {
			await release.promise;
			return { doubled: event.payload.value * 2 };
		});
		const starter = conductor.createTask(
			{ name: "starter" },
			{ invocable: true },
			async (_e, ctx) => {
				const id = await ctx.start(
					"start-work",
					{ name: "work" },
					{ value: 2 },
					{ priority: 5, group: "g1" },
				);
				return { id };
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [starter, work],
			defaultWorker: config,
		});

		await orchestrator.start();
		const starterId = await conductor.invoke({ name: "starter" }, {});
		const { id } = await conductor.waitForResult({ name: "starter" }, starterId, {
			pollIntervalMs: 10,
		});
		expect((await conductor.getExecution(id))?.status).not.toBe("completed");

		release.resolve();
		const result = await conductor.waitForResult({ name: "work" }, id, { pollIntervalMs: 10 });
		await orchestrator.stop();

		expect(result).toEqual({ doubled: 4 });
		const [row] = await db.sql<
			{ parent_execution_id: string | null; priority: number; group: string | null }[]
		>`
			select parent_execution_id, priority, "group" from pgconductor._private_executions
			where id = ${id}
		`;
		expect(row).toEqual({ parent_execution_id: null, priority: 5, group: "g1" });
	});

	test("retries and resumes return the same execution, also after a crash before the step was saved", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "starter" }),
				defineTask({ name: "work" }),
			]),
			context: {},
		});
		const ids: string[] = [];
		const work = conductor.createTask({ name: "work" }, { invocable: true }, async () => {});
		const starter = conductor.createTask(
			{ name: "starter", maxAttempts: 2 },
			{ invocable: true },
			async (_e, ctx) => {
				ids.push(await ctx.start("start-work", { name: "work" }, {}));
				await ctx.sleep("pause", 0);
				if (ids.length === 2) {
					await db.sql`delete from pgconductor._private_steps where key = 'start-work'`;
					throw new Error("crash");
				}
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [starter, work],
			defaultWorker: config,
		});

		await db.client.setFakeTime({ date: new Date("2024-01-01T12:00:00Z") });
		await orchestrator.start();
		const starterId = await conductor.invoke({ name: "starter" }, {});
		let retryAt: Date | undefined;
		await waitForCondition(async () => {
			const [row] = await db.sql<{ run_at: Date }[]>`
				select run_at from pgconductor._private_executions
				where task_key = 'starter' and last_error is not null and locked_by is null
			`;
			retryAt = row?.run_at;
			return retryAt !== undefined;
		});
		await db.client.setFakeTime({ date: new Date((retryAt?.getTime() || 0) + 1) });
		await conductor.waitForResult(starterId, { pollIntervalMs: 10 });
		await orchestrator.stop();
		await db.client.clearFakeTime();

		expect(ids).toHaveLength(3);
		expect(new Set(ids).size).toBe(1);
		const rows =
			await db.sql`select id from pgconductor._private_executions where task_key = 'work'`;
		expect(rows).toHaveLength(1);
	});

	test("a lost response is retried without starting a second execution", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: loseFirstResponse(db.sql, "started_execution"),
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "starter", returns: z.object({ id: z.string() }) }),
				defineTask({ name: "work" }),
			]),
			context: {},
		});
		const work = conductor.createTask({ name: "work" }, { invocable: true }, async () => {});
		const starter = conductor.createTask(
			{ name: "starter" },
			{ invocable: true },
			async (_e, ctx) => {
				return { id: await ctx.start("start-work", { name: "work" }, {}) };
			},
		);
		const orchestrator = Orchestrator.create({ conductor, tasks: [starter, work] });

		await conductor.ensureInstalled();
		const starterId = await conductor.invoke({ name: "starter" }, {});
		await orchestrator.drain();

		const rows = await db.sql<{ id: string }[]>`
			select id from pgconductor._private_executions where task_key = 'work'
		`;
		expect(rows).toHaveLength(1);
		expect(await conductor.getExecution({ name: "starter" }, starterId)).toMatchObject({
			status: "completed",
			result: { id: rows[0]?.id },
		});
	});

	test("cancelling the starter does not cancel the started execution", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "starter" }),
				defineTask({ name: "work", returns: z.object({ done: z.boolean() }) }),
			]),
			context: {},
		});
		const release = new Deferred<void>();
		let workId: string | undefined;
		const work = conductor.createTask({ name: "work" }, { invocable: true }, async () => {
			await release.promise;
			return { done: true };
		});
		const starter = conductor.createTask(
			{ name: "starter" },
			{ invocable: true },
			async (_e, ctx) => {
				workId = await ctx.start("start-work", { name: "work" }, {});
				await ctx.sleep("wait", 60 * 60 * 1000);
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [starter, work],
			defaultWorker: config,
		});

		await orchestrator.start();
		const starterId = await conductor.invoke({ name: "starter" }, {});
		await waitForCondition(async () => {
			const [row] = await db.sql<{ run_at: Date }[]>`
				select run_at from pgconductor._private_executions
				where id = ${starterId} and locked_by is null
			`;
			return row !== undefined && row.run_at.getTime() > Date.now() + 60_000;
		});
		expect(await conductor.cancel(starterId)).toBe(true);
		release.resolve();
		const result = await conductor.waitForResult({ name: "work" }, workId || "", {
			pollIntervalMs: 10,
		});
		await orchestrator.stop();

		expect(result).toEqual({ done: true });
	});

	test("the started execution inherits metadata, and an override applies to it only", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "starter" }),
				defineTask({ name: "work", payload: z.object({ label: z.string() }) }),
			]),
			metadata: metadataSchema,
			context: {},
		});
		const seen: Record<string, unknown> = {};
		const work = conductor.createTask({ name: "work" }, { invocable: true }, async (event, ctx) => {
			seen[event.payload.label] = ctx.metadata;
		});
		const starter = conductor.createTask(
			{ name: "starter" },
			{ invocable: true },
			async (_e, ctx) => {
				await ctx.start("inherited", { name: "work" }, { label: "inherited" });
				await ctx.start(
					"overridden",
					{ name: "work" },
					{ label: "overridden" },
					{ metadata: (metadata) => ({ tenant: metadata?.tenant || "", replyTo: "thread-2" }) },
				);
				seen.starter = ctx.metadata;
			},
		);
		const orchestrator = Orchestrator.create({ conductor, tasks: [starter, work] });

		await conductor.ensureInstalled();
		await conductor.invoke(
			{ name: "starter" },
			{},
			{ metadata: { tenant: "acme", replyTo: "thread-1" } },
		);
		await orchestrator.drain();

		expect(seen).toEqual({
			inherited: { tenant: "acme", replyTo: "thread-1" },
			overridden: { tenant: "acme", replyTo: "thread-2" },
			starter: { tenant: "acme", replyTo: "thread-1" },
		});
	});

	test("a dedupe_key returns the execution holding it without superseding it", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "starter", returns: z.object({ id: z.string() }) }),
				defineTask({ name: "work", payload: z.object({ label: z.string() }) }),
			]),
			context: {},
		});
		const release = new Deferred<void>();
		const ran: string[] = [];
		const work = conductor.createTask({ name: "work" }, { invocable: true }, async (event) => {
			ran.push(event.payload.label);
			await release.promise;
		});
		const starter = conductor.createTask(
			{ name: "starter" },
			{ invocable: true },
			async (_e, ctx) => {
				const id = await ctx.start(
					"reconnect",
					{ name: "work" },
					{ label: "started" },
					{ dedupe_key: "thread-1" },
				);
				return { id };
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [starter, work],
			defaultWorker: { ...config, concurrency: 2 },
		});

		await orchestrator.start();
		const existingId = await conductor.invoke(
			{ name: "work" },
			{ label: "existing" },
			{ dedupe_key: "thread-1" },
		);
		await waitForCondition(() => ran.length === 1);
		const starterId = await conductor.invoke({ name: "starter" }, {});
		const { id } = await conductor.waitForResult({ name: "starter" }, starterId, {
			pollIntervalMs: 10,
		});
		release.resolve();
		await conductor.waitForResult(existingId, { pollIntervalMs: 10 });
		await orchestrator.stop();

		expect(id).toBe(existingId);
		expect(ran).toEqual(["existing"]);
	});
});
