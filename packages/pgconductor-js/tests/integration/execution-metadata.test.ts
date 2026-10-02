import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { defineEvent } from "../../src/event-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const metadataSchema = z.object({ tenant: z.string(), replyTo: z.string().optional() });

describe("execution metadata (Postgres integration)", () => {
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

	test("invoked executions see their metadata, and none when not set", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "record" })]),
			metadata: metadataSchema,
			context: {},
		});
		const seen: unknown[] = [];
		const task = conductor.createTask({ name: "record" }, { invocable: true }, async (_e, ctx) => {
			seen.push(ctx.metadata);
		});
		const orchestrator = Orchestrator.create({ conductor, tasks: [task] });

		await conductor.ensureInstalled();
		await conductor.invoke({ name: "record" }, {}, { metadata: { tenant: "acme" } });
		await conductor.invoke({ name: "record" }, [
			{ payload: {}, metadata: { tenant: "globex" } },
			{ payload: {} },
		]);
		await orchestrator.drain();

		expect(seen).toHaveLength(3);
		expect(seen).toContainEqual({ tenant: "acme" });
		expect(seen).toContainEqual({ tenant: "globex" });
		expect(seen).toContainEqual(undefined);
	});

	test("children inherit metadata, and an override applies to that child only", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "parent" }),
				defineTask({ name: "child", payload: z.object({ label: z.string() }) }),
			]),
			metadata: metadataSchema,
			context: {},
		});
		const seen: Record<string, unknown> = {};
		const childTask = conductor.createTask(
			{ name: "child" },
			{ invocable: true },
			async (event, ctx) => {
				seen[event.payload.label] = ctx.metadata;
			},
		);
		const parentTask = conductor.createTask(
			{ name: "parent" },
			{ invocable: true },
			async (_e, ctx) => {
				await ctx.invoke("inherited", { name: "child" }, { label: "inherited" });
				await ctx.invoke(
					"overridden",
					{ name: "child" },
					{ label: "overridden" },
					{ metadata: (metadata) => ({ tenant: metadata?.tenant || "", replyTo: "thread-2" }) },
				);
				await ctx.invoke("after", { name: "child" }, { label: "after" });
				seen.parent = ctx.metadata;
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parentTask, childTask],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});

		await orchestrator.start();
		await conductor.invoke(
			{ name: "parent" },
			{},
			{ metadata: { tenant: "acme", replyTo: "thread-1" } },
		);
		await waitForCondition(() => "parent" in seen);
		await orchestrator.stop();

		expect(seen).toEqual({
			inherited: { tenant: "acme", replyTo: "thread-1" },
			overridden: { tenant: "acme", replyTo: "thread-2" },
			after: { tenant: "acme", replyTo: "thread-1" },
			parent: { tenant: "acme", replyTo: "thread-1" },
		});
	});

	test("event-triggered executions inherit metadata from conductor.emit and ctx.emit", async () => {
		const db = await child();
		const orderPlaced = defineEvent({
			name: "order.placed",
			payload: z.object({ source: z.string() }),
		});
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "emitter" })]),
			events: EventSchemas.fromSchema([orderPlaced]),
			metadata: metadataSchema,
			context: {},
		});
		const seen: Record<string, unknown> = {};
		const listener = conductor.createTask(
			{ name: "listener" },
			{ event: "order.placed" },
			async (event, ctx) => {
				seen[event.payload.source] = ctx.metadata;
			},
		);
		const emitter = conductor.createTask(
			{ name: "emitter" },
			{ invocable: true },
			async (_e, ctx) => {
				await ctx.emit("order.placed", { source: "ctx" });
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [listener, emitter],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});

		await orchestrator.start();
		await conductor.emit("order.placed", { source: "conductor" }, { metadata: { tenant: "acme" } });
		await conductor.invoke({ name: "emitter" }, {}, { metadata: { tenant: "globex" } });
		await waitForCondition(() => Object.keys(seen).length === 2);
		await orchestrator.stop();

		expect(seen).toEqual({ conductor: { tenant: "acme" }, ctx: { tenant: "globex" } });
	});

	test("retries, resumes and dead-letter deliveries keep metadata", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "flaky" }),
				defineTask({ name: "failures", queue: "dlq" }),
			]),
			metadata: metadataSchema,
			context: {},
		});
		const seen: unknown[] = [];
		const failures = conductor.createTask(
			{ name: "failures", queue: "dlq" },
			{ invocable: true },
			async (_e, ctx) => {
				seen.push({ deadLetter: ctx.metadata });
			},
		);
		const flaky = conductor.createTask(
			{ name: "flaky", maxAttempts: 2, deadLetter: { queue: "dlq", task: failures } },
			{ invocable: true },
			async (_e, ctx) => {
				await ctx.sleep("pause", 0);
				seen.push(ctx.metadata);
				throw new Error("boom");
			},
		);
		const config = { pollIntervalMs: 10, flushIntervalMs: 10 };
		const orchestrator = Orchestrator.create({
			conductor,
			workers: [
				conductor.createWorker({ queue: "default", tasks: [flaky], config }),
				conductor.createWorker({ queue: "dlq", tasks: [failures], config }),
			],
		});

		await db.client.setFakeTime({ date: new Date("2024-01-01T12:00:00Z") });
		await orchestrator.start();
		await conductor.invoke({ name: "flaky" }, {}, { metadata: { tenant: "acme" } });
		let retryAt: Date | undefined;
		await waitForCondition(async () => {
			const [row] = await db.sql<{ run_at: Date }[]>`
				select run_at from pgconductor._private_executions
				where task_key = 'flaky' and last_error is not null and locked_by is null
			`;
			retryAt = row?.run_at;
			return retryAt !== undefined;
		});
		await db.client.setFakeTime({ date: new Date((retryAt?.getTime() || 0) + 1) });
		await waitForCondition(() => seen.length === 3);
		await orchestrator.stop();
		await db.client.clearFakeTime();

		expect(seen).toEqual([
			{ tenant: "acme" },
			{ tenant: "acme" },
			{ deadLetter: { tenant: "acme" } },
		]);
	});

	test("cron executions created by ctx.schedule inherit metadata", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "scheduler" }),
				defineTask({ name: "report" }),
			]),
			metadata: metadataSchema,
			context: {},
		});
		const seen: unknown[] = [];
		const report = conductor.createTask(
			{ name: "report" },
			{ invocable: true },
			async (_e, ctx) => {
				seen.push(ctx.metadata);
			},
		);
		const scheduler = conductor.createTask(
			{ name: "scheduler" },
			{ invocable: true },
			async (_e, ctx) => {
				await ctx.schedule({ name: "report" }, "reporting", { cron: "* * * * * *" });
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [report, scheduler],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await orchestrator.start();
		await conductor.invoke({ name: "scheduler" }, {}, { metadata: { tenant: "acme" } });
		await waitForCondition(() => seen.length >= 2);
		await orchestrator.stop();

		expect(seen.slice(0, 2)).toEqual([{ tenant: "acme" }, { tenant: "acme" }]);
	}, 30000);

	test("metadata is validated against the schema and limited to 8 KB", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "noop" })]),
			events: EventSchemas.fromSchema([defineEvent({ name: "ping", payload: z.object({}) })]),
			metadata: z.object({ tenant: z.string() }).passthrough(),
			context: {},
		});
		await conductor.ensureInstalled();

		await expect(
			conductor.invoke({ name: "noop" }, {}, { metadata: { tenant: 1 } as never }),
		).rejects.toThrow("Invalid metadata: tenant:");
		await expect(conductor.emit("ping", {}, { metadata: { tenant: 1 } as never })).rejects.toThrow(
			"Invalid metadata: tenant:",
		);

		const oversized = { tenant: "acme", blob: "x".repeat(8192) };
		await expect(conductor.invoke({ name: "noop" }, {}, { metadata: oversized })).rejects.toThrow(
			"Execution metadata must not exceed 8192 bytes",
		);
		await expect(conductor.emit("ping", {}, { metadata: oversized })).rejects.toThrow(
			"Execution metadata must not exceed 8192 bytes",
		);
		await conductor.invoke({ name: "noop" }, {}, { metadata: { tenant: "acme", blob: "x" } });
	});

	test("an oversized ctx.invoke override fails only the invoking attempt", async () => {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "parent" }),
				defineTask({ name: "child" }),
				defineTask({ name: "bystander" }),
			]),
			context: {},
		});
		const childTask = conductor.createTask({ name: "child" }, { invocable: true }, async () => {});
		const parentTask = conductor.createTask(
			{ name: "parent", maxAttempts: 1 },
			{ invocable: true },
			async (_e, ctx) => {
				await ctx.invoke("child", { name: "child" }, {}, { metadata: { blob: "x".repeat(8192) } });
			},
		);
		const bystander = conductor.createTask(
			{ name: "bystander" },
			{ invocable: true },
			async () => {},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parentTask, childTask, bystander],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10, flushBatchSize: 10 },
		});

		await conductor.ensureInstalled();
		await conductor.invoke({ name: "parent" }, {});
		await conductor.invoke({ name: "bystander" }, {});
		await orchestrator.drain();

		const rows = await db.sql<
			{ task_key: string; completed: boolean; last_error: string | null }[]
		>`
			select task_key, completed_at is not null as completed, last_error
			from pgconductor._private_executions
			where task_key in ('parent', 'child', 'bystander')
			order by task_key
		`;
		expect(rows).toHaveLength(2);
		expect(rows[0]).toMatchObject({ task_key: "bystander", completed: true });
		expect(rows[1]?.task_key).toBe("parent");
		expect(rows[1]?.last_error).toContain("Execution metadata must not exceed 8192 bytes");
	});
});
