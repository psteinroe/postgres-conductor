import postgres from "postgres";
import { z } from "zod";
import { afterAll, afterEach, beforeAll, describe, expect, expectTypeOf, test } from "vitest";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask, type SettledSignal } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const sessionDefinition = defineTask({
	name: "session",
	payload: z.object({}),
	// declares signals, but not pgconductor.settled
	signals: { start: z.object({ mode: z.enum(["complete", "fail"]) }) },
});
const runDefinition = defineTask({
	name: "run",
	payload: z.object({ mode: z.enum(["complete", "fail", "block"]) }),
	returns: z.object({ value: z.string() }),
});
const jobDefinition = defineTask({
	name: "job",
	payload: z.object({}),
	returns: z.object({ value: z.string() }),
});
const flowDefinition = defineTask({
	name: "flow",
	payload: z.object({}),
	returns: z.object({ value: z.string() }),
});
const tasks = TaskSchemas.fromSchema([
	sessionDefinition,
	runDefinition,
	jobDefinition,
	flowDefinition,
]);

const session = (key = "s1") => ({ signal: { task: { name: "session" }, key } });

describe("onSettled", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const cleanups: (() => Promise<void>)[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
		await Promise.all(databases.map((db) => db.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	async function setup({ maxAttempts }: { maxAttempts?: number } = {}) {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({ sql: db.sql, tasks, context: {}, clock: db.clock });
		const settled: SettledSignal[] = [];
		let activations = 0;
		const gate = new Deferred<void>();
		const started = new Deferred<string>();

		const sessionTask = conductor.createTask(
			{ name: "session", actor: true },
			{ invocable: true },
			async (_event, ctx) => {
				activations++;
				ctx.onSignal("pgconductor.settled", (payload) => {
					expectTypeOf(payload).toEqualTypeOf<SettledSignal>();
					settled.push(payload);
				});
				ctx.onSignal("start", async (payload, { id }) => {
					await ctx.invoke(id, { name: "run" }, payload, { onSettled: session() });
				});
			},
		);
		const runTask = conductor.createTask(
			{ name: "run", maxAttempts },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name !== "pgconductor.invoke") throw new Error("unexpected event");
				if (event.payload.mode === "fail") throw new Error("boom");
				if (event.payload.mode === "block") {
					started.resolve(ctx.executionId);
					await gate.promise;
				}
				return { value: event.payload.mode };
			},
		);
		const jobTask = conductor.createTask(
			{ name: "job", actor: true },
			{ invocable: true },
			async (_event, ctx) => {
				ctx.onSignal("finish", async () => {
					await ctx.complete({ value: "finished" });
				});
				await ctx.untilIdle(async () => {});
			},
		);

		const flowTask = conductor.createTask(
			{ name: "flow" },
			{ invocable: true },
			async (_event, ctx) => ctx.invokeAndWait("child", { name: "run" }, { mode: "fail" }),
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [sessionTask, runTask, jobTask, flowTask],
			defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 20, concurrency: 4 },
		});
		cleanups.push(() => orchestrator.stop());
		// a superseded run keeps blocking its handler, which stopping waits for
		cleanups.push(async () => gate.resolve());
		await conductor.ensureInstalled();
		await orchestrator.start();

		const openSession = async () => {
			const { id } = await conductor.signalWithStart({
				task: "session",
				key: "s1",
				signal: { name: "hello", id: "hello" },
			});
			await waitForCondition(async () => (await row(id))?.waiting_step_key === "pgconductor.idle");
			return id;
		};
		const row = async (id: string) => {
			const [execution] = await db.sql<
				{
					waiting_step_key: string | null;
					attempts: number;
					last_error: string | null;
					on_settled: unknown;
					completed_at: Date | null;
					failed_at: Date | null;
				}[]
			>`select waiting_step_key, attempts, last_error, on_settled, completed_at, failed_at
				from pgconductor._private_executions where id = ${id}`;
			return execution;
		};
		const settledSignals = (executionId: string) =>
			db.sql<{ execution_id: string; id: string; payload: unknown }[]>`
				select execution_id, id, payload from pgconductor._private_execution_signals
				where name = 'pgconductor.settled' and id = ${`pgconductor.settled:${executionId}`}`;

		return {
			db,
			conductor,
			settled,
			activations: () => activations,
			gate,
			started,
			openSession,
			row,
			settledSignals,
		};
	}

	test("a completed execution signals the actor that invoked it with ctx.invoke, and wakes it", async () => {
		const { conductor, settled, activations, openSession, settledSignals } = await setup();
		const sessionId = await openSession();
		const before = activations();

		await conductor.signal(
			{ task: { name: "session" }, key: "s1" },
			{ name: "start", id: "go", payload: { mode: "complete" } },
		);
		await waitForCondition(() => settled.length === 1);

		const [run] = await conductor.listExecutions({ task: { name: "run" } });
		expect(settled[0]).toEqual({
			executionId: run?.id,
			taskKey: "run",
			status: "completed",
			result: { value: "complete" },
			error: null,
		});
		// one activation started the run, a later one received the signal
		expect(activations()).toBeGreaterThan(before);
		const stored = await settledSignals(run?.id || "");
		expect(stored).toHaveLength(1);
		expect(stored[0]?.execution_id).toBe(sessionId);
	});

	test("conductor.invoke: a permanent failure signals once, after the last retry", async () => {
		const { db, conductor, settled, openSession, row, settledSignals } = await setup({
			maxAttempts: 2,
		});
		await openSession();

		const id = await conductor.invoke({ name: "run" }, { mode: "fail" }, { onSettled: session() });
		await waitForCondition(async () => (await row(id))?.last_error === "boom");
		// retried: nothing is sent
		expect(await settledSignals(id)).toHaveLength(0);

		await db.clock.advance("1m");
		await waitForCondition(() => settled.length === 1);
		expect(settled[0]).toEqual({
			executionId: id,
			taskKey: "run",
			status: "failed",
			result: null,
			error: "boom",
		});
		expect((await row(id))?.failed_at).not.toBeNull();
	});

	test("a workflow failed by the child it waits on signals failed", async () => {
		const { conductor, settled, openSession } = await setup({ maxAttempts: 1 });
		await openSession();

		const id = await conductor.invoke({ name: "flow" }, {}, { onSettled: session() });
		await waitForCondition(() => settled.length === 1);
		expect(settled[0]).toEqual({
			executionId: id,
			taskKey: "flow",
			status: "failed",
			result: null,
			error: "Child execution failed: boom",
		});
	});

	test("cancelling a pending or a running execution signals cancelled", async () => {
		const { conductor, settled, started, openSession, settledSignals } = await setup();
		await openSession();

		const pending = await conductor.invoke(
			{ name: "run" },
			{ mode: "complete" },
			{ onSettled: session(), run_at: new Date(Date.now() + 3_600_000) },
		);
		expect(await conductor.cancel(pending, { reason: "not needed" })).toBe(true);
		await waitForCondition(() => settled.length === 1);
		expect(settled[0]).toMatchObject({
			executionId: pending,
			status: "cancelled",
			error: "not needed",
			result: null,
		});

		const running = await conductor.invoke(
			{ name: "run" },
			{ mode: "block" },
			{ onSettled: session() },
		);
		expect(await started.promise).toBe(running);
		expect(await conductor.cancel(running, { reason: "stop it" })).toBe(true);
		await waitForCondition(() => settled.length === 2);
		expect(settled[1]).toMatchObject({
			executionId: running,
			status: "cancelled",
			error: "stop it",
		});
		expect(await settledSignals(running)).toHaveLength(1);
	});

	test("a superseded execution signals failed; a dedupe key keeps the first target", async () => {
		const { db, conductor, settled, started, openSession, row } = await setup();
		await openSession();

		const first = await conductor.invoke(
			{ name: "run" },
			{ mode: "block" },
			{ onSettled: session(), dedupe_key: "same" },
		);
		await started.promise;
		const second = await conductor.invoke(
			{ name: "run" },
			{ mode: "complete" },
			{ onSettled: session(), dedupe_key: "same" },
		);
		expect(second).not.toBe(first);
		await waitForCondition(() => settled.length === 2);
		expect(settled.find((s) => s.executionId === first)).toMatchObject({
			status: "failed",
			error: "superseded by reinvoke",
		});
		expect(settled.find((s) => s.executionId === second)).toMatchObject({ status: "completed" });

		// a pending execution holding the key keeps its target
		const later = new Date(Date.now() + 3_600_000);
		const pending = await conductor.invoke(
			{ name: "run" },
			{ mode: "complete" },
			{ onSettled: session("s1"), dedupe_key: "pending", run_at: later },
		);
		const again = await conductor.invoke(
			{ name: "run" },
			{ mode: "complete" },
			{ onSettled: session("other"), dedupe_key: "pending", run_at: later },
		);
		expect(again).toBe(pending);
		expect((await row(pending))?.on_settled).toEqual({
			signal: { task_key: "session", queue: "default", key: "s1" },
		});
		const [{ count }] = await db.sql<[{ count: number }]>`
			select count(*)::int as count from pgconductor._private_executions where on_settled is not null`;
		expect(count).toBe(3);
	});

	test("without an open execution for the key, nothing is stored and nothing starts", async () => {
		const { db, conductor, row } = await setup();

		const id = await conductor.invoke(
			{ name: "run" },
			{ mode: "complete" },
			{ onSettled: session("nobody") },
		);
		await waitForCondition(async () => (await row(id))?.completed_at != null);
		const signals = await db.sql`
			select 1 from pgconductor._private_execution_signals where name = 'pgconductor.settled'`;
		expect(signals).toHaveLength(0);
		expect(await conductor.listExecutions({ task: { name: "session" } })).toEqual([]);
	});

	test("an invoked actor signals only when it completes, not when activations go idle", async () => {
		const { conductor, settled, openSession, row, settledSignals } = await setup();
		await openSession();

		const id = await conductor.invoke({ name: "job" }, {}, { onSettled: session() });
		await waitForCondition(async () => (await row(id))?.waiting_step_key === "pgconductor.idle");
		await conductor.signal(id, { name: "ping", id: "p1" });
		await waitForCondition(async () => (await row(id))?.waiting_step_key === "pgconductor.idle");
		expect(await settledSignals(id)).toHaveLength(0);

		await conductor.signal(id, { name: "finish", id: "f1" });
		await waitForCondition(() => settled.length === 1);
		expect(settled[0]).toEqual({
			executionId: id,
			taskKey: "job",
			status: "completed",
			result: { value: "finished" },
			error: null,
		});
	});

	test("a cancel racing the settlement sends exactly one signal", async () => {
		const { db, conductor, settled, gate, started, openSession, row, settledSignals } =
			await setup();
		await openSession();

		const id = await conductor.invoke({ name: "run" }, { mode: "block" }, { onSettled: session() });
		await started.promise;
		const other = postgres(db.url, { max: 1 });
		cleanups.push(() => other.end());
		const cancelled = other`select pgconductor.cancel_execution(${id}::uuid, 'raced')`;
		gate.resolve();
		await cancelled;
		await waitForCondition(async () => {
			const execution = await row(id);
			return execution?.completed_at != null || execution?.failed_at != null;
		});
		await waitForCondition(() => settled.length === 1);

		// a second delivery of the same execution stores nothing
		await db.sql`select pgconductor._private_signal_settled(${db.sql.json([
			{
				execution_id: id,
				task_key: "run",
				on_settled: { signal: { task_key: "session", queue: "default", key: "s1" } },
				status: "completed",
			},
		])}::jsonb)`;
		expect(await settledSignals(id)).toHaveLength(1);
		expect(settled).toHaveLength(1);
		expect(["completed", "cancelled"]).toContain(settled[0]?.status);
		await conductor.cancel(id);
		expect(await settledSignals(id)).toHaveLength(1);
	});
});
