import { setTimeout as sleep } from "node:timers/promises";
import postgres from "postgres";
import { z } from "zod";
import { afterAll, afterEach, beforeAll, describe, expect, expectTypeOf, test } from "vitest";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import { QueryBuilder } from "../../src/query-builder";
import type { ExecutionInfo } from "../../src/database-client";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const workerDefinition = defineTask({
	name: "agent-worker",
	payload: z.object({ workerId: z.string() }),
	returns: z.object({ stopped: z.boolean() }),
});
const approvalDefinition = defineTask({
	name: "approval",
	queue: "approvals",
	payload: z.object({ requestId: z.string() }),
	returns: z.object({ approved: z.boolean() }),
});
const stopperDefinition = defineTask({
	name: "stopper",
	queue: "stoppers",
	payload: z.object({ workerId: z.string() }),
	returns: z.object({ sent: z.boolean() }),
});
const tasks = TaskSchemas.fromSchema([workerDefinition, approvalDefinition, stopperDefinition]);

const worker = { name: "agent-worker" } as const;
const approval = { name: "approval", queue: "approvals" } as const;

describe("Executions by key", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const cleanups: (() => Promise<unknown>)[] = [];

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

	async function setup() {
		const db = await pool.child();
		databases.push(db);
		const conductor = createConductor(db);
		await conductor.ensureInstalled();
		// a queue's partition is created with its row, which workers otherwise register
		await db.sql`
			insert into pgconductor._private_queues (name) values ('approvals'), ('stoppers')
			on conflict do nothing
		`;
		return { db, conductor };
	}

	function createConductor(db: TestDatabase, sql = db.sql) {
		return Conductor.create({ sql, tasks, context: {} });
	}

	async function storedSignals(db: TestDatabase) {
		return db.sql<{ execution_id: string; id: string }[]>`
			select execution_id, id from pgconductor._private_execution_signals order by seq
		`;
	}

	async function startWorker(conductor: ReturnType<typeof createConductor>, workerId: string) {
		const { id } = await conductor.signalWithStart({
			task: "agent-worker",
			key: workerId,
			signal: { name: "start", id: `start-${workerId}` },
			input: { workerId },
		});
		return id;
	}

	test("signals the open execution holding a key", async () => {
		const { db, conductor } = await setup();
		const id = await startWorker(conductor, "w1");

		const sent = await conductor.signal(
			{ task: worker, key: "w1" },
			{ name: "stop", id: "stop-1", payload: { reason: "deploy" } },
		);

		expect(sent).toBe(true);
		const signals = await storedSignals(db);
		expect(signals.map((s) => [s.execution_id, s.id])).toEqual([
			[id, "start-w1"],
			[id, "stop-1"],
		]);

		// a resent id is stored once
		expect(
			await conductor.signal({ task: worker, key: "w1" }, { name: "stop", id: "stop-1" }),
		).toBe(true);
		expect(await storedSignals(db)).toHaveLength(2);
	});

	test("returns false for a key no open execution holds", async () => {
		const { db, conductor } = await setup();
		await startWorker(conductor, "w1");

		const stop = { name: "stop", id: "stop-1" };
		expect(await conductor.signal({ task: worker, key: "unknown" }, stop)).toBe(false);
		// the key is scoped to its task and queue
		expect(await conductor.signal({ task: approval, key: "w1" }, stop)).toBe(false);
		expect(
			await conductor.signal({ task: { name: "agent-worker", queue: "other" }, key: "w1" }, stop),
		).toBe(false);
		expect(await storedSignals(db)).toHaveLength(1);
	});

	test("an ended execution no longer holds its key", async () => {
		const { db, conductor } = await setup();

		const cancelled = await startWorker(conductor, "cancelled");
		expect(await conductor.cancel(cancelled)).toBe(true);

		const completed = await startWorker(conductor, "completed");
		const failed = await startWorker(conductor, "failed");
		await db.sql`
			update pgconductor._private_executions set completed_at = now()
			where id = ${completed}::uuid
		`;
		await db.sql`
			update pgconductor._private_executions set failed_at = now()
			where id = ${failed}::uuid
		`;

		for (const key of ["cancelled", "completed", "failed"]) {
			expect(await conductor.signal({ task: worker, key }, { name: "stop", id: "stop" })).toBe(
				false,
			);
			expect(await conductor.getExecution({ task: worker, key })).toBeNull();
		}
		expect((await conductor.getExecution(cancelled))?.status).toBe("cancelled");

		// signalWithStart starts a new execution for the key, which signals then reach
		const next = await startWorker(conductor, "cancelled");
		expect(next).not.toBe(cancelled);
		expect(
			await conductor.signal({ task: worker, key: "cancelled" }, { name: "stop", id: "stop" }),
		).toBe(true);
		expect((await conductor.getExecution({ task: worker, key: "cancelled" }))?.id).toBe(next);
	});

	test("waits for a signalWithStart starting the execution for the key", async () => {
		const { db, conductor } = await setup();

		// start the execution in a transaction that has not committed yet
		const starter = postgres(db.url, { max: 1 });
		cleanups.push(() => starter.end());
		const started = new Deferred<string>();
		const commit = new Deferred<void>();
		const tx = starter.begin(async (tx) => {
			const [row] = await tx<{ id: string }[]>`
				select id from pgconductor.signal_with_start(
					'agent-worker', 'default', 'w1', 'start', 'start-w1', null, '{"workerId":"w1"}'::jsonb
				)
			`;
			started.resolve(row?.id || "");
			await commit.promise;
		});
		const id = await started.promise;

		const sql = postgres(db.url, { max: 1 });
		cleanups.push(() => sql.end());
		const sender = createConductor(db, sql);
		let settled = false;
		const sent = sender
			.signal({ task: worker, key: "w1" }, { name: "stop", id: "stop-1" })
			.finally(() => {
				settled = true;
			});

		// a lookup without the key lock would find no committed execution and return false
		await sleep(200);
		expect(settled).toBe(false);

		commit.resolve();
		await tx;
		expect(await sent).toBe(true);
		const signals = await storedSignals(db);
		expect(signals.map((s) => [s.execution_id, s.id])).toEqual([
			[id, "start-w1"],
			[id, "stop-1"],
		]);
	});

	test("concurrent signalWithStart and signals by key reach one execution", async () => {
		const { db } = await setup();
		const total = 10;
		const sql = postgres(db.url, { max: total * 2 });
		cleanups.push(() => sql.end());
		const sender = createConductor(db, sql);

		const [starts, signals] = await Promise.all([
			Promise.all(
				Array.from({ length: total }, (_, n) =>
					sender.signalWithStart({
						task: "agent-worker",
						key: "w1",
						signal: { name: "start", id: `start-${n}` },
						input: { workerId: "w1" },
					}),
				),
			),
			Promise.all(
				Array.from({ length: total }, (_, n) =>
					sender.signal({ task: worker, key: "w1" }, { name: "stop", id: `stop-${n}` }),
				),
			),
		]);

		expect(new Set(starts.map((start) => start.id)).size).toBe(1);
		// exactly one call started it
		expect(starts.filter((start) => start.started)).toHaveLength(1);
		const stored = await storedSignals(db);
		// every signal that reported true is stored for the one execution
		expect(stored.every((s) => s.execution_id === starts[0]?.id)).toBe(true);
		expect(stored.filter((s) => s.id.startsWith("stop-"))).toHaveLength(
			signals.filter(Boolean).length,
		);
	});

	test("ctx.sendSignal signals by key", async () => {
		const { conductor } = await setup();
		const received: string[] = [];
		const stopped = new Deferred<void>();

		const agent = conductor.createTask(
			{ name: "agent-worker" },
			{ invocable: true },
			async (_event, ctx) => {
				ctx.onSignal("stop", (_payload, { id }) => {
					received.push(id);
					stopped.resolve();
				});
				await stopped.promise;
				return { stopped: true };
			},
		);
		const stopper = conductor.createTask(
			{ name: "stopper", queue: "stoppers" },
			{ invocable: true },
			async (event, ctx) => {
				const sent = await ctx.sendSignal(
					{ task: worker, key: event.payload.workerId },
					{ name: "stop", id: `stop-${event.payload.workerId}` },
				);
				return { sent };
			},
		);
		const config = { pollIntervalMs: 50, flushIntervalMs: 50 };
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [agent],
			defaultWorker: config,
			workers: [conductor.createWorker({ queue: "stoppers", tasks: [stopper], config })],
		});
		cleanups.push(() => orchestrator.stop());

		const workerId = await startWorker(conductor, "w1");
		const known = await conductor.invoke(
			{ name: "stopper", queue: "stoppers" },
			{ workerId: "w1" },
		);
		const unknown = await conductor.invoke(
			{ name: "stopper", queue: "stoppers" },
			{ workerId: "w2" },
		);
		await orchestrator.start();

		const stopperTask = { name: "stopper", queue: "stoppers" } as const;
		expect(await conductor.waitForResult(stopperTask, known, { pollIntervalMs: 50 })).toEqual({
			sent: true,
		});
		expect(await conductor.waitForResult(stopperTask, unknown, { pollIntervalMs: 50 })).toEqual({
			sent: false,
		});
		await waitForCondition(
			async () => (await conductor.getExecution(workerId))?.status === "completed",
		);
		expect(received).toEqual(["stop-w1"]);
	});

	test("getExecution reads the open execution holding a key", async () => {
		const { conductor } = await setup();
		const id = await startWorker(conductor, "w1");

		const execution = await conductor.getExecution({ task: worker, key: "w1" });
		expect(execution).toMatchObject({
			id,
			taskKey: "agent-worker",
			queue: "default",
			key: "w1",
			status: "pending",
			payload: { workerId: "w1" },
		});
		expectTypeOf(execution).toEqualTypeOf<ExecutionInfo<
			{ stopped: boolean },
			{ workerId: string }
		> | null>();

		expect(await conductor.getExecution({ task: worker, key: "w2" })).toBeNull();
		expect(await conductor.getExecution({ task: approval, key: "w1" })).toBeNull();
	});

	test("getExecution reads the execution invoked with a dedupe key", async () => {
		const { db, conductor } = await setup();
		const id = await conductor.invoke(approval, { requestId: "r1" }, { dedupe_key: "r1" });

		const execution = await conductor.getExecution({ task: approval, dedupeKey: "r1" });
		expect(execution).toMatchObject({ id, queue: "approvals", payload: { requestId: "r1" } });
		expectTypeOf(execution).toEqualTypeOf<ExecutionInfo<
			{ approved: boolean },
			{ requestId: string }
		> | null>();

		// a settled execution keeps its dedupe key
		await db.sql`
			update pgconductor._private_executions
			set completed_at = now(), result = '{"approved":true}'::jsonb
			where id = ${id}::uuid
		`;
		expect(await conductor.getExecution({ task: approval, dedupeKey: "r1" })).toMatchObject({
			id,
			status: "completed",
			result: { approved: true },
		});

		expect(await conductor.getExecution({ task: approval, dedupeKey: "r2" })).toBeNull();
		expect(await conductor.getExecution({ task: worker, dedupeKey: "r1" })).toBeNull();
	});

	test("getExecution by dedupe key skips throttled and debounced executions", async () => {
		const { conductor } = await setup();
		await conductor.invoke(
			approval,
			{ requestId: "r1" },
			{ dedupe_key: "r1", throttle: { seconds: 60 } },
		);
		await conductor.invoke(
			approval,
			{ requestId: "r1" },
			{ dedupe_key: "r1", debounce: { seconds: 60 } },
		);

		// throttled and debounced executions are keyed by their slot, so a key may have several
		expect(await conductor.getExecution({ task: approval, dedupeKey: "r1" })).toBeNull();

		const id = await conductor.invoke(approval, { requestId: "r1" }, { dedupe_key: "r1" });
		expect((await conductor.getExecution({ task: approval, dedupeKey: "r1" }))?.id).toBe(id);
	});

	test("lookups by key and dedupe key read their indexes in a generic plan", async () => {
		const { db, conductor } = await setup();
		await startWorker(conductor, "w1");
		await conductor.invoke(
			approval,
			Array.from({ length: 200 }, (_, n) => ({
				payload: { requestId: `r${n}` },
				dedupe_key: `r${n}`,
			})),
		);
		await db.sql`analyze pgconductor._private_executions`;

		const builder = new QueryBuilder(db.sql);
		const task = { key: "approval", queue: "approvals" };
		const queries = {
			byKey: builder.buildGetExecution({ task, key: "w1" }),
			byDedupeKey: builder.buildGetExecution({ task, dedupeKey: "r1" }),
		};

		await db.sql`set enable_seqscan = off`;
		await db.sql`set plan_cache_mode = force_generic_plan`;
		for (const [name, query] of Object.entries(queries)) {
			const { string } = await query.describe();
			await db.sql.unsafe(`prepare ${name} as ${string}`);
			const plan = await db.sql.unsafe<{ "QUERY PLAN": string }[]>(
				`explain execute ${name}('approval', 'approvals', 'r1')`,
			);
			const text = plan.map((row) => row["QUERY PLAN"]).join("\n");
			const index = name === "byKey" ? /_task_key_key_queue_idx/ : /_task_key_dedupe_key_queue_idx/;
			expect(text).toMatch(index);
		}
	});
});
