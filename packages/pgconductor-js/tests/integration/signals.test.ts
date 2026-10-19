import postgres from "postgres";
import { z } from "zod";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import type { JsonValue } from "../../src/database-client";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const sessionDefinition = defineTask({
	name: "session",
	payload: z.object({ topic: z.string().optional() }),
});
const senderDefinition = defineTask({
	name: "sender",
	queue: "senders",
	payload: z.object({ target: z.string() }),
});
const tasks = TaskSchemas.fromSchema([sessionDefinition, senderDefinition]);

type Received = { attempt: number; id: string; payload: JsonValue };

describe("Signals", () => {
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

	async function setup(
		handler: (
			ctx: { attempt: number; received: Received[] },
			taskCtx: Parameters<Parameters<ReturnType<typeof createConductor>["createTask"]>[2]>[1],
		) => Promise<void>,
		options: { flushIntervalMs?: number; pollIntervalMs?: number } = {},
	) {
		const db = await pool.child();
		databases.push(db);
		const conductor = createConductor(db);
		const received: Received[] = [];
		let attempt = 0;

		const task = conductor.createTask({ name: "session" }, { invocable: true }, async (_e, ctx) => {
			const state = { attempt: ++attempt, received };
			await handler(state, ctx);
		});

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: {
				pollIntervalMs: options.pollIntervalMs || 50,
				flushIntervalMs: options.flushIntervalMs || 50,
			},
		});
		cleanups.push(() => orchestrator.stop());

		await conductor.ensureInstalled();
		return { db, conductor, orchestrator, received, attempts: () => attempt };
	}

	function createConductor(db: TestDatabase, sql = db.sql) {
		return Conductor.create({ sql, tasks, context: {} });
	}

	function collect(state: { attempt: number; received: Received[] }) {
		return (payload: JsonValue, { id }: { id: string }) => {
			state.received.push({ attempt: state.attempt, id, payload });
		};
	}

	async function executionRow(db: TestDatabase, id: string) {
		const [row] = await db.sql<
			{ completed_at: Date | null; failed_at: Date | null; locked_by: string | null }[]
		>`select completed_at, failed_at, locked_by from pgconductor._private_executions where id = ${id}::uuid`;
		return row;
	}

	test("a running execution receives a signal within a few poll intervals", async () => {
		const started = new Deferred<void>();
		const got = new Deferred<number>();
		const { conductor, orchestrator } = await setup(async (state, ctx) => {
			ctx.onSignal("input", () => got.resolve(Date.now()));
			started.resolve();
			await got.promise;
			void state;
		});

		const id = await conductor.invoke({ name: "session" }, {});
		await orchestrator.start();
		await started.promise;
		await Bun.sleep(200);

		const sentAt = Date.now();
		expect(await conductor.signal(id, { name: "input", id: "s1", payload: { text: "hi" } })).toBe(
			true,
		);
		// pollIntervalMs is 50
		expect((await got.promise) - sentAt).toBeLessThan(500);
	});

	test("signals stored before the handler started are delivered first, in order", async () => {
		const done = new Deferred<void>();
		const { db, conductor, orchestrator, received } = await setup(async (state, ctx) => {
			ctx.onSignal("input", (payload, info) => {
				collect(state)(payload, info);
				if (state.received.length === 4) done.resolve();
			});
			await done.promise;
		});

		const id = await conductor.invoke({ name: "session" }, {});
		for (const n of [1, 2, 3]) {
			await conductor.signal(id, { name: "input", id: `s${n}`, payload: n });
		}
		// a redelivered id is stored once
		expect(await conductor.signal(id, { name: "input", id: "s2", payload: 2 })).toBe(true);

		await orchestrator.start();
		await waitForCondition(() => received.length === 3);
		await conductor.signal(id, { name: "input", id: "s4", payload: 4 });
		await done.promise;

		expect(received.map((r) => r.id)).toEqual(["s1", "s2", "s3", "s4"]);
		await waitForCondition(async () => !!(await executionRow(db, id))?.completed_at);
		const [row] = await db.sql<{ count: number }[]>`
			select count(*)::int as count from pgconductor._private_execution_signals
			where execution_id = ${id}::uuid and delivered_at is not null
		`;
		expect(row?.count).toBe(4);
	});

	test("signals stored before the claim reach a handler that returns right away", async () => {
		const { db, conductor, orchestrator, received, attempts } = await setup(
			async (state, ctx) => {
				ctx.onSignal("input", collect(state));
			},
			// no poll runs while the execution is claimed
			{ pollIntervalMs: 60_000 },
		);

		const id = await conductor.invoke({ name: "session" }, {});
		for (const n of [1, 2, 3]) {
			await conductor.signal(id, { name: "input", id: `s${n}`, payload: n });
		}
		await orchestrator.start();

		await waitForCondition(async () => !!(await executionRow(db, id))?.completed_at, 5000);
		expect(attempts()).toBe(1);
		expect(received.map((r) => [r.attempt, r.id, r.payload])).toEqual([
			[1, "s1", 1],
			[1, "s2", 2],
			[1, "s3", 3],
		]);
		const [row] = await db.sql<{ attempts: number; undelivered: number }[]>`
			select e.attempts, (
				select count(*)::int from pgconductor._private_execution_signals s
				where s.execution_id = e.id and s.delivered_at is null
			) as undelivered
			from pgconductor._private_executions e where e.id = ${id}::uuid
		`;
		expect(row).toEqual({ attempts: 1, undelivered: 0 });
	});

	test("a re-queued attempt receives its pending signals when it starts", async () => {
		const { db, conductor, orchestrator, received, attempts } = await setup(
			async (state, ctx) => {
				ctx.onSignal("input", collect(state));
			},
			{ pollIntervalMs: 1000 },
		);

		const id = await conductor.invoke({ name: "session" }, {});
		await conductor.signal(id, { name: "input", id: "s1" });

		// store a signal after the first attempt returned and before it settles
		const returnExecutions = conductor.db.returnExecutions.bind(conductor.db);
		let sent = false;
		conductor.db.returnExecutions = async (grouped, opts) => {
			if (!sent && grouped.completed.length > 0) {
				sent = true;
				await conductor.signal(id, { name: "input", id: "s2" });
			}
			return returnExecutions(grouped, opts);
		};

		await orchestrator.start();
		await waitForCondition(async () => !!(await executionRow(db, id))?.completed_at, 10000);

		expect(sent).toBe(true);
		expect(attempts()).toBe(2);
		expect(received.map((r) => [r.attempt, r.id])).toEqual([
			[1, "s1"],
			[2, "s2"],
		]);
	}, 20000);

	test("concurrent signalWithStart calls start one execution and deliver every signal", async () => {
		const total = 20;
		const done = new Deferred<void>();
		const { db, orchestrator, received } = await setup(async (state, ctx) => {
			ctx.onSignal("input", (payload, info) => {
				collect(state)(payload, info);
				if (new Set(state.received.map((r) => r.id)).size === total) done.resolve();
			});
			await done.promise;
		});

		// a pool of its own, so the calls really run concurrently
		const sql = postgres(db.url, { max: total });
		cleanups.push(() => sql.end());
		const sender = createConductor(db, sql);

		await orchestrator.start();
		const ids = await Promise.all(
			Array.from({ length: total }, (_, n) =>
				sender.signalWithStart({
					task: "session",
					key: "thread-1",
					signal: { name: "input", id: `s${n}`, payload: n },
					input: { topic: "t" },
				}),
			),
		);

		expect(new Set(ids).size).toBe(1);
		await done.promise;
		const [executionId] = ids;
		await waitForCondition(async () => !!(await executionRow(db, executionId!))?.completed_at);

		const rows = await db.sql`
			select id from pgconductor._private_executions where key = 'thread-1'
		`;
		expect(rows).toHaveLength(1);
		expect(new Set(received.map((r) => r.id)).size).toBe(total);

		// the key is free again once the execution finished
		const next = await sender.signalWithStart({
			task: "session",
			key: "thread-1",
			signal: { name: "input", id: "later" },
		});
		expect(next).not.toBe(executionId);
	});

	test("a signal committed while settlement waits for the execution re-queues it", async () => {
		const { db, conductor, orchestrator, received, attempts } = await setup(async (state, ctx) => {
			ctx.onSignal("input", collect(state));
			await waitForCondition(() => state.received.some((r) => r.attempt === state.attempt));
		});

		const id = await conductor.invoke({ name: "session" }, {});
		await conductor.signal(id, { name: "input", id: "s1" });

		// hold the execution row in another transaction that stores a signal, so the completion's
		// snapshot is older than the signal it must not drop
		const sql = postgres(db.url, { max: 1 });
		cleanups.push(() => sql.end());
		const returnExecutions = conductor.db.returnExecutions.bind(conductor.db);
		let held = false;
		conductor.db.returnExecutions = async (grouped, opts) => {
			if (held || grouped.completed.length === 0) return returnExecutions(grouped, opts);
			held = true;
			const locked = new Deferred<void>();
			const commit = new Deferred<void>();
			const tx = sql.begin(async (tx) => {
				await tx`select pgconductor.signal_execution(${id}::uuid, 'input', 's2', null)`;
				locked.resolve();
				await commit.promise;
			});
			await locked.promise;
			const settled = returnExecutions(grouped, opts);
			await Bun.sleep(200);
			commit.resolve();
			await tx;
			return settled;
		};

		await orchestrator.start();
		await waitForCondition(async () => !!(await executionRow(db, id))?.completed_at);

		expect(held).toBe(true);
		expect(attempts()).toBe(2);
		expect(received.map((r) => [r.attempt, r.id])).toEqual([
			[1, "s1"],
			[2, "s2"],
		]);
	}, 20000);

	test("a signal sent after the handler returned re-queues the execution", async () => {
		const returned = new Deferred<void>();
		const { db, conductor, orchestrator, received, attempts } = await setup(
			async (state, ctx) => {
				ctx.onSignal("input", collect(state));
				await waitForCondition(() => state.received.some((r) => r.attempt === state.attempt));
				if (state.attempt === 1) returned.resolve();
			},
			{ flushIntervalMs: 1000 },
		);

		const id = await conductor.invoke({ name: "session" }, {});
		await conductor.signal(id, { name: "input", id: "s1" });
		await orchestrator.start();
		await returned.promise;
		await conductor.signal(id, { name: "input", id: "s2" });

		await waitForCondition(async () => !!(await executionRow(db, id))?.completed_at);
		expect(attempts()).toBe(2);
		expect(received.map((r) => [r.attempt, r.id])).toEqual([
			[1, "s1"],
			[2, "s2"],
		]);
	}, 20000);

	test("signals for unknown names do not keep an execution alive", async () => {
		const { db, conductor, orchestrator, attempts } = await setup(async (_state, ctx) => {
			ctx.onSignal("input", () => {});
		});

		const id = await conductor.invoke({ name: "session" }, {});
		await conductor.signal(id, { name: "other", id: "o1" });
		await orchestrator.start();

		await waitForCondition(async () => !!(await executionRow(db, id))?.completed_at);
		expect(attempts()).toBe(1);
	});

	test("a signal reached before its handler registered re-queues the execution", async () => {
		const { db, conductor, orchestrator, received, attempts } = await setup(async (state, ctx) => {
			ctx.onSignal("input", collect(state));
			if (state.attempt === 1) await waitForCondition(() => state.received.length > 0);
			ctx.onSignal("late", collect(state));
		});

		const id = await conductor.invoke({ name: "session" }, {});
		await conductor.signal(id, { name: "late", id: "l1" });
		await conductor.signal(id, { name: "input", id: "i1" });
		await orchestrator.start();

		await waitForCondition(async () => !!(await executionRow(db, id))?.completed_at);
		expect(attempts()).toBe(2);
		expect(received.map((r) => [r.attempt, r.id])).toEqual([
			[1, "i1"],
			[2, "l1"],
		]);
	});

	test("retrying and sleeping executions receive signals when they run again", async () => {
		const { db, conductor, orchestrator, received } = await setup(async (state, ctx) => {
			if (state.attempt === 1) throw new Error("first attempt fails");
			ctx.onSignal("input", collect(state));
			await waitForCondition(() => state.received.length >= 1);
			await ctx.sleep("nap", 60_000);
			await waitForCondition(() => state.received.length === 2);
		});

		const id = await conductor.invoke({ name: "session" }, {});
		await orchestrator.start();

		// retrying: the first attempt failed and backs off
		await waitForCondition(async () => {
			const [row] = await db.sql`
				select last_error from pgconductor._private_executions where id = ${id}::uuid and locked_by is null
			`;
			return row?.last_error === "first attempt fails";
		});
		await conductor.signal(id, { name: "input", id: "while-retrying" });
		await db.sql`update pgconductor._private_executions set run_at = now() where id = ${id}::uuid`;

		// sleeping: the second attempt released the execution until the nap ends
		await waitForCondition(() => received.length === 1);
		await waitForCondition(async () => {
			const [step] = await db.sql`
				select 1 from pgconductor._private_steps where execution_id = ${id}::uuid and key = 'nap'
			`;
			return !!step && !(await executionRow(db, id))?.locked_by;
		});
		await conductor.signal(id, { name: "input", id: "while-sleeping" });
		await db.sql`update pgconductor._private_executions set run_at = now() where id = ${id}::uuid`;

		await waitForCondition(async () => !!(await executionRow(db, id))?.completed_at);
		expect(received.map((r) => [r.attempt, r.id])).toEqual([
			[2, "while-retrying"],
			[3, "while-sleeping"],
		]);
	}, 20000);

	test("a throwing signal handler fails the attempt and the retry receives the signal again", async () => {
		const { db, conductor, orchestrator, received } = await setup(async (state, ctx) => {
			ctx.onSignal("input", (payload, info) => {
				collect(state)(payload, info);
				if (state.attempt === 1) throw new Error("boom");
			});
			await waitForCondition(() => state.received.some((r) => r.attempt === state.attempt));
			if (state.attempt === 1) await new Promise(() => {});
		});

		const id = await conductor.invoke({ name: "session" }, {});
		await conductor.signal(id, { name: "input", id: "s1" });
		await orchestrator.start();

		await waitForCondition(async () => {
			const [row] = await db.sql`
				select last_error from pgconductor._private_executions where id = ${id}::uuid and locked_by is null
			`;
			return row?.last_error === 'Signal handler for "input" failed: boom';
		});
		await db.sql`update pgconductor._private_executions set run_at = now() where id = ${id}::uuid`;

		await waitForCondition(async () => !!(await executionRow(db, id))?.completed_at);
		expect(received.map((r) => [r.attempt, r.id])).toEqual([
			[1, "s1"],
			[2, "s1"],
		]);
	}, 20000);

	test("a task in another orchestrator signals a running execution", async () => {
		const got = new Deferred<void>();
		const started = new Deferred<void>();
		const { db, conductor, orchestrator, received } = await setup(async (state, ctx) => {
			ctx.onSignal("input", (payload, info) => {
				collect(state)(payload, info);
				got.resolve();
			});
			started.resolve();
			await got.promise;
		});

		const sql = postgres(db.url, { max: 2 });
		cleanups.push(() => sql.end());
		const senderConductor = createConductor(db, sql);
		const senderTask = senderConductor.createTask(
			{ name: "sender", queue: "senders" },
			{ invocable: true },
			async (event, ctx) => {
				await ctx.sendSignal(event.payload.target, {
					name: "input",
					id: "from-task",
					payload: "hello",
				});
			},
		);
		const senderOrchestrator = Orchestrator.create({
			conductor: senderConductor,
			workers: [
				senderConductor.createWorker({
					queue: "senders",
					tasks: [senderTask],
					config: { pollIntervalMs: 50, flushIntervalMs: 50 },
				}),
			],
		});
		cleanups.push(() => senderOrchestrator.stop());

		const id = await conductor.invoke({ name: "session" }, {});
		await orchestrator.start();
		await started.promise;
		await senderOrchestrator.start();
		await senderConductor.invoke({ name: "sender", queue: "senders" }, { target: id });

		await got.promise;
		expect(received.map((r) => [r.id, r.payload])).toEqual([["from-task", "hello"]]);
	});

	test("ctx.signalWithStart is memoized by its step", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = createConductor(db);
		let starts = 0;
		const session = conductor.createTask({ name: "session" }, { invocable: true }, async () => {});
		const sender = conductor.createTask(
			{ name: "sender", queue: "senders" },
			{ invocable: true },
			async (_event, ctx) => {
				const first = await ctx.signalWithStart("start", {
					task: "session",
					key: "k",
					signal: { name: "input", id: `s${++starts}` },
				});
				const again = await ctx.signalWithStart("start", {
					task: "session",
					key: "k",
					signal: { name: "input", id: "never" },
				});
				expect(again).toBe(first);
			},
		);
		await conductor.ensureInstalled();
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [session],
			workers: [
				conductor.createWorker({
					queue: "senders",
					tasks: [sender],
					config: { pollIntervalMs: 50, flushIntervalMs: 50 },
				}),
			],
		});
		cleanups.push(() => orchestrator.stop());

		await orchestrator.start();
		const senderId = await conductor.invoke({ name: "sender", queue: "senders" }, { target: "" });
		await waitForCondition(async () => !!(await executionRow(db, senderId))?.completed_at);

		const signals = await db.sql`select id from pgconductor._private_execution_signals`;
		expect(signals.map((s) => s.id)).toEqual(["s1"]);
	});

	test("a stale claim neither reads nor acknowledges signals", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = createConductor(db);
		await conductor.ensureInstalled();
		await db.client.registerWorker({
			queueName: "default",
			taskSpecs: [{ key: "session", queue: "default", maxAttempts: 3 }],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		const id = await conductor.invoke({ name: "session" }, {});
		await conductor.signal(id, { name: "input", id: "s1" });

		const orchestratorId = crypto.randomUUID();
		const claim = async () => {
			const [execution] = (
				await db.client.getExecutions({
					orchestratorId,
					queueName: "default",
					batchSize: 1,
					taskKeys: ["session"],
				})
			).executions;
			if (!execution) throw new Error("expected claim");
			return execution;
		};
		const first = await claim();
		await db.client.returnExecutions({
			orchestratorId,
			completed: [],
			failed: [],
			released: [
				{
					execution_id: id,
					claim_token: first.claim_token,
					queue: "default",
					task_key: "session",
					status: "released",
					reschedule_in_ms: 0,
				},
			],
			invokeChild: [],
		});
		const second = await claim();

		const poll = async (claimToken: string) => {
			const { signals } = await db.client.getExecutions({
				orchestratorId,
				queueName: "default",
				batchSize: 0,
				taskKeys: [],
				running: [{ execution_id: id, claim_token: claimToken, after_seq: 0 }],
			});
			return signals.map((s) => s.id);
		};
		expect(await poll(first.claim_token)).toEqual([]);
		expect(await poll(second.claim_token)).toEqual(["s1"]);

		const ack = (claimToken: string) =>
			db.client.ackSignals({
				executionId: id,
				queue: "default",
				orchestratorId,
				claimToken,
				ids: ["s1"],
			});
		const delivered = async () => {
			const [row] = await db.sql<{ delivered_at: Date | null }[]>`
				select delivered_at from pgconductor._private_execution_signals where id = 's1'
			`;
			return !!row?.delivered_at;
		};
		await ack(first.claim_token);
		expect(await delivered()).toBe(false);
		await ack(second.claim_token);
		expect(await delivered()).toBe(true);
	});

	test("cancellation reaches a running execution on the poll and drops its signals", async () => {
		const started = new Deferred<void>();
		const { db, conductor, orchestrator, received } = await setup(async (state, ctx) => {
			ctx.onSignal("input", collect(state));
			started.resolve();
			await new Promise((resolve) => ctx.signal.addEventListener("abort", resolve));
		});

		const id = await conductor.invoke({ name: "session" }, {});
		await orchestrator.start();
		await started.promise;

		const cancelledAt = Date.now();
		expect(await conductor.cancel(id)).toBe(true);
		await waitForCondition(async () => !!(await executionRow(db, id))?.failed_at, 5000);
		// aborted on the next poll (50 ms) rather than the 30 s heartbeat; settled after the flush
		expect(Date.now() - cancelledAt).toBeLessThan(1000);

		expect(await conductor.signal(id, { name: "input", id: "late" })).toBe(false);
		expect(received).toEqual([]);

		// a cancelled execution does not hold its key
		const [keyed] = await db.sql<{ id: string }[]>`
			update pgconductor._private_executions set key = 'k' where id = ${id}::uuid returning id
		`;
		const next = await conductor.signalWithStart({
			task: "session",
			key: "k",
			signal: { name: "input", id: "after-cancel" },
		});
		expect(next).not.toBe(keyed?.id);
	});
});
