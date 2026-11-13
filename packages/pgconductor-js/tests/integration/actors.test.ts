import postgres from "postgres";
import { z } from "zod";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import type { DatabaseClient, JsonValue } from "../../src/database-client";
import type { TaskContext } from "../../src/task-context";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const counterDefinition = defineTask({
	name: "counter",
	payload: z.object({}),
});
const tallyDefinition = defineTask({
	name: "tally",
	payload: z.object({}),
	returns: z.object({ total: z.number() }),
});
const tasks = TaskSchemas.fromSchema([counterDefinition, tallyDefinition]);

type Received = { activation: number; id: string; payload: JsonValue };

describe("Actors", () => {
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
		activate: (activation: number, ctx: TaskContext, received: Received[]) => Promise<void>,
		{ maxAttempts }: { maxAttempts?: number } = {},
	) {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({ sql: db.sql, tasks, context: {}, clock: db.clock });
		const received: Received[] = [];
		let activations = 0;

		const task = conductor.createTask(
			{ name: "counter", actor: true, maxAttempts },
			{ invocable: true },
			async (_event, ctx) => {
				const activation = ++activations;
				ctx.onSignal("message", (payload, { id }) => {
					received.push({ activation, id, payload });
				});
				await activate(activation, ctx, received);
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 20 },
		});
		cleanups.push(() => orchestrator.stop());

		await conductor.ensureInstalled();
		await orchestrator.start();

		const send = (id: string, payload: JsonValue = null) =>
			conductor
				.signalWithStart({
					task: "counter",
					key: "k1",
					signal: { name: "message", id, payload },
				})
				.then(({ id }) => id);
		const row = async (id: string) => {
			const [execution] = await db.sql<
				{
					attempts: number;
					run_at: Date;
					locked_by: string | null;
					waiting_step_key: string | null;
					completed_at: Date | null;
					failed_at: Date | null;
				}[]
			>`select attempts, run_at, locked_by, waiting_step_key, completed_at, failed_at
				from pgconductor._private_executions where id = ${id}`;
			return execution;
		};
		const idle = async (id: string) => {
			await waitForCondition(
				async () => (await row(id))?.waiting_step_key === "pgconductor.idle",
				5000,
			);
			return row(id);
		};

		return { db, conductor, received, activations: () => activations, send, row, idle };
	}

	// Gives an actor the history of a long-lived one: signals delivered by earlier activations
	async function storeDeliveredSignals(db: TestDatabase, id: string, count: number) {
		await db.sql`
			with stored as (
				insert into pgconductor._private_execution_signals (execution_id, queue, id, name, delivered_at)
				select ${id}::uuid, 'default', 'old-' || n, 'message', pgconductor._private_current_time()
				from generate_series(1, ${count}::int) n
				returning 1
			)
			update pgconductor._private_executions
			set signal_count = signal_count + (select count(*) from stored)
			where id = ${id}::uuid
		`;
	}

	// Holds the execution row in another transaction that stores a signal while the next idle
	// settlement waits for it, so the settlement's snapshot is older than the signal
	function storeDuringIdleSettlement(
		db: TestDatabase,
		client: DatabaseClient,
		store: (tx: postgres.TransactionSql) => Promise<unknown>,
	) {
		const sql = postgres(db.url, { max: 1 });
		cleanups.push(() => sql.end());
		const returnExecutions = client.returnExecutions.bind(client);
		const held = new Deferred<void>();
		let holding = false;
		client.returnExecutions = async (args, opts) => {
			if (holding || !args.results.some((r) => r.status === "idle")) {
				return returnExecutions(args, opts);
			}
			holding = true;
			held.resolve();
			const locked = new Deferred<void>();
			const commit = new Deferred<void>();
			const tx = sql.begin(async (tx) => {
				await store(tx);
				locked.resolve();
				await commit.promise;
			});
			await locked.promise;
			const settled = returnExecutions(args, opts);
			await new Promise((resolve) => setTimeout(resolve, 200));
			commit.resolve();
			await tx;
			return settled;
		};
		return held.promise;
	}

	test("sleeps when an activation returns, and a signal wakes the same execution", async () => {
		const { received, activations, send, idle } = await setup(async () => {});

		const id = await send("s1", 1);
		const sleeping = await idle(id);
		expect(received).toEqual([{ activation: 1, id: "s1", payload: 1 }]);
		expect(sleeping).toMatchObject({ attempts: 0, locked_by: null, completed_at: null });
		// infinity reads back as an invalid date
		expect(Number.isNaN(sleeping?.run_at.getTime())).toBe(true);

		expect(await send("s2", 2)).toBe(id);
		await waitForCondition(() => received.length === 2, 5000);
		await idle(id);
		expect(received[1]).toEqual({ activation: 2, id: "s2", payload: 2 });
		expect(activations()).toBe(2);

		// A signal id is stored once, so a resend wakes nothing
		expect(await send("s2", 2)).toBe(id);
		await idle(id);
		expect(activations()).toBe(2);
	});

	test("an alarm wakes an activation without a signal", async () => {
		const wakes: number[] = [];
		const { db, send, idle } = await setup(async (activation, ctx) => {
			wakes.push(activation);
			if (activation === 1) ctx.setAlarm(new Date(db.clock.now().getTime() + 60 * 60 * 1000));
		});

		const id = await send("s1");
		const sleeping = await idle(id);
		expect(sleeping?.run_at.getTime()).toBeGreaterThan(db.clock.now().getTime() + 59 * 60 * 1000);

		await db.clock.advance("2h");
		await waitForCondition(() => wakes.length === 2, 5000);
		const after = await idle(id);
		// The second activation set no alarm
		expect(Number.isNaN(after?.run_at.getTime())).toBe(true);
	});

	test("untilIdle waits out signals delivered meanwhile, and later ones start the next activation", async () => {
		const firstWait = new Deferred<void>();
		const sentLate = new Deferred<void>();
		let waits = 0;
		const { conductor, received, send, idle } = await setup(async (activation, ctx) => {
			if (activation > 1) return;
			await ctx.untilIdle(async () => {
				waits++;
				if (waits === 1) await firstWait.promise;
			});
			await sentLate.promise;
		});

		const id = await send("s1", 1);
		await waitForCondition(() => waits === 1, 5000);
		await conductor.signal(id, { name: "message", id: "s2", payload: 2 });
		await waitForCondition(() => received.length === 2, 5000);
		firstWait.resolve();
		await waitForCondition(() => waits === 2, 5000);

		// Delivery stopped: this one waits for the next activation
		await conductor.signal(id, { name: "message", id: "s3", payload: 3 });
		await new Promise((resolve) => setTimeout(resolve, 100));
		expect(received.map((r) => r.id)).toEqual(["s1", "s2"]);
		sentLate.resolve();

		await waitForCondition(() => received.length === 3, 5000);
		await idle(id);
		expect(received).toEqual([
			{ activation: 1, id: "s1", payload: 1 },
			{ activation: 1, id: "s2", payload: 2 },
			{ activation: 2, id: "s3", payload: 3 },
		]);
		expect(waits).toBe(2);
	});

	test("a failed activation retries, and a clean one resets its attempts", async () => {
		const { db, activations, send, idle, row } = await setup(async (activation) => {
			if (activation === 1) throw new Error("boom");
		});

		const id = await send("s1");
		await waitForCondition(async () => {
			const execution = await row(id);
			return execution?.attempts === 1 && execution.locked_by === null;
		}, 5000);
		expect((await row(id))?.waiting_step_key).toBeNull();
		await db.clock.advance("1m");
		await idle(id);
		expect(activations()).toBe(2);
		expect((await row(id))?.attempts).toBe(0);
	});

	test("cancelling ends an actor, and the next signal starts a new execution", async () => {
		const { conductor, send, idle, row } = await setup(async () => {});

		const id = await send("s1");
		await idle(id);
		expect(await conductor.cancel(id)).toBe(true);
		expect((await row(id))?.failed_at).not.toBeNull();

		const next = await send("s2");
		expect(next).not.toBe(id);
		await idle(next);
	});

	test("an activation that cancels its own execution ends the actor", async () => {
		const { conductor, activations, send, row } = await setup(async (activation, ctx) => {
			if (activation === 2) await conductor.cancel(ctx.executionId, { reason: "done for good" });
		});

		const id = await send("s1");
		await waitForCondition(
			async () => (await row(id))?.waiting_step_key === "pgconductor.idle",
			5000,
		);
		await send("s2");
		await waitForCondition(async () => (await row(id))?.failed_at != null, 5000);
		expect(await conductor.getExecution(id)).toMatchObject({
			status: "cancelled",
			error: "done for good",
		});
		expect(activations()).toBe(2);
	});

	test("a delayed signal wakes an idle actor when due, before its alarm", async () => {
		const hour = 60 * 60 * 1000;
		const { db, conductor, received, activations, send, row, idle } = await setup(
			async (activation, ctx) => {
				if (activation === 1) ctx.setAlarm(new Date(db.clock.now().getTime() + 3 * hour));
			},
		);

		const id = await send("s1", 1);
		await idle(id);
		const at = new Date(db.clock.now().getTime() + hour);
		const later = { name: "message", id: "later", payload: 2 };
		expect(await conductor.signal(id, later, { at })).toBe(true);
		// Idempotent by id: a resend stores nothing
		expect(await conductor.signal(id, later, { at })).toBe(true);
		const [{ count }] = await db.sql<[{ count: number }]>`
			select count(*)::int from pgconductor._private_execution_signals where execution_id = ${id}
		`;
		expect(count).toBe(2);

		// It sleeps until the signal is due, which is before its alarm, and the signal waits
		expect(await row(id)).toMatchObject({ waiting_step_key: "pgconductor.idle", run_at: at });
		await new Promise((resolve) => setTimeout(resolve, 150));
		expect(received.map((r) => r.id)).toEqual(["s1"]);
		expect(activations()).toBe(1);

		await db.clock.advance("2h");
		await waitForCondition(() => received.length === 2, 5000);
		expect(received[1]).toEqual({ activation: 2, id: "later", payload: 2 });
		await idle(id);
		expect(activations()).toBe(2);
	});

	test("an actor with a long signal history settles without counting it, and a signal stored meanwhile wakes it", async () => {
		const { db, conductor, received, activations, send, idle } = await setup(async () => {});

		const id = await send("s1");
		await idle(id);
		await storeDeliveredSignals(db, id, 5000);
		const held = storeDuringIdleSettlement(db, conductor.db, (tx) => {
			return tx`select pgconductor.signal_execution(${id}::uuid, 'message', 's3', null)`;
		});

		await send("s2");
		await held;
		await waitForCondition(() => received.length === 3, 5000);
		await idle(id);
		expect(received.map((r) => [r.activation, r.id])).toEqual([
			[1, "s1"],
			[2, "s2"],
			[3, "s3"],
		]);
		expect(activations()).toBe(3);
	});

	test("a delayed signal stored while an actor settles idle wakes it when due", async () => {
		const hour = 60 * 60 * 1000;
		const { db, conductor, received, activations, send, idle } = await setup(async () => {});

		const id = await send("s1");
		await idle(id);
		await storeDeliveredSignals(db, id, 100);
		const at = new Date(db.clock.now().getTime() + hour);
		const held = storeDuringIdleSettlement(db, conductor.db, (tx) => {
			return tx`select pgconductor.signal_execution(${id}::uuid, 'message', 'later', null, ${at})`;
		});

		await send("s2");
		await held;
		// The settlement could not see the signal, so it starts another activation, which sleeps
		// until the signal is due
		await waitForCondition(() => activations() === 3, 5000);
		expect(await idle(id)).toMatchObject({ run_at: at });
		expect(received.map((r) => r.id)).toEqual(["s1", "s2"]);

		await db.clock.advance("2h");
		await waitForCondition(() => received.length === 3, 5000);
		expect(received[2]).toMatchObject({ activation: 4, id: "later" });
	});

	test("a running activation receives a delayed signal when due, and the next one gets a later one", async () => {
		const hour = 60 * 60 * 1000;
		const expired = new Deferred<void>();
		const expiries: string[] = [];
		let tomorrow: Date | undefined;
		const { db, received, send, row, idle } = await setup(async (activation, ctx) => {
			if (activation !== 1) return;
			ctx.onSignal("expire", (_payload, { id }) => {
				expiries.push(id);
				expired.resolve();
			});
			// The activation expires itself at a time; it keeps running until then
			const now = db.clock.now().getTime();
			const expiresAt = new Date(now + hour);
			await ctx.sendSignal(
				ctx.executionId,
				{ name: "expire", id: `expire:${expiresAt.toISOString()}` },
				{ at: expiresAt },
			);
			tomorrow = new Date(now + 24 * hour);
			await ctx.sendSignal(ctx.executionId, { name: "message", id: "tomorrow" }, { at: tomorrow });
			await ctx.untilIdle(() => expired.promise);
		});

		const id = await send("s1");
		await waitForCondition(() => received.length === 1, 5000);
		await new Promise((resolve) => setTimeout(resolve, 150));
		expect(expiries).toEqual([]);
		expect((await row(id))?.locked_by).not.toBeNull();

		await db.clock.advance("2h");
		const sleeping = await idle(id);
		expect(expiries).toHaveLength(1);
		// Not due when the activation returned: it sleeps until the signal is
		expect(sleeping?.run_at).toEqual(tomorrow);
		expect(received.map((r) => r.id)).toEqual(["s1"]);

		await db.clock.advance("1d");
		await waitForCondition(() => received.length === 2, 5000);
		expect(received[1]).toMatchObject({ activation: 2, id: "tomorrow" });
	});

	test("ctx.complete ends an actor with its result, and nothing re-opens it", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({ sql: db.sql, tasks, context: {}, clock: db.clock });
		let activations = 0;
		const createdAt: Date[] = [];
		const added: string[] = [];
		const tally = conductor.createTask(
			{ name: "tally", actor: true },
			{ invocable: true },
			async (_event, ctx) => {
				activations++;
				createdAt.push(ctx.createdAt);
				ctx.onSignal("add", (_payload, { id }) => {
					added.push(id);
				});
				// Ends the actor from a handler while untilIdle waits for it
				ctx.onSignal("done", async (payload) => {
					// Stored meanwhile: it neither re-queues the completion nor is delivered
					await conductor.signal(ctx.executionId, { name: "add", id: "meanwhile" });
					await ctx.complete({ total: Number(payload) });
				});
				await ctx.untilIdle(async () => {});
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [tally],
			defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 20 },
		});
		cleanups.push(() => orchestrator.stop());
		await conductor.ensureInstalled();
		await orchestrator.start();

		const start = await conductor.signalWithStart({
			task: "tally",
			key: "k",
			signal: { name: "done", id: "d1", payload: 3 },
		});
		expect(start.started).toBe(true);
		const { id } = start;
		expect(await conductor.waitForResult({ name: "tally" }, id, { pollIntervalMs: 10 })).toEqual({
			total: 3,
		});
		const execution = await conductor.getExecution(id);
		expect(execution).toMatchObject({ status: "completed", result: { total: 3 } });
		expect(createdAt).toEqual([execution?.createdAt]);

		// Ended for good: a signal is refused, and the key starts a new execution
		expect(await conductor.signal(id, { name: "add", id: "later" })).toBe(false);
		const next = await conductor.signalWithStart({
			task: "tally",
			key: "k",
			signal: { name: "add", id: "a1" },
		});
		expect(next.started).toBe(true);
		expect(next.id).not.toBe(id);
		await waitForCondition(() => added.includes("a1"), 5000);
		expect(await conductor.getExecution(id)).toMatchObject({ status: "completed" });
		expect(added).toEqual(["a1"]);
		expect(activations).toBe(2);
	});

	test("an actor cannot process batches", () => {
		const conductor = Conductor.create({
			connectionString: "postgres://localhost/unused",
			tasks,
			context: {},
		});
		expect(() =>
			conductor.createTask(
				{ name: "counter", actor: true, batch: { size: 2, timeoutMs: 10 } },
				{ invocable: true },
				async () => {},
			),
		).toThrow("An actor cannot process batches");
	});
});
