import { z } from "zod";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import type { JsonValue } from "../../src/database-client";
import type { TaskContext } from "../../src/task-context";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const counterDefinition = defineTask({
	name: "counter",
	payload: z.object({}),
});
const tasks = TaskSchemas.fromSchema([counterDefinition]);

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
			conductor.signalWithStart({
				task: "counter",
				key: "k1",
				signal: { name: "message", id, payload },
			});
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
