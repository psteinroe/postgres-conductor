import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { PermanentError } from "../../src/permanent-error";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const empty = z.object({});
const item = z.object({ value: z.number() });
const definitions = [
	defineTask({ name: "direct", payload: empty }),
	defineTask({ name: "in-step", payload: empty }),
	defineTask({ name: "guarded", payload: empty }),
	defineTask({ name: "flaky", payload: empty }),
	defineTask({ name: "listener", payload: empty }),
	defineTask({ name: "charge", payload: empty }),
	defineTask({ name: "charge-failed", queue: "dlq", payload: empty }),
	defineTask({ name: "items", queue: "batches", payload: item, returns: item }),
	defineTask({ name: "all-items", queue: "batches", payload: item }),
];

type Row = {
	task_key: string;
	attempts: number;
	last_error: string | null;
	failed_at: Date | null;
	completed_at: Date | null;
	result: unknown;
	payload: unknown;
};

describe("PermanentError", () => {
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

	async function setup() {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(definitions),
			context: {},
			middleware: [
				async ({ execution }, next) => {
					if (execution.task_key === "guarded") throw new PermanentError("not allowed");
					return next({});
				},
			],
		});
		await conductor.ensureInstalled();
		const row = async (id: string) => {
			const [execution] = await db.sql<Row[]>`
				select task_key, attempts, last_error, failed_at, completed_at, result, payload
				from pgconductor._private_executions where id = ${id}`;
			return execution;
		};
		return { db, conductor, row };
	}

	test("thrown by a handler, it fails the execution at once; another error retries", async () => {
		const { conductor, row } = await setup();
		const direct = conductor.createTask(
			{ name: "direct", maxAttempts: 3 },
			{ invocable: true },
			async () => {
				throw new PermanentError("invalid input");
			},
		);
		const flaky = conductor.createTask(
			{ name: "flaky", maxAttempts: 3 },
			{ invocable: true },
			async () => {
				throw new Error("try again");
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [direct, flaky],
			defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 20 },
		});

		const permanent = await conductor.invoke({ name: "direct" }, {});
		const retried = await conductor.invoke({ name: "flaky" }, {});
		await orchestrator.drain();

		expect(await row(permanent)).toMatchObject({
			attempts: 1,
			last_error: "invalid input",
			failed_at: expect.any(Date),
		});
		expect(await row(retried)).toMatchObject({
			attempts: 1,
			last_error: "try again",
			failed_at: null,
		});
	});

	test("thrown in a step, it is not memoized and fails the execution at once", async () => {
		const { db, conductor, row } = await setup();
		const inStep = conductor.createTask(
			{ name: "in-step", maxAttempts: 3 },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.step("before", () => 1);
				await ctx.step("check", async () => {
					throw new PermanentError("bad order");
				});
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [inStep],
			defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 20 },
		});

		const id = await conductor.invoke({ name: "in-step" }, {});
		await orchestrator.drain();

		expect(await row(id)).toMatchObject({
			attempts: 1,
			last_error: "bad order",
			failed_at: expect.any(Date),
		});
		const steps = await db.sql<{ key: string }[]>`
			select key from pgconductor._private_steps where execution_id = ${id}`;
		expect(steps.map((step) => step.key)).toEqual(["before"]);
	});

	test("thrown by middleware or a signal handler, it fails the execution at once", async () => {
		const { conductor, row } = await setup();
		let handlerRan = false;
		const guarded = conductor.createTask(
			{ name: "guarded", maxAttempts: 3 },
			{ invocable: true },
			async () => {
				handlerRan = true;
			},
		);
		const listener = conductor.createTask(
			{ name: "listener", maxAttempts: 3 },
			{ invocable: true },
			async (_event, ctx) => {
				ctx.onSignal("order", () => {
					throw new PermanentError("unknown order");
				});
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [guarded, listener],
			defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 20 },
		});

		const fromMiddleware = await conductor.invoke({ name: "guarded" }, {});
		const fromSignal = await conductor.invoke({ name: "listener" }, {});
		await conductor.signal(fromSignal, { name: "order", id: "o1" });
		await orchestrator.drain();

		expect(handlerRan).toBe(false);
		expect(await row(fromMiddleware)).toMatchObject({
			attempts: 1,
			last_error: "not allowed",
			failed_at: expect.any(Date),
		});
		expect(await row(fromSignal)).toMatchObject({
			attempts: 1,
			last_error: 'Signal handler for "order" failed: unknown order',
			failed_at: expect.any(Date),
		});
	});

	test("a dead-letter task receives the failure once, without retries", async () => {
		const { db, conductor } = await setup();
		const delivered: { attempts: number; error: string | null }[] = [];
		const destination = conductor.createTask(
			{ name: "charge-failed", queue: "dlq" },
			{ invocable: true },
			async (_event, ctx) => {
				if (ctx.deadLetter) {
					delivered.push({ attempts: ctx.deadLetter.attempts, error: ctx.deadLetter.error });
				}
			},
		);
		const charge = conductor.createTask(
			{ name: "charge", maxAttempts: 3, deadLetter: { queue: "dlq", task: destination } },
			{ invocable: true },
			async () => {
				throw new PermanentError("card number invalid");
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [charge],
			workers: [
				conductor.createWorker({
					queue: "dlq",
					tasks: [destination],
					config: { pollIntervalMs: 20, flushIntervalMs: 20 },
				}),
			],
			defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 20 },
		});
		await orchestrator.start();
		try {
			await conductor.invoke({ name: "charge" }, {});
			await waitForCondition(() => delivered.length === 1);
		} finally {
			await orchestrator.stop();
		}

		expect(delivered).toEqual([{ attempts: 1, error: "card number invalid" }]);
		const [{ count }] = await db.sql<[{ count: number }]>`
			select count(*)::int as count from pgconductor._private_executions
			where task_key = 'charge-failed'`;
		expect(count).toBe(1);
	});

	test("returned for a batch member, it fails only that member; thrown, it fails the batch", async () => {
		const { conductor, row } = await setup();
		const items = conductor.createTask(
			{ name: "items", queue: "batches", maxAttempts: 3, batch: { size: 3, timeoutMs: 100 } },
			{ invocable: true },
			async (events) =>
				events.map((event) =>
					event.payload.value < 0 ? new PermanentError("negative value") : event.payload,
				),
		);
		const allItems = conductor.createTask(
			{
				name: "all-items",
				queue: "batches",
				maxAttempts: 3,
				batch: { size: 3, timeoutMs: 100 },
			},
			{ invocable: true },
			async () => {
				throw new PermanentError("export rejected");
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			workers: [
				conductor.createWorker({
					queue: "batches",
					tasks: [items, allItems],
					config: { pollIntervalMs: 20, flushIntervalMs: 20 },
				}),
			],
		});

		// Registration creates the queue's partition
		await orchestrator.start();
		const ids = await conductor.invoke({ name: "items", queue: "batches" }, [
			{ payload: { value: 1 } },
			{ payload: { value: -1 } },
			{ payload: { value: 2 } },
		]);
		const rejected = await conductor.invoke({ name: "all-items", queue: "batches" }, [
			{ payload: { value: 1 } },
			{ payload: { value: 2 } },
		]);
		try {
			await waitForCondition(async () => {
				const rows = await Promise.all([...ids, ...rejected].map((id) => row(id || "")));
				return rows.every((execution) => execution?.completed_at || execution?.failed_at);
			});
		} finally {
			await orchestrator.stop();
		}

		const [first, second, third] = await Promise.all(ids.map((id) => row(id || "")));
		expect(first).toMatchObject({ completed_at: expect.any(Date), result: { value: 1 } });
		expect(second).toMatchObject({
			attempts: 1,
			last_error: "negative value",
			failed_at: expect.any(Date),
		});
		expect(third).toMatchObject({ completed_at: expect.any(Date), result: { value: 2 } });
		for (const id of rejected) {
			expect(await row(id || "")).toMatchObject({
				attempts: 1,
				last_error: "export rejected",
				failed_at: expect.any(Date),
			});
		}
	});

	test("its brand is recognized across copies of the package", () => {
		const error = new PermanentError("invalid input");
		expect(error).toBeInstanceOf(Error);
		expect(error.name).toBe("PermanentError");
		expect(error.message).toBe("invalid input");
		expect(Object.getOwnPropertySymbols(error)).toContain(
			Symbol.for("pgconductor.permanent-error"),
		);
	});
});
