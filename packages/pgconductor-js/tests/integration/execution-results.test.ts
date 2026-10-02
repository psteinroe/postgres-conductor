import { z } from "zod";
import { test, expect, describe, beforeAll, afterAll, afterEach } from "bun:test";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { TaskSchemas } from "../../src/schemas";
import { waitForCondition } from "../test-utils";

const addDefinition = defineTask({
	name: "add",
	payload: z.object({ a: z.number(), b: z.number() }),
	returns: z.object({ sum: z.number() }),
});

const failingDefinition = defineTask({
	name: "failing",
	payload: z.object({}),
});

const slowDefinition = defineTask({
	name: "slow",
	payload: z.object({}),
});

describe("Execution results", () => {
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

	async function setup(options: { removeOnComplete?: boolean } = {}) {
		const db = await pool.child();
		databases.push(db);

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([addDefinition, failingDefinition, slowDefinition]),
			context: {},
		});

		const add = conductor.createTask(
			{ name: "add", removeOnComplete: options.removeOnComplete },
			{ invocable: true },
			async (event) => {
				if (event.name !== "pgconductor.invoke") throw new Error("unexpected event");
				return { sum: event.payload.a + event.payload.b };
			},
		);

		const failing = conductor.createTask(
			{ name: "failing", maxAttempts: 1 },
			{ invocable: true },
			async () => {
				throw new Error("boom");
			},
		);

		const slow = conductor.createTask({ name: "slow" }, { invocable: true }, async (_, ctx) => {
			await new Promise((resolve) => ctx.signal.addEventListener("abort", resolve));
		});

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [add, failing, slow],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});

		await conductor.ensureInstalled();

		return { db, conductor, orchestrator };
	}

	test("getExecution returns a pending execution", async () => {
		const { conductor } = await setup();

		const id = await conductor.invoke({ name: "add" }, { a: 1, b: 2 });
		const execution = await conductor.getExecution(id);

		expect(execution).toMatchObject({
			id,
			taskKey: "add",
			queue: "default",
			status: "pending",
			result: null,
			error: null,
			attempts: 0,
			completedAt: null,
			failedAt: null,
		});
		expect(execution?.createdAt).toBeInstanceOf(Date);
	});

	test("getExecution returns the result of a completed execution", async () => {
		const { conductor, orchestrator } = await setup();

		const id = await conductor.invoke({ name: "add" }, { a: 1, b: 2 });
		await orchestrator.drain();

		const execution = await conductor.getExecution({ name: "add" }, id);

		expect(execution).toMatchObject({
			id,
			status: "completed",
			result: { sum: 3 },
			error: null,
			attempts: 1,
			failedAt: null,
		});
		expect(execution?.completedAt).toBeInstanceOf(Date);
	});

	test("getExecution returns the error of a failed execution", async () => {
		const { conductor, orchestrator } = await setup();

		const id = await conductor.invoke({ name: "failing" }, {});
		await orchestrator.drain();

		const execution = await conductor.getExecution(id);

		expect(execution).toMatchObject({
			status: "failed",
			result: null,
			error: "boom",
			completedAt: null,
		});
		expect(execution?.failedAt).toBeInstanceOf(Date);
	});

	test("getExecution returns a cancelled execution", async () => {
		const { conductor, orchestrator } = await setup();

		await orchestrator.start();
		const id = await conductor.invoke({ name: "slow" }, {});
		await waitForCondition(async () => (await conductor.getExecution(id))?.status === "running");

		await conductor.cancel(id, { reason: "no longer needed" });
		expect((await conductor.getExecution(id))?.status).toBe("running");
		await orchestrator.stop();

		expect(await conductor.getExecution(id)).toMatchObject({
			status: "cancelled",
			error: "no longer needed",
		});
	}, 30000);

	test("getExecution returns null for unknown and removed executions", async () => {
		const { conductor, orchestrator } = await setup({ removeOnComplete: true });

		expect(await conductor.getExecution(crypto.randomUUID())).toBeNull();

		const id = await conductor.invoke({ name: "add" }, { a: 1, b: 2 });
		await orchestrator.drain();

		expect(await conductor.getExecution(id)).toBeNull();
	});

	test("getExecution with a task returns null for an execution of another task", async () => {
		const { conductor } = await setup();

		const id = await conductor.invoke({ name: "failing" }, {});

		expect(await conductor.getExecution({ name: "add" }, id)).toBeNull();
	});

	test("waitForResult resolves when a running execution completes", async () => {
		const { conductor, orchestrator } = await setup();

		await orchestrator.start();
		const id = await conductor.invoke({ name: "add" }, { a: 2, b: 3 });

		const result = await conductor.waitForResult({ name: "add" }, id, { pollIntervalMs: 50 });

		expect(result).toEqual({ sum: 5 });

		await orchestrator.stop();
	}, 30000);

	test("waitForResult rejects when the execution fails", async () => {
		const { conductor, orchestrator } = await setup();

		await orchestrator.start();
		const id = await conductor.invoke({ name: "failing" }, {});

		await expect(conductor.waitForResult(id, { pollIntervalMs: 50 })).rejects.toThrow("boom");

		await orchestrator.stop();
	}, 30000);

	test("waitForResult rejects when the execution is cancelled", async () => {
		const { conductor } = await setup();

		const id = await conductor.invoke({ name: "add" }, { a: 1, b: 2 });
		const result = conductor.waitForResult(id, { pollIntervalMs: 50 });
		await conductor.cancel(id, { reason: "no longer needed" });

		await expect(result).rejects.toThrow("no longer needed");
	});

	test("waitForResult rejects when the execution does not exist", async () => {
		const { conductor } = await setup();

		await expect(conductor.waitForResult(crypto.randomUUID())).rejects.toThrow("not found");
	});

	test("waitForResult rejects on timeout", async () => {
		const { conductor } = await setup();

		const id = await conductor.invoke({ name: "add" }, { a: 1, b: 2 });

		await expect(
			conductor.waitForResult(id, { timeout: 200, pollIntervalMs: 50 }),
		).rejects.toMatchObject({ name: "TimeoutError" });
	});

	test("waitForResult rejects when the signal is aborted", async () => {
		const { conductor } = await setup();

		const id = await conductor.invoke({ name: "add" }, { a: 1, b: 2 });
		const controller = new AbortController();
		const result = conductor.waitForResult(id, { signal: controller.signal, pollIntervalMs: 50 });
		controller.abort(new Error("stopped waiting"));

		await expect(result).rejects.toThrow("stopped waiting");
	});
});
