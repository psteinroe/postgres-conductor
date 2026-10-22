import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineEvent } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const ping = defineEvent({ name: "status.ping", payload: z.object({}) });
const parentDefinition = defineTask({ name: "parent", payload: z.object({ mode: z.string() }) });
const childDefinition = defineTask({ name: "child" });

describe("getExecution waiting status", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const orchestrators: Orchestrator[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		await Promise.allSettled(orchestrators.map((orchestrator) => orchestrator.stop()));
		orchestrators.length = 0;
		await Promise.all(databases.map((database) => database.destroy()));
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
			tasks: TaskSchemas.fromSchema([parentDefinition, childDefinition]),
			events: [ping],
			context: {},
		});
		const parent = conductor.createTask(
			{ name: "parent", maxAttempts: 3 },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name !== "pgconductor.invoke") return;
				const { mode } = event.payload;
				if (mode === "sleep") await ctx.sleep("nap", "1h");
				if (mode === "sleep-then-fail") {
					await ctx.sleep("nap", 0);
					throw new Error("after nap");
				}
				if (mode === "child") await ctx.invokeAndWait("call", { name: "child" });
				if (mode === "child-timeout") {
					await ctx.invokeAndWait("call", { name: "child" }, {}, { timeout: "1h" });
				}
				if (mode === "event") await ctx.waitForEvent("ping", { event: ping, timeout: "1h" });
				if (mode === "signal")
					await ctx.waitForAny("approval", { approved: { signal: "approve" } });
				if (mode === "fail") throw new Error("boom");
			},
		);
		const child = conductor.createTask({ name: "child" }, { invocable: true }, async () => {});
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parent],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		orchestrators.push(orchestrator);
		await conductor.ensureInstalled();
		// registers the child task without running it
		await Orchestrator.create({ conductor, tasks: [child] }).drain();
		await orchestrator.start();
		return { db, conductor };
	}

	async function settled(conductor: Conductor<any, any, any, any>, id: string) {
		await waitForCondition(async () => {
			const execution = await conductor.getExecution(id);
			return execution?.status !== "pending" && execution?.status !== "running";
		});
		return conductor.getExecution(id);
	}

	test("reports a sleeping execution with its step and wake-up time", async () => {
		const { db, conductor } = await setup();

		const before = Date.now();
		const id = await conductor.invoke({ name: "parent" }, { mode: "sleep" });
		const execution = await settled(conductor, id);

		expect(execution).toMatchObject({
			status: "waiting",
			waitingFor: { type: "sleep", stepKey: "nap" },
		});
		const until = execution?.waitingFor?.until?.getTime() || 0;
		expect(until).toBeGreaterThanOrEqual(before + 3_600_000);
		expect(until).toBeLessThan(Date.now() + 3_600_000);

		await Promise.all(orchestrators.map((orchestrator) => orchestrator.stop()));
		await db.clock.advance("2h");
		expect(await conductor.getExecution(id)).toMatchObject({ status: "pending", waitingFor: null });
	});

	test("reports a child wait with the child id and no deadline", async () => {
		const { db, conductor } = await setup();

		const id = await conductor.invoke({ name: "parent" }, { mode: "child" });
		const execution = await settled(conductor, id);
		const [child] = await db.sql<{ id: string }[]>`
			select id from pgconductor._private_executions where task_key = 'child'
		`;

		expect(execution).toMatchObject({
			status: "waiting",
			waitingFor: { type: "child", stepKey: "call", executionId: child?.id, until: null },
		});
	});

	test("reports the timeout of a child wait", async () => {
		const { conductor } = await setup();

		const id = await conductor.invoke({ name: "parent" }, { mode: "child-timeout" });
		const execution = await settled(conductor, id);

		expect(execution?.waitingFor).toMatchObject({ type: "child", stepKey: "call" });
		expect(execution?.waitingFor?.until).toBeInstanceOf(Date);
	});

	test("reports an event wait with its deadline", async () => {
		const { conductor } = await setup();

		const id = await conductor.invoke({ name: "parent" }, { mode: "event" });
		const execution = await settled(conductor, id);

		expect(execution).toMatchObject({
			status: "waiting",
			waitingFor: { type: "wait", stepKey: "ping" },
		});
		expect(execution?.waitingFor?.until).toBeInstanceOf(Date);
	});

	test("reports a signal wait without a deadline", async () => {
		const { conductor } = await setup();

		const id = await conductor.invoke({ name: "parent" }, { mode: "signal" });
		const execution = await settled(conductor, id);

		expect(execution).toMatchObject({
			status: "waiting",
			waitingFor: { type: "wait", stepKey: "approval", until: null },
		});
	});

	test("keeps retries and delayed executions pending", async () => {
		const { conductor } = await setup();

		const delayed = await conductor.invoke(
			{ name: "parent" },
			{ mode: "none" },
			{ run_at: new Date(Date.now() + 3_600_000) },
		);
		const retried = await conductor.invoke({ name: "parent" }, { mode: "fail" });
		const sleptThenFailed = await conductor.invoke({ name: "parent" }, { mode: "sleep-then-fail" });
		await waitForCondition(async () => {
			const executions = await Promise.all(
				[retried, sleptThenFailed].map((id) => conductor.getExecution(id)),
			);
			return executions.every((execution) => execution?.error);
		});

		for (const id of [delayed, retried, sleptThenFailed]) {
			expect(await conductor.getExecution(id)).toMatchObject({
				status: "pending",
				waitingFor: null,
			});
		}
	});

	test("waitForResult keeps polling a waiting execution", async () => {
		const { conductor } = await setup();

		const id = await conductor.invoke({ name: "parent" }, { mode: "signal" });
		await settled(conductor, id);
		const result = conductor.waitForResult(id, { pollIntervalMs: 10 });
		await conductor.signal(id, { name: "approve", id: "a" });

		await expect(result).resolves.toBeNull();
	});
});
