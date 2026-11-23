import { z } from "zod";
import { afterAll, afterEach, beforeAll, describe, expect, expectTypeOf, test } from "vitest";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { QueryBuilder } from "../../src/query-builder";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";

const requestDefinition = defineTask({
	name: "request",
	payload: z.object({ id: z.string(), owner: z.string(), wait: z.boolean() }),
	returns: z.object({ done: z.string() }),
});

describe("listing executions", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60_000);

	afterEach(async () => {
		await Promise.all(databases.map((database) => database.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	test("lists a task's executions with their payloads, open ones and by payload fields", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([requestDefinition]),
			context: {},
		});
		// Requests that wait stay open, suspended at their sleep; the others complete.
		const task = conductor.createTask(
			{ name: "request" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name !== "pgconductor.invoke") throw new Error("unexpected event");
				if (event.payload.wait) await ctx.sleep("wait", "1h");
				return { done: event.payload.id };
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await conductor.ensureInstalled();

		const first = await conductor.invoke(
			{ name: "request" },
			{ id: "r1", owner: "ann", wait: true },
		);
		await conductor.invoke({ name: "request" }, { id: "r2", owner: "bea", wait: true });
		await conductor.invoke({ name: "request" }, { id: "r3", owner: "ann", wait: false });
		await orchestrator.drain();

		const all = await conductor.listExecutions({ task: { name: "request" } });
		expect(all.map((execution) => execution.payload.id)).toEqual(["r1", "r2", "r3"]);
		expect(all.map((execution) => execution.status)).toEqual(["pending", "pending", "completed"]);
		expect(all[2]?.result).toEqual({ done: "r3" });

		const open = await conductor.listExecutions({ task: { name: "request" }, open: true });
		expect(open.map((execution) => execution.payload.id)).toEqual(["r1", "r2"]);

		const ann = await conductor.listExecutions({
			task: { name: "request" },
			where: { owner: "ann" },
			open: true,
		});
		expect(ann.map((execution) => execution.id)).toEqual([first]);

		expect(
			await conductor.listExecutions({ task: { name: "request" }, offset: 1, limit: 1 }),
		).toMatchObject([{ payload: { id: "r2" } }]);
		const newest = await conductor.listExecutions({ task: { name: "request" }, order: "desc" });
		expect(newest.map((execution) => execution.payload.id)).toEqual(["r3", "r2", "r1"]);
		expect((await conductor.getExecution(first))?.payload).toEqual({
			id: "r1",
			owner: "ann",
			wait: true,
		});
	});

	test("filters by metadata, and shows where an execution waits and the steps it saved", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([requestDefinition]),
			metadata: z.object({ origin: z.object({ thread: z.string() }), tenant: z.string() }),
			context: {},
		});
		const task = conductor.createTask(
			{ name: "request" },
			{ invocable: true },
			async (event, ctx) => {
				if (event.name !== "pgconductor.invoke") throw new Error("unexpected event");
				for (let i = 0; i < 12; i++) await ctx.step(`step-${i}`, () => i);
				if (event.payload.wait) {
					await ctx.waitForAny("approval", { approved: { signal: "approve" } }, { timeout: "1h" });
				}
				return { done: event.payload.id };
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await conductor.ensureInstalled();

		const waiting = await conductor.invoke(
			{ name: "request" },
			{ id: "r1", owner: "ann", wait: true },
			{ metadata: { origin: { thread: "t1" }, tenant: "acme" } },
		);
		await conductor.invoke(
			{ name: "request" },
			{ id: "r2", owner: "ann", wait: false },
			{ metadata: { origin: { thread: "t2" }, tenant: "acme" } },
		);
		await orchestrator.drain();

		const t1 = await conductor.listExecutions({
			task: { name: "request" },
			metadata: { origin: { thread: "t1" } },
			open: true,
		});
		expect(t1.map((execution) => execution.id)).toEqual([waiting]);
		expectTypeOf(t1[0]?.metadata).toEqualTypeOf<
			{ origin: { thread: string }; tenant: string } | null | undefined
		>();
		expect(t1[0]?.metadata).toEqual({ origin: { thread: "t1" }, tenant: "acme" });
		expect(t1[0]?.waitingStepKey).toBe("approval");

		const acme = await conductor.listExecutions({
			task: { name: "request" },
			metadata: { tenant: "acme" },
			where: { owner: "ann" },
		});
		expect(acme.map((execution) => execution.payload.id)).toEqual(["r1", "r2"]);
		expect(acme[1]?.waitingStepKey).toBeNull();
		expect(
			await conductor.listExecutions({ task: { name: "request" }, metadata: { tenant: "globex" } }),
		).toEqual([]);
		expect((await conductor.getExecution(waiting))?.waitingStepKey).toBe("approval");

		expect(await conductor.getSteps(waiting)).toEqual({
			count: 12,
			recent: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map((i) => `step-${i}`),
		});
		expect(await conductor.getSteps(waiting, { limit: 2 })).toEqual({
			count: 12,
			recent: ["step-10", "step-11"],
		});
		expect(await conductor.getSteps("00000000-0000-0000-0000-000000000000")).toEqual({
			count: 0,
			recent: [],
		});
	});

	test("the open metadata filter reads one partition through the open executions' index", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([requestDefinition]),
			context: {},
		});
		await conductor.ensureInstalled();
		await db.sql`insert into pgconductor._private_queues (name) values ('default'), ('other') on conflict do nothing`;

		const query = new QueryBuilder(db.sql).buildListExecutions({
			task: { key: "request", queue: "default" },
			metadata: { tenant: "acme" },
			open: true,
			order: "asc",
			limit: 100,
			offset: 0,
		});
		await db.sql`set enable_seqscan = off`;
		await db.sql`set plan_cache_mode = force_generic_plan`;
		const { string } = await query.describe();
		await db.sql.unsafe(`prepare list as ${string}`);
		const plan = await db.sql.unsafe<{ "QUERY PLAN": string }[]>(
			`explain execute list('request', 'default', '{"tenant": "acme"}', 100, 0)`,
		);
		const text = plan.map((row) => row["QUERY PLAN"]).join("\n");
		expect(text).toMatch(/_task_key_queue_created_at_id_idx/);
		// pruned to the default partition
		expect(text.match(/ on executions_/g)).toHaveLength(1);
	});
});
