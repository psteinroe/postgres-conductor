import { z } from "zod";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
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
});
