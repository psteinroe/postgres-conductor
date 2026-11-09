import { z } from "zod";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import type { Payload } from "../../src/database-client";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const workerDefinition = defineTask({
	name: "worker",
	payload: z.object({ owner: z.string() }),
});

describe("execution state", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const cleanups: (() => Promise<void>)[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60_000);

	afterEach(async () => {
		for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
		await Promise.all(databases.map((database) => database.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	test("an execution sets its state, which others read and find it by", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([workerDefinition]),
			context: {},
		});
		const seen: (Payload | null)[] = [];
		// An actor that adds the topic of each message to the topics in its state.
		const task = conductor.createTask(
			{ name: "worker", actor: true },
			{ invocable: true },
			async (_event, ctx) => {
				seen.push(ctx.state);
				ctx.onSignal("topic", async (payload) => {
					const topics = ctx.state?.topics;
					await ctx.setState({
						status: "busy",
						topics: [...(Array.isArray(topics) ? topics : []), String(payload)],
					});
				});
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		cleanups.push(() => orchestrator.stop());
		await conductor.ensureInstalled();
		await orchestrator.start();

		const send = (key: string, owner: string, topic: string) =>
			conductor.signalWithStart({
				task: "worker",
				key,
				signal: { name: "topic", id: `${key}:${topic}`, payload: topic },
				input: { owner },
				state: { status: "new", topics: [] },
			});
		const ann = await send("w1", "ann", "billing");
		const bea = await send("w2", "bea", "export");
		await waitForCondition(async () => {
			const executions = await conductor.listExecutions({
				task: { name: "worker" },
				state: { status: "busy" },
			});
			return executions.length === 2;
		}, 5000);

		// The initial state is used only when signalWithStart starts the execution
		expect(await send("w1", "ann", "invoices")).toBe(ann);
		await waitForCondition(async () => {
			const execution = await conductor.getExecution(ann);
			return Array.isArray(execution?.state?.topics) && execution.state.topics.length === 2;
		}, 5000);
		expect(await conductor.getExecution(ann)).toMatchObject({
			key: "w1",
			payload: { owner: "ann" },
			state: { status: "busy", topics: ["billing", "invoices"] },
		});
		// Each activation starts with the state the previous one left
		expect(seen).toContainEqual({ status: "new", topics: [] });
		expect(seen).toContainEqual({ status: "busy", topics: ["billing"] });

		const billing = await conductor.listExecutions({
			task: { name: "worker" },
			open: true,
			state: { topics: ["billing"] },
		});
		expect(billing.map((execution) => execution.id)).toEqual([ann]);
		const busyBea = await conductor.listExecutions({
			task: { name: "worker" },
			where: { owner: "bea" },
			state: { status: "busy" },
		});
		expect(busyBea.map((execution) => execution.id)).toEqual([bea]);

		// An execution that ended keeps its state for reading, but is not open
		await conductor.cancel(bea);
		expect((await conductor.getExecution(bea))?.state).toEqual({
			status: "busy",
			topics: ["export"],
		});
		const open = await conductor.listExecutions({
			task: { name: "worker" },
			open: true,
			state: { status: "busy" },
		});
		expect(open.map((execution) => execution.id)).toEqual([ann]);
	});
});
