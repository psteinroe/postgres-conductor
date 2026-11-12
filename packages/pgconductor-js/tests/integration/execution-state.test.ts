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

const counterDefinition = defineTask({
	name: "counter",
	state: z.object({ count: z.number().int(), label: z.string().trim() }),
});

const tallyDefinition = defineTask({
	name: "tally",
	state: z.object({ count: z.number(), log: z.array(z.string()) }),
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

	test("an execution sets its state, which others read", async () => {
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
		const busy = async () =>
			(await conductor.listExecutions({ task: { name: "worker" } })).filter(
				(execution) => execution.state?.status === "busy",
			);
		await waitForCondition(async () => (await busy()).length === 2, 5000);

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

		// An execution that ended keeps its state for reading, but is not open
		await conductor.cancel(bea);
		expect((await conductor.getExecution(bea))?.state).toEqual({
			status: "busy",
			topics: ["export"],
		});
		const open = await conductor.listExecutions({ task: { name: "worker" }, open: true });
		expect(open.map((execution) => execution.id)).toEqual([ann]);
	});

	test("state is validated where it enters, and the schema's output is written", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([counterDefinition]),
			context: {},
		});
		const errors: string[] = [];
		const seen: unknown[] = [];
		const task = conductor.createTask({ name: "counter" }, { invocable: true }, async (_e, ctx) => {
			seen.push(ctx.state);
			// An invalid state throws at the call site and writes nothing
			await ctx.setState({ count: 2.5, label: "x" }).catch((error) => errors.push(error.message));
			await ctx
				.updateState((state) => ({ count: (state?.count || 0) + 0.5, label: "x" }))
				.catch((error) => errors.push(error.message));
			seen.push(ctx.state);
			await ctx
				.signalWithStart("start", {
					task: "counter",
					key: "other",
					signal: { name: "go", id: "1" },
					state: { count: -0.5, label: "other" },
				})
				.catch((error) => errors.push(error.message));
			await ctx.setState({ count: 2, label: "  second  " });
			seen.push(ctx.state);
		});
		const orchestrator = Orchestrator.create({ conductor, tasks: [task] });
		await conductor.ensureInstalled();

		await expect(
			conductor.signalWithStart({
				task: "counter",
				key: "first",
				signal: { name: "go", id: "1" },
				state: { count: 1.5, label: "first" },
			}),
		).rejects.toThrow(/^Invalid state for task "counter": count: /);
		expect(await conductor.listExecutions({ task: { name: "counter" } })).toEqual([]);

		const id = await conductor.signalWithStart({
			task: "counter",
			key: "first",
			signal: { name: "go", id: "1" },
			state: { count: 1, label: "  first  " },
		});
		await orchestrator.drain();

		expect(errors).toHaveLength(3);
		for (const error of errors) {
			expect(error).toMatch(/^Invalid state for task "counter": count: /);
		}
		expect(seen).toEqual([
			{ count: 1, label: "first" },
			{ count: 1, label: "first" },
			{ count: 2, label: "second" },
		]);
		const executions = await conductor.listExecutions({ task: { name: "counter" } });
		expect(executions.map((execution) => [execution.id, execution.state])).toEqual([
			[id, { count: 2, label: "second" }],
		]);
	});

	test("updateState calls of an attempt run one after another", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([tallyDefinition]),
			context: {},
		});
		const returned: unknown[] = [];
		const errors: string[] = [];
		const task = conductor.createTask({ name: "tally" }, { invocable: true }, async (_e, ctx) => {
			// Concurrent callers, each slower than the next: without queueing, updates would be lost
			const updates = await Promise.all(
				Array.from({ length: 10 }, (_, i) =>
					ctx.updateState(async (state) => {
						await new Promise((resolve) => setTimeout(resolve, 20 - 2 * i));
						return { count: (state?.count || 0) + 1, log: [...(state?.log || []), `call-${i}`] };
					}),
				),
			);
			returned.push(updates.map((update) => update?.count));

			// Returning undefined writes nothing, and a throwing fn does not stop later updates
			returned.push(await ctx.updateState(() => undefined));
			const failing = ctx.updateState(() => {
				throw new Error("boom");
			});
			const next = ctx.updateState((state) => ({
				count: (state?.count || 0) + 1,
				log: [...(state?.log || []), "after-failure"],
			}));
			await failing.catch((error) => errors.push(error.message));
			returned.push((await next)?.count);

			// setState waits for the updates started before it
			const slow = ctx.updateState(async (state) => {
				await new Promise((resolve) => setTimeout(resolve, 20));
				return { count: (state?.count || 0) + 1, log: [...(state?.log || []), "slow"] };
			});
			await ctx.setState({ count: 100, log: ["reset"] });
			returned.push((await slow)?.count, ctx.state?.count);
		});
		const orchestrator = Orchestrator.create({ conductor, tasks: [task] });
		await conductor.ensureInstalled();
		const id = await conductor.invoke({ name: "tally" }, {});
		await orchestrator.drain();

		expect(errors).toEqual(["boom"]);
		expect(returned).toEqual([[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], undefined, 11, 12, 100]);
		expect((await conductor.getExecution({ name: "tally" }, id))?.state).toEqual({
			count: 100,
			log: ["reset"],
		});
	});
});
