import { z } from "zod";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { Conductor } from "../../src/conductor";
import { defineEvent } from "../../src/event-definition";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";

const message = defineEvent({
	name: "chat.message",
	payload: z.object({ thread: z.string(), text: z.string() }),
	filterable: ["thread"],
});
const threadDefinition = defineTask({
	name: "thread",
	payload: z.object({ owner: z.string() }),
	state: z.object({ turns: z.number() }),
});
const readerDefinition = defineTask({
	name: "reader",
	payload: z.object({}),
	returns: z.object({
		owner: z.string().nullable(),
		turns: z.number().nullable(),
		open: z.array(z.string()),
		matched: z.array(z.array(z.string())),
		texts: z.array(z.string()),
		count: z.number(),
		event: z.string().nullable(),
	}),
});

describe("ctx reads", () => {
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

	test("a task reads executions, listeners and events through its ctx, as the conductor does", async () => {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([threadDefinition, readerDefinition]),
			events: [message],
			context: {},
		});
		const thread = conductor.createTask(
			{ name: "thread", actor: true },
			{ invocable: true },
			async (_event, ctx) => {
				ctx.onSignal("chat.message", () => {});
				ctx.onSignal("start", () => {});
			},
		);
		const reader = conductor.createTask(
			{ name: "reader" },
			{ invocable: true },
			async (_event, ctx) => {
				const execution = await ctx.getExecution({ task: { name: "thread" }, key: "t1" });
				const open = await ctx.listExecutions({ task: { name: "thread" }, open: true });
				const listeners = await Promise.all(
					["1", "2"].map((each) =>
						ctx.listListeners({
							event: "chat.message",
							payload: { thread: each, text: "hi" },
							task: { name: "thread" },
						}),
					),
				);
				const events = await ctx.listEvents({ name: "chat.message", where: { thread: "1" } });
				const first = events[0] && (await ctx.getEvent(events[0].id));
				return {
					owner: execution?.payload.owner || null,
					turns: execution?.state?.turns || null,
					open: open.map((each) => each.key || ""),
					matched: listeners.map((found) => found.flatMap((each) => each.matched)),
					texts: events.map((each) => each.payload.text),
					count: await ctx.countEvents({ name: "chat.message" }),
					event: first ? first.name : null,
				};
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [thread, reader],
			defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 20 },
		});
		cleanups.push(() => orchestrator.stop());
		await conductor.ensureInstalled();
		await orchestrator.start();

		await conductor.signalWithStart({
			task: "thread",
			key: "t1",
			signal: { name: "start", id: "start" },
			input: { owner: "ann" },
			state: { turns: 2 },
			listen: { thread: { event: message, filter: { thread: ["1"] } } },
		});
		await conductor.emit("chat.message", { thread: "1", text: "first" });
		await conductor.emit("chat.message", { thread: "2", text: "second" });

		const id = await conductor.invoke({ name: "reader" }, {});
		expect(await conductor.waitForResult({ name: "reader" }, id, { timeout: 10_000 })).toEqual({
			owner: "ann",
			turns: 2,
			open: ["t1"],
			matched: [["thread"], []],
			texts: ["first"],
			count: 2,
			event: "chat.message",
		});
	});
});
