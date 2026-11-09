import { z } from "zod";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { Conductor } from "../../src/conductor";
import { defineEvent } from "../../src/event-definition";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
import { defineTask } from "../../src/task-definition";
import type { TaskContext } from "../../src/task-context";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const message = defineEvent({
	name: "chat.message",
	payload: z.object({ thread: z.string(), text: z.string() }),
	filterable: ["thread"],
});
const threadDefinition = defineTask({ name: "thread", payload: z.object({}) });
const routeDefinition = defineTask({ name: "route", payload: z.object({}) });

type Received = { activation: number; id: string; text: string };

describe("Listens", () => {
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

	/**
	 * A `thread` actor that runs `activate` on each activation and records the messages it receives,
	 * and a `route` task for the messages nobody listens for.
	 */
	async function setup(activate: (activation: number, ctx: TaskContext) => Promise<void>) {
		const db = await pool.child();
		databases.push(db);
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([threadDefinition, routeDefinition]),
			events: [message],
			context: {},
		});
		const received: Received[] = [];
		const routed: string[] = [];
		let activations = 0;

		const thread = conductor.createTask(
			{ name: "thread", actor: true },
			{ invocable: true },
			async (_event, ctx) => {
				const activation = ++activations;
				ctx.onSignal("chat.message", (payload, { id }) => {
					received.push({ activation, id, text: message.payload.parse(payload).text });
				});
				ctx.onSignal("command", () => {});
				await activate(activation, ctx);
			},
		);
		const route = conductor.createTask(
			{ name: "route" },
			{ event: "chat.message", unlistened: true },
			async (event) => {
				routed.push(event.payload.text);
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [thread, route],
			defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 20 },
		});
		cleanups.push(() => orchestrator.stop());
		await conductor.ensureInstalled();
		await orchestrator.start();

		const command = (
			id: string,
			listen?: Parameters<typeof conductor.signalWithStart>[0]["listen"],
		) =>
			conductor.signalWithStart({
				task: "thread",
				key: "t1",
				signal: { name: "command", id },
				listen,
			});
		const idle = (id: string) =>
			waitForCondition(async () => {
				const [row] = await db.sql<{ waiting_step_key: string | null }[]>`
					select waiting_step_key from pgconductor._private_executions where id = ${id}
				`;
				return row?.waiting_step_key === "pgconductor.idle";
			}, 5000);
		const emit = (thread: string, text: string) => conductor.emit("chat.message", { thread, text });
		return { conductor, received, routed, command, idle, emit };
	}

	test("an actor receives the events it listens for, and others go to unlistened triggers", async () => {
		const { received, routed, command, idle, emit } = await setup(async (activation, ctx) => {
			if (activation === 1)
				await ctx.listen("thread", { event: message, filter: { thread: ["1"] } });
			if (activation === 3) await ctx.unlisten("thread");
		});

		const id = await command("listen");
		await idle(id);
		const eventId = await emit("1", "for the thread");
		await emit("2", "for nobody");
		await waitForCondition(() => received.length === 1 && routed.length === 1, 5000);
		// The signal is named after the event, its id is the event's, and it woke the idle actor
		expect(received).toEqual([{ activation: 2, id: eventId, text: "for the thread" }]);
		expect(routed).toEqual(["for nobody"]);

		await idle(id);
		await command("unlisten");
		await waitForCondition(async () => {
			await idle(id);
			return true;
		}, 5000);
		await new Promise((resolve) => setTimeout(resolve, 100));
		await emit("1", "after unlisten");
		await waitForCondition(() => routed.length === 2, 5000);
		expect(routed).toEqual(["for nobody", "after unlisten"]);
		expect(received).toHaveLength(1);
	});

	test("signalWithStart listens when it starts the execution, and an ended one listens no more", async () => {
		const { conductor, received, routed, command, idle, emit } = await setup(async (_, ctx) => {
			// A second listen for the same events: each event still arrives once
			await ctx.listen("all", { event: message });
		});

		const id = await command("start", {
			thread: { event: message, filter: { thread: ["1"] } },
		});
		const eventId = await emit("1", "right after the start");
		await waitForCondition(() => received.length === 1, 5000);
		expect(received[0]?.id).toBe(eventId);
		await idle(id);
		await new Promise((resolve) => setTimeout(resolve, 100));
		expect(received).toHaveLength(1);
		expect(routed).toEqual([]);

		await conductor.cancel(id);
		await emit("1", "after the end");
		await waitForCondition(() => routed.length === 1, 5000);
		expect(routed).toEqual(["after the end"]);
		expect(received).toHaveLength(1);
	});

	test("listListeners finds the open executions an event would reach, with their listens", async () => {
		const { conductor, command, idle } = await setup(async (activation, ctx) => {
			if (activation > 1) return;
			await ctx.listen("thread", { event: message, filter: { thread: ["1"] } });
			await ctx.listen("all", { event: message });
		});
		const id = await command("start");
		await idle(id);

		const listeners = (thread: string) =>
			conductor.listListeners({
				event: "chat.message",
				payload: { thread, text: "hi" },
				task: { name: "thread" },
			});
		expect(await listeners("1")).toMatchObject([
			{ id, taskKey: "thread", key: "t1", listens: ["all", "thread"] },
		]);
		expect((await listeners("2")).map((each) => each.listens)).toEqual([["all"]]);
		// Nothing is emitted
		expect(await conductor.listEvents({ name: "chat.message" })).toEqual([]);

		await conductor.cancel(id);
		expect(await listeners("1")).toEqual([]);
	});
});
