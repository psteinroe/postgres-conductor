import { z } from "zod";
import postgres from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { Conductor } from "../../src/conductor";
import { ListenHeldError } from "../../src/database-client";
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

type Received = { activation: number; id: string; text: string; listens: string[] };

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
				ctx.onSignal("chat.message", (payload, { id, listens }) => {
					received.push({ activation, id, text: message.payload.parse(payload).text, listens });
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
			conductor
				.signalWithStart({
					task: "thread",
					key: "t1",
					signal: { name: "command", id },
					listen,
				})
				.then(({ id }) => id);
		const idle = (id: string) =>
			waitForCondition(async () => {
				const [row] = await db.sql<{ waiting_step_key: string | null }[]>`
					select waiting_step_key from pgconductor._private_executions where id = ${id}
				`;
				return row?.waiting_step_key === "pgconductor.idle";
			}, 5000);
		const emit = (thread: string, text: string) => conductor.emit("chat.message", { thread, text });
		return { db, conductor, received, routed, command, idle, emit };
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
		expect(received).toEqual([
			{ activation: 2, id: eventId, text: "for the thread", listens: ["thread"] },
		]);
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

	test("listListeners finds the open executions an event would reach, with the listens it matches", async () => {
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
		// `matched` names the listens the event reaches, `listens` all of them, in creation order
		expect(await listeners("1")).toMatchObject([
			{ id, taskKey: "thread", key: "t1", listens: ["thread", "all"], matched: ["thread", "all"] },
		]);
		expect(await listeners("2")).toMatchObject([{ listens: ["thread", "all"], matched: ["all"] }]);
		// Nothing is emitted
		expect(await conductor.listEvents({ name: "chat.message" })).toEqual([]);

		await conductor.cancel(id);
		expect(await listeners("1")).toEqual([]);
	});

	test("getExecution and listExecutions read the listens signalWithStart and the execution set", async () => {
		const seen: { activation: number; listens: readonly string[] }[] = [];
		const { conductor, command, idle } = await setup(async (activation, ctx) => {
			seen.push({ activation, listens: ctx.listens });
			if (activation === 1) {
				await ctx.listen("all", { event: message });
				// Unchanged: keeps its place
				await ctx.listen("thread", { event: message, filter: { thread: ["1"] } });
				await ctx.listen("other", { event: message, filter: { thread: ["2"] } });
			}
			if (activation === 2) {
				await ctx.unlisten("all");
				// Changed: starts anew, after the others
				await ctx.listen("thread", { event: message, filter: { thread: ["3"] } });
			}
			seen.push({ activation, listens: ctx.listens });
		});

		const id = await command("start", {
			thread: { event: message, filter: { thread: ["1"] } },
		});
		const read = async () => (await conductor.getExecution(id))?.listens;
		expect(await read()).toEqual(["thread"]);

		await waitForCondition(() => seen.length === 2, 5000);
		await idle(id);
		expect(await read()).toEqual(["thread", "all", "other"]);
		expect(await conductor.listExecutions({ task: { name: "thread" }, open: true })).toMatchObject([
			{ id, listens: ["thread", "all", "other"] },
		]);

		await command("second");
		await waitForCondition(() => seen.length === 4, 5000);
		await idle(id);
		await command("third");
		await waitForCondition(() => seen.length === 6, 5000);
		await idle(id);
		// ctx.listens is current after the activation's own calls, and the next activation reads it
		expect(seen).toEqual([
			{ activation: 1, listens: ["thread"] },
			{ activation: 1, listens: ["thread", "all", "other"] },
			{ activation: 2, listens: ["thread", "all", "other"] },
			{ activation: 2, listens: ["other", "thread"] },
			{ activation: 3, listens: ["other", "thread"] },
			{ activation: 3, listens: ["other", "thread"] },
		]);
		expect(await read()).toEqual(["other", "thread"]);

		// Listens end with their execution
		await conductor.cancel(id);
		await waitForCondition(
			async () => (await conductor.getExecution(id))?.status === "cancelled",
			5000,
		);
		expect(await read()).toEqual([]);
		expect(await conductor.listExecutions({ task: { name: "thread" } })).toMatchObject([
			{ id, status: "cancelled", listens: [] },
		]);
	});

	test("an execution without listens reads none", async () => {
		const { conductor, command, idle } = await setup(async (_, ctx) => {
			expect(ctx.listens).toEqual([]);
		});
		const id = await command("start");
		await idle(id);
		expect(await conductor.getExecution(id)).toMatchObject({ listens: [] });
	});

	test("a signal from listens names the listens the event matched", async () => {
		const { received, command, idle, emit } = await setup(async (activation, ctx) => {
			if (activation === 1) {
				await ctx.listen("all", { event: message });
				await ctx.listen("thread", { event: message, filter: { thread: ["1"] } });
			}
		});

		const id = await command("start");
		await idle(id);
		const both = await emit("1", "both");
		const all = await emit("2", "all only");
		await waitForCondition(() => received.length === 2, 5000);
		expect(received.map(({ id, listens }) => ({ id, listens }))).toEqual(
			expect.arrayContaining([
				{ id: both, listens: ["all", "thread"] },
				{ id: all, listens: ["all"] },
			]),
		);
	});

	test("an execution that completes itself ends its listens", async () => {
		const { db, conductor, received, routed, command, idle, emit } = await setup(
			async (activation, ctx) => {
				if (activation === 1)
					await ctx.listen("thread", { event: message, filter: { thread: ["1"] } });
				if (activation === 2) await ctx.complete();
			},
		);
		const listeners = () =>
			conductor.listListeners({ event: "chat.message", payload: { thread: "1", text: "" } });

		const id = await command("start");
		await idle(id);
		expect((await listeners()).map((listener) => listener.id)).toEqual([id]);

		await command("end");
		await waitForCondition(
			async () => (await conductor.getExecution(id))?.status === "completed",
			5000,
		);
		expect(await conductor.getExecution(id)).toMatchObject({ listens: [] });
		expect(await listeners()).toEqual([]);
		const subscriptions = await db.sql`
			select 1 from pgconductor._private_custom_event_subscriptions where execution_id = ${id}
		`;
		expect(subscriptions).toHaveLength(0);

		// Its events now go to the unlistened trigger
		await emit("1", "after the end");
		await waitForCondition(() => routed.length === 1, 5000);
		expect(received).toEqual([]);
	});

	test("an exclusive listen holds its name among the open executions of the task", async () => {
		const outcomes: { id: string; heldBy: string | null; listens: readonly string[] }[] = [];
		const conversation = { event: message, filter: { thread: ["1"] }, exclusive: true };
		const { conductor, idle } = await setup(async (_activation, ctx) => {
			try {
				// The holder may listen again; another execution is refused
				await ctx.listen("conversation", conversation);
				outcomes.push({ id: ctx.executionId, heldBy: null, listens: ctx.listens });
			} catch (error) {
				if (!(error instanceof ListenHeldError)) throw error;
				outcomes.push({ id: ctx.executionId, heldBy: error.heldBy, listens: ctx.listens });
			}
		});
		const start = (key: string, listen?: { conversation: typeof conversation }) =>
			conductor.signalWithStart({
				task: "thread",
				key,
				signal: { name: "command", id: "start" },
				listen,
			});

		const holder = await start("t1", { conversation });
		await idle(holder.id);
		expect(outcomes).toEqual([{ id: holder.id, heldBy: null, listens: ["conversation"] }]);

		// signalWithStart is refused and starts nothing
		const refused = await start("t2", { conversation }).catch((error: unknown) => error);
		expect(refused).toBeInstanceOf(ListenHeldError);
		expect(refused).toMatchObject({ listen: "conversation", heldBy: holder.id });
		expect(await conductor.getExecution({ task: { name: "thread" }, key: "t2" })).toBeNull();

		// ctx.listen is refused and listens for nothing
		const other = await start("t3");
		await idle(other.id);
		expect(outcomes[1]).toEqual({ id: other.id, heldBy: holder.id, listens: [] });
		expect(await conductor.getExecution(other.id)).toMatchObject({ listens: [] });

		// An ended execution holds no name
		await conductor.cancel(holder.id);
		const next = await start("t2", { conversation });
		expect(next.started).toBe(true);
		await idle(next.id);
		expect(outcomes[2]).toEqual({ id: next.id, heldBy: null, listens: ["conversation"] });
	});

	test("of executions listening exclusively at once, exactly one holds the name", async () => {
		const total = 8;
		const { db, conductor } = await setup(async () => {});
		// A pool of its own, so the calls run concurrently
		const sql = postgres(db.url, { max: total });
		cleanups.push(() => sql.end());
		const sender = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([threadDefinition, routeDefinition]),
			events: [message],
			context: {},
		});

		const results = await Promise.allSettled(
			Array.from({ length: total }, (_, n) =>
				sender.signalWithStart({
					task: "thread",
					key: `t${n}`,
					signal: { name: "command", id: "start" },
					listen: {
						conversation: { event: message, filter: { thread: ["1"] }, exclusive: true },
					},
				}),
			),
		);

		const started = results.flatMap((result) =>
			result.status === "fulfilled" ? [result.value] : [],
		);
		expect(started).toHaveLength(1);
		const winner = started[0]?.id;
		const refusals = results.flatMap((result) =>
			result.status === "rejected" ? [result.reason] : [],
		);
		expect(refusals).toHaveLength(total - 1);
		for (const refusal of refusals) {
			expect(refusal).toBeInstanceOf(ListenHeldError);
			expect(refusal).toMatchObject({ heldBy: winner });
		}
		expect((await conductor.listExecutions({ task: { name: "thread" } })).map((e) => e.id)).toEqual(
			[winner],
		);
	});
});
