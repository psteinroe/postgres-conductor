import { z } from "zod";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { Conductor } from "../../src/conductor";
import type { JsonValue } from "../../src/database-client";
import { defineEvent } from "../../src/event-definition";
import { Orchestrator } from "../../src/orchestrator";
import { TaskSchemas } from "../../src/schemas";
import type { SignalMessage, TaskContext } from "../../src/task-context";
import { defineTask } from "../../src/task-definition";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const message = defineEvent({
	name: "chat.message",
	payload: z.object({ thread: z.string(), text: z.string() }),
});

// The schema trims the text, so a received payload shows the receiver parsed it
const Delivery = z.object({ text: z.string().trim(), from: z.string() });
// A sender may leave out the reason: the receiver gets the default
const Withdraw = z.object({ reason: z.string().default("none") });
const Decision = z.object({
	status: z.enum(["approved", "rejected"]),
	comment: z.string().trim(),
});

const sessionDefinition = defineTask({
	name: "session",
	payload: z.object({}),
	signals: { input: Delivery, withdraw: Withdraw },
});
const approvalDefinition = defineTask({
	name: "approval",
	payload: z.object({}),
	signals: { decision: Decision },
});
const senderDefinition = defineTask({ name: "sender", payload: z.object({}) });

type Received = { name: string; id: string; payload: JsonValue };
type SenderContext = TaskContext<
	readonly [typeof sessionDefinition, typeof approvalDefinition, typeof senderDefinition],
	readonly [typeof message]
>;

// Typed `SignalMessage`, so that the compiler lets these invalid payloads through to validation
const badInput: SignalMessage = { name: "input", id: "bad", payload: { text: 1, from: "ann" } };
const badWithdraw: SignalMessage = { name: "withdraw", id: "bad", payload: "not an object" };

describe("typed signals", () => {
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

	/**
	 * A `session` actor that records the signals it receives, an `approval` that waits for its
	 * decision, and a `sender` that runs `send` with its context.
	 */
	async function setup(
		send: (ctx: SenderContext, errors: string[]) => Promise<void> = async () => {},
	) {
		const db = await pool.child();
		databases.push(db);
		const logged: string[] = [];
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([sessionDefinition, approvalDefinition, senderDefinition]),
			events: [message],
			context: {},
			logger: {
				debug: () => {},
				info: () => {},
				warn: () => {},
				error: (message: string) => logged.push(message),
			},
		});
		const received: Received[] = [];
		const decisions: z.output<typeof Decision>[] = [];
		const errors: string[] = [];

		const session = conductor.createTask(
			{ name: "session", actor: true },
			{ invocable: true },
			async (_event, ctx) => {
				ctx.onSignal("input", (payload, { id, name }) => {
					received.push({ name, id, payload: { text: payload.text, from: payload.from } });
				});
				ctx.onSignal("withdraw", (payload, { id, name }) => {
					received.push({ name, id, payload });
				});
				// Not declared: the event-named signals of ctx.listen()
				ctx.onSignal("chat.message", (payload, { id, name }) => {
					received.push({ name, id, payload });
				});
			},
		);
		const approval = conductor.createTask(
			{ name: "approval", maxAttempts: 1 },
			{ invocable: true },
			async (_event, ctx) => {
				ctx.onSignal("decision", () => {});
				const winner = await ctx.waitForAny("decided", { decision: { signal: "decision" } });
				if (winner.key === "decision") {
					decisions.push({
						status: winner.signal.payload.status,
						comment: winner.signal.payload.comment,
					});
				}
			},
		);
		const sender = conductor.createTask(
			{ name: "sender" },
			{ invocable: true },
			async (_event, ctx) => {
				await send(ctx, errors);
			},
		);

		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [session, approval, sender],
			defaultWorker: { pollIntervalMs: 20, flushIntervalMs: 20 },
		});
		cleanups.push(() => orchestrator.stop());
		await conductor.ensureInstalled();
		await orchestrator.start();
		return { db, conductor, received, decisions, errors, logged };
	}

	test("a task receives the signals it declares, sent by key, by task and id, or starting it", async () => {
		const { conductor, received } = await setup();

		const { id } = await conductor.signalWithStart({
			task: "session",
			key: "s1",
			signal: { name: "input", id: "1", payload: { text: "  hello ", from: "ann" } },
			input: {},
		});
		expect(
			await conductor.signal(
				{ task: { name: "session" }, key: "s1" },
				{ name: "input", id: "2", payload: { text: "by key", from: "bea" } },
			),
		).toBe(true);
		expect(
			await conductor.signal(
				{ task: { name: "session" }, id },
				{ name: "withdraw", id: "3", payload: {} },
			),
		).toBe(true);

		await waitForCondition(() => received.length === 3, 5000);
		expect(received).toEqual([
			{ name: "input", id: "1", payload: { text: "hello", from: "ann" } },
			{ name: "input", id: "2", payload: { text: "by key", from: "bea" } },
			{ name: "withdraw", id: "3", payload: { reason: "none" } },
		]);
	});

	test("a waitForAny signal branch returns the declared signal's payload", async () => {
		const { conductor, decisions } = await setup();

		const id = await conductor.invoke({ name: "approval" }, {});
		await waitForCondition(async () => {
			const execution = await conductor.getExecution(id);
			return execution?.status === "pending";
		}, 5000);
		await conductor.signal(
			{ task: { name: "approval" }, id },
			{ name: "decision", id: "d1", payload: { status: "approved", comment: " ship it " } },
		);
		await waitForCondition(() => decisions.length === 1, 5000);
		expect(decisions).toEqual([{ status: "approved", comment: "ship it" }]);
		expect(await conductor.waitForResult(id, { pollIntervalMs: 20 })).toBeNull();
	});

	test("ctx.sendSignal and ctx.signalWithStart send typed signals", async () => {
		const { conductor, received } = await setup(async (ctx) => {
			const { id } = await ctx.signalWithStart("start", {
				task: "session",
				key: "s1",
				signal: { name: "input", id: "1", payload: { text: " from a task ", from: "sender" } },
				input: {},
			});
			await ctx.sendSignal(
				{ task: { name: "session" }, key: "s1" },
				{ name: "input", id: "2", payload: { text: "by key", from: "sender" } },
			);
			await ctx.sendSignal(
				{ task: { name: "session" }, id },
				{ name: "withdraw", id: "3", payload: { reason: "done" } },
			);
		});

		await conductor.invoke({ name: "sender" }, {});
		await waitForCondition(() => received.length === 3, 5000);
		expect(received).toEqual([
			{ name: "input", id: "1", payload: { text: "from a task", from: "sender" } },
			{ name: "input", id: "2", payload: { text: "by key", from: "sender" } },
			{ name: "withdraw", id: "3", payload: { reason: "done" } },
		]);
	});

	test("an invalid payload is rejected where the signal is sent, and nothing is stored", async () => {
		const invalid = /^Invalid payload of signal "input" for task "session": text: /;
		const { db, conductor, received, errors } = await setup(async (ctx, errors) => {
			await ctx
				.sendSignal({ task: { name: "session" }, key: "s1" }, badInput)
				.catch((error: Error) => errors.push(error.message));
			await ctx
				.signalWithStart("start", { task: "session", key: "s2", signal: badInput, input: {} })
				.catch((error: Error) => errors.push(error.message));
		});

		// Starting nothing for a payload that fails validation
		await expect(
			conductor.signalWithStart({ task: "session", key: "s1", signal: badInput, input: {} }),
		).rejects.toThrow(invalid);
		expect(await conductor.listExecutions({ task: { name: "session" } })).toEqual([]);

		const { id } = await conductor.signalWithStart({
			task: "session",
			key: "s1",
			signal: { name: "withdraw", id: "start", payload: {} },
			input: {},
		});
		await expect(
			conductor.signal({ task: { name: "session" }, key: "s1" }, badInput),
		).rejects.toThrow(invalid);
		await expect(conductor.signal({ task: { name: "session" }, id }, badWithdraw)).rejects.toThrow(
			/^Invalid payload of signal "withdraw" for task "session"/,
		);

		await conductor.invoke({ name: "sender" }, {});
		await waitForCondition(() => errors.length === 2, 5000);
		expect(errors).toEqual([expect.stringMatching(invalid), expect.stringMatching(invalid)]);

		// Only the valid signal was stored, and no execution started for s2
		await waitForCondition(() => received.length === 1, 5000);
		const signals = await db.sql<{ id: string }[]>`
			select id from pgconductor._private_execution_signals order by seq
		`;
		expect(signals.map((signal) => signal.id)).toEqual(["start"]);
		expect(await conductor.getExecution({ task: { name: "session" }, key: "s2" })).toBeNull();
	});

	test("signalling a task's execution by id throws if it belongs to another task", async () => {
		const { conductor, received } = await setup();

		const { id } = await conductor.signalWithStart({
			task: "session",
			key: "s1",
			signal: { name: "input", id: "1", payload: { text: "hi", from: "ann" } },
			input: {},
		});
		await expect(
			conductor.signal(
				{ task: { name: "approval" }, id },
				{ name: "decision", id: "d1", payload: { status: "approved", comment: "" } },
			),
		).rejects.toThrow(`Execution ${id} belongs to task "session" of queue "default"`);
		await waitForCondition(() => received.length === 1, 5000);
		expect(received.map((signal) => signal.id)).toEqual(["1"]);
	});

	test("a payload that does not match on receipt is skipped and logged; undeclared names take any", async () => {
		const { db, conductor, received, logged } = await setup();

		const { id } = await conductor.signalWithStart({
			task: "session",
			key: "s1",
			signal: { name: "note", id: "1", payload: ["any", 1] },
			input: {},
			listen: { chat: { event: message } },
		});
		// A bare execution id checks nothing: the receiver parses the payload and skips it
		await conductor.signal(id, { name: "withdraw", id: "2", payload: "unchecked" });
		await conductor.signal(id, { name: "withdraw", id: "3", payload: { reason: "valid" } });
		await waitForCondition(() => received.length === 1, 5000);

		const eventId = await conductor.emit("chat.message", { thread: "t1", text: "hi" });
		await waitForCondition(() => received.length === 2, 5000);
		expect(received).toEqual([
			{ name: "withdraw", id: "3", payload: { reason: "valid" } },
			{ name: "chat.message", id: eventId, payload: { thread: "t1", text: "hi" } },
		]);
		expect(logged).toEqual([
			expect.stringMatching(/^Skipped signal "withdraw": Invalid payload of signal "withdraw"/),
		]);
		// The skipped signal counts as delivered, so it wakes the actor no more
		const [skipped] = await db.sql<{ delivered: boolean }[]>`
			select delivered_at is not null as delivered
			from pgconductor._private_execution_signals
			where id = '2'
		`;
		expect(skipped?.delivered).toBe(true);
	});

	test("a waitForAny branch won by a payload that does not match fails the attempt", async () => {
		const { conductor, decisions } = await setup();

		const id = await conductor.invoke({ name: "approval" }, {});
		await waitForCondition(async () => {
			const execution = await conductor.getExecution(id);
			return execution?.status === "pending";
		}, 5000);
		await conductor.signal(id, { name: "decision", id: "d1", payload: { status: "maybe" } });
		await expect(conductor.waitForResult(id, { pollIntervalMs: 20 })).rejects.toThrow(
			/Invalid payload of signal "decision" for task "approval"/,
		);
		expect(decisions).toEqual([]);
	});
});
