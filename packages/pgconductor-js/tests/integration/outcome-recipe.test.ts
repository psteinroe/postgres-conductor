import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineEvent } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import type { Task } from "../../src/task";
import type { JsonValue } from "../../src/database-client";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

// The examples of docs/content/crafting-tasks/report-outcomes.md

const approvalDecided = defineEvent({
	name: "approval.decided",
	payload: z.object({ executionId: z.string(), approved: z.boolean() }),
	filterable: ["executionId"],
});
const toolCall = defineTask({
	name: "tool-call",
	payload: z.object({ sessionKey: z.string(), input: z.string() }),
	returns: z.object({ output: z.string() }),
});
const watchToolCall = defineTask({
	name: "watch-tool-call",
	payload: z.object({ sessionKey: z.string(), input: z.string() }),
});
const session = defineTask({ name: "session" });
const agentRun = defineTask({ name: "agent-run" });
const sandbox = defineTask({ name: "sandbox", payload: z.object({ callerId: z.string() }) });
const watchCaller = defineTask({
	name: "watch-caller",
	payload: z.object({ callerId: z.string(), workId: z.string() }),
});
const deploy = defineTask({ name: "deploy" });
const approvalCard = defineTask({
	name: "approval-card",
	payload: z.object({ deployId: z.string() }),
});

describe("report an execution's outcome to another execution", () => {
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
		const database = await pool.child();
		databases.push(database);
		const conductor = Conductor.create({
			sql: database.sql,
			tasks: TaskSchemas.fromSchema([
				toolCall,
				watchToolCall,
				session,
				agentRun,
				sandbox,
				watchCaller,
				deploy,
				approvalCard,
			]),
			events: [approvalDecided],
			context: {},
		});
		await conductor.ensureInstalled();
		return { database, conductor };
	}

	async function start(
		conductor: Awaited<ReturnType<typeof setup>>["conductor"],
		tasks: readonly Task<any, "default", any, any, any, any>[],
	) {
		const orchestrator = Orchestrator.create({
			conductor,
			tasks,
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10, concurrency: 8 },
		});
		orchestrators.push(orchestrator);
		await orchestrator.start();
	}

	test("a supervisor reports the outcome to a session, restarting it when it finished", async () => {
		const { database, conductor } = await setup();
		const received: { sessionId: string; id: string; payload: JsonValue }[] = [];
		await start(conductor, [
			conductor.createTask(
				{ name: "tool-call", maxAttempts: 1 },
				{ invocable: true },
				async (event) => {
					if (event.payload.input === "fail") throw new Error("tool failed");
					return { output: event.payload.input.toUpperCase() };
				},
			),
			conductor.createTask({ name: "watch-tool-call" }, { invocable: true }, async (event, ctx) => {
				const id = await ctx.invoke("start", { name: "tool-call" }, event.payload);
				const outcome = await ctx.waitForResult("result", { name: "tool-call" }, id);
				await ctx.signalWithStart("report", {
					task: "session",
					key: event.payload.sessionKey,
					signal: {
						name: "tool-settled",
						id: `tool-settled:${id}`,
						payload: { executionId: id, outcome },
					},
				});
			}),
			conductor.createTask(
				{ name: "session", maxAttempts: 1 },
				{ invocable: true },
				async (_event, ctx) => {
					ctx.onSignal("tool-settled", async (payload, { id }) => {
						received.push({ sessionId: ctx.executionId, id, payload });
					});
				},
			),
		]);

		const first = await conductor.invoke(
			{ name: "watch-tool-call" },
			{ sessionKey: "s1", input: "hi" },
		);
		await conductor.waitForResult(first, { pollIntervalMs: 10 });
		await waitForCondition(() => received.length === 1);
		await conductor.waitForResult(received[0]?.sessionId || "", { pollIntervalMs: 10 });
		const second = await conductor.invoke(
			{ name: "watch-tool-call" },
			{ sessionKey: "s1", input: "fail" },
		);
		await conductor.waitForResult(second, { pollIntervalMs: 10 });
		await waitForCondition(() => received.length === 2);

		const toolIds = await database.sql<{ id: string }[]>`
			select id from pgconductor._private_executions where task_key = 'tool-call' order by created_at
		`;
		const [okId, failedId] = toolIds.map((row) => row.id);
		expect(received.map(({ id, payload }) => ({ id, payload }))).toEqual([
			{
				id: `tool-settled:${okId}`,
				payload: { executionId: okId, outcome: { status: "completed", result: { output: "HI" } } },
			},
			{
				id: `tool-settled:${failedId}`,
				payload: { executionId: failedId, outcome: { status: "failed", error: "tool failed" } },
			},
		]);
		expect(received[0]?.sessionId).not.toBe(received[1]?.sessionId);
	});

	test("a watcher shuts down the work it owns when its caller is cancelled", async () => {
		const { conductor } = await setup();
		const sandboxIds = new Deferred<string>();
		const stopped = new Deferred<JsonValue>();
		await start(conductor, [
			conductor.createTask({ name: "agent-run" }, { invocable: true }, async (_event, ctx) => {
				const id = await ctx.invoke("sandbox", { name: "sandbox" }, { callerId: ctx.executionId });
				sandboxIds.resolve(id);
				await ctx.sleep("idle", 3_600_000);
			}),
			conductor.createTask({ name: "sandbox" }, { invocable: true }, async (event, ctx) => {
				const shutdown = new Deferred<void>();
				ctx.onSignal("shutdown", async (payload) => {
					stopped.resolve(payload);
					shutdown.resolve();
				});
				await ctx.invoke(
					"watch-caller",
					{ name: "watch-caller" },
					{ callerId: event.payload.callerId, workId: ctx.executionId },
					{ cancelWithParent: true },
				);
				await shutdown.promise;
			}),
			conductor.createTask({ name: "watch-caller" }, { invocable: true }, async (event, ctx) => {
				const { callerId, workId } = event.payload;
				const outcome = await ctx.waitForResult("caller", callerId);
				if (outcome.status === "completed") return;
				await ctx.sendSignal(workId, {
					name: "shutdown",
					id: `shutdown:${callerId}`,
					payload: outcome,
				});
			}),
		]);

		const runId = await conductor.invoke({ name: "agent-run" }, {});
		const sandboxId = await sandboxIds.promise;
		await waitForCondition(async () => (await conductor.getExecution(runId))?.status === "pending");
		expect(await conductor.cancel(runId, { reason: "user stopped the run" })).toBe(true);

		expect(await stopped.promise).toEqual({ status: "cancelled", reason: "user stopped the run" });
		await conductor.waitForResult(sandboxId, { pollIntervalMs: 10 });
	});

	test("an approval card is removed when the deploy ends before a decision", async () => {
		const { conductor } = await setup();
		const release = new Deferred<void>();
		const removed = new Deferred<string>();
		await start(conductor, [
			conductor.createTask({ name: "deploy" }, { invocable: true }, async () => release.promise),
			conductor.createTask({ name: "approval-card" }, { invocable: true }, async (event, ctx) => {
				const { deployId } = event.payload;
				const decided = await ctx.subscribe("decided", {
					event: approvalDecided,
					filter: { executionId: [deployId] },
				});
				const winner = await ctx.waitForAny("decision", {
					decided,
					deploy: { execution: deployId },
				});
				if (winner.key === "deploy") {
					await ctx.step("remove-card", async () => removed.resolve(winner.outcome.status));
				}
			}),
		]);

		const deployId = await conductor.invoke({ name: "deploy" }, {});
		const cardId = await conductor.invoke({ name: "approval-card" }, { deployId });
		await waitForCondition(
			async () => (await conductor.getExecution(cardId))?.status === "pending",
		);
		release.resolve();

		expect(await removed.promise).toBe("completed");
	});
});
