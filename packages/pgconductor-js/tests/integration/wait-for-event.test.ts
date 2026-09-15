import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineEvent } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { WaitForEventTimeoutError } from "../../src/index";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";

const event = defineEvent({
	name: "wait.order",
	payload: z.object({ id: z.string(), kind: z.enum(["match", "other"]) }),
	filterable: ["kind"],
});
const taskDefinition = defineTask({ name: "wait.task", payload: z.object({ id: z.string() }) });

type Handler = (id: string, ctx: any) => Promise<void>;

async function until(check: () => Promise<boolean>, timeout = 20_000) {
	const end = Date.now() + timeout;
	while (Date.now() < end) {
		if (await check()) return;
		await Bun.sleep(10);
	}
	throw new Error("condition was not met");
}

async function setup(db: TestDatabase, fn: Handler, orchestrators: Orchestrator[] = []) {
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([taskDefinition]),
		events: EventSchemas.fromSchema([event]),
		context: {},
	});
	const task = conductor.createTask(
		{ name: "wait.task" },
		{ invocable: true },
		async (taskEvent, ctx) => fn(String(taskEvent.payload?.id), ctx),
	);
	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [task],
		defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
	});
	orchestrators.push(orchestrator);
	await orchestrator.start();
	return { conductor, orchestrator };
}

describe.serial("waitForEvent", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const orchestrators: Orchestrator[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		// Stop workers before closing their database connections. In particular, a
		// poll or event processor can otherwise be between two database queries.
		await Promise.allSettled(orchestrators.map((orchestrator) => orchestrator.stop()));
		orchestrators.length = 0;
		await Promise.all(databases.map((database) => database.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	async function db() {
		const database = await pool.child();
		databases.push(database);
		await Conductor.create({ sql: database.sql, context: {} }).ensureInstalled();
		return database;
	}

	async function waiting(database: TestDatabase, count = 1) {
		await until(
			async () =>
				Number(
					(
						await database.sql<{ n: number }[]>`
							select count(*)::int as n
							from pgconductor._private_event_subscriptions
							where kind = 'execution_wait'
						`
					)[0]?.n ?? 0,
				) === count,
		);
	}

	test.serial(
		"matches filters, fans out, and replays a step without another subscription",
		async () => {
			const database = await db();
			const entries: string[] = [];
			const { conductor } = await setup(
				database,
				async (id, ctx) => {
					entries.push(`entered:${id}`);
					const result = await ctx.waitForEvent(`order:${id}`, {
						event,
						filter: { kind: ["match"] },
					});
					entries.push(`result:${result.payload.id}`);
					const replay = await ctx.waitForEvent(`order:${id}`, { event });
					entries.push(`replay:${replay.payload.id}`);
				},
				orchestrators,
			);

			const one = await conductor.invoke({ name: "wait.task" }, { id: "one" });
			const two = await conductor.invoke({ name: "wait.task" }, { id: "two" });
			await waiting(database, 2);
			await conductor.emit("wait.order", { id: "no", kind: "other" });
			await Bun.sleep(100);
			expect(entries).toEqual(["entered:one", "entered:two"]);
			await conductor.emit("wait.order", { id: "yes", kind: "match" });
			await until(async () => entries.filter((entry) => entry.startsWith("replay:")).length === 2);
			expect(entries).toEqual([
				"entered:one",
				"entered:two",
				"entered:one",
				"result:yes",
				"replay:yes",
				"entered:two",
				"result:yes",
				"replay:yes",
			]);
			expect(one).not.toBe(two);
		},
	);

	test.serial("uses a stable payload-based key when a timeout resumes the function", async () => {
		const database = await db();
		const entered: string[] = [];
		const errors: unknown[] = [];
		const { conductor } = await setup(
			database,
			async (id, ctx) => {
				entered.push(id);
				try {
					await ctx.waitForEvent(`timeout:${id}`, { event, timeout: "20ms" });
				} catch (error) {
					errors.push(error);
				}
			},
			orchestrators,
		);

		await conductor.invoke({ name: "wait.task" }, { id: "one" });
		await until(async () => errors.length === 1);
		expect(entered).toEqual(["one", "one"]);
		expect(errors).toHaveLength(1);
		expect(errors[0]).toBeInstanceOf(WaitForEventTimeoutError);
	});

	test.serial("events emitted immediately after registration win the wait", async () => {
		const database = await db();
		const result: string[] = [];
		const { conductor } = await setup(
			database,
			async (id, ctx) => {
				const value = await ctx.waitForEvent(`immediate:${id}`, { event });
				result.push(value.payload.id);
			},
			orchestrators,
		);

		await conductor.invoke({ name: "wait.task" }, { id: "race" });
		await waiting(database);
		await conductor.emit("wait.order", { id: "race", kind: "match" });
		await until(async () => result.length === 1);
		expect(result).toEqual(["race"]);
	});

	test.serial("does not deliver events emitted before the subscription boundary", async () => {
		const database = await db();
		const result: string[] = [];
		await database.client.emitEvent({
			eventKey: "wait.order",
			payload: { id: "old", kind: "match" },
		});
		const handler: Handler = async (id, ctx) => {
			const value = await ctx.waitForEvent(`boundary:${id}`, { event });
			result.push(value.payload.id);
		};
		const first = await setup(database, handler, orchestrators);
		const executionId = await first.conductor.invoke({ name: "wait.task" }, { id: "run" });
		await waiting(database);
		await Bun.sleep(100);
		expect(result).toEqual([]);
		await first.orchestrator.stop();
		const second = await setup(database, handler, orchestrators);
		await second.conductor.emit("wait.order", { id: "new", kind: "match" });
		await until(async () => result.length === 1);
		expect(result).toEqual(["new"]);
		expect(executionId).toBeTruthy();
	});

	test.serial("restarts the same handler with the same step key", async () => {
		const database = await db();
		const entered: string[] = [];
		const result: string[] = [];
		const handler: Handler = async (id, ctx) => {
			entered.push(id);
			const value = await ctx.waitForEvent(`restart:${id}`, { event });
			result.push(value.payload.id);
		};
		const first = await setup(database, handler, orchestrators);
		await first.conductor.invoke({ name: "wait.task" }, { id: "restart" });
		await waiting(database);
		await first.orchestrator.stop();
		const second = await setup(database, handler, orchestrators);
		await second.conductor.emit("wait.order", { id: "restart", kind: "match" });
		await until(async () => result.length === 1);
		expect(entered).toEqual(["restart", "restart"]);
		expect(result).toEqual(["restart"]);
	});

	test.serial("delivers the oldest matching event", async () => {
		const database = await db();
		const result: string[] = [];
		const { conductor } = await setup(
			database,
			async (id, ctx) => {
				const value = await ctx.waitForEvent(`oldest:${id}`, {
					event,
					filter: { kind: ["match"] },
				});
				result.push(value.payload.id);
			},
			orchestrators,
		);
		await conductor.invoke({ name: "wait.task" }, { id: "oldest" });
		await waiting(database);
		await conductor.emit("wait.order", { id: "first", kind: "match" });
		await Bun.sleep(5);
		await conductor.emit("wait.order", { id: "second", kind: "match" });
		await Bun.sleep(5);
		await conductor.emit("wait.order", { id: "third", kind: "match" });
		await until(async () => result.length === 1);
		expect(result).toEqual(["first"]);
	});

	test.serial("does not register a wait with a stale execution claim", async () => {
		const database = await db();
		const { conductor, orchestrator } = await setup(database, async () => {}, orchestrators);
		await orchestrator.stop();
		const executionId = await conductor.invoke({ name: "wait.task" }, { id: "stale" });
		if (!executionId) throw new Error("invoke did not return an execution id");
		const registered = await database.client.registerEventWait({
			executionId,
			queue: "default",
			taskKey: "wait.task",
			eventKey: "wait.order",
			stepKey: "stale:stale",
			filter: null,
			timeoutMs: null,
			orchestratorId: crypto.randomUUID(),
		});
		expect(registered).toBe(false);
		const rows = await database.sql<{ n: number; waiting_step_key: string | null }[]>`
			select count(s.*)::int as n, max(e.waiting_step_key) as waiting_step_key
			from pgconductor._private_event_subscriptions s
			right join pgconductor._private_executions e on e.id = ${executionId}::uuid
			where s.execution_id = ${executionId}::uuid
		`;
		expect(Number(rows[0]?.n)).toBe(0);
		expect(rows[0]?.waiting_step_key).toBeNull();
	});

	test.serial(
		"resolves an eligible wait after more than one event batch of child waits",
		async () => {
			const database = await db();
			await database.sql`
			insert into pgconductor._private_tasks (key, queue)
			values ('fairness.task', 'default')
			on conflict (key, queue) do nothing
		`;
			await database.sql`
			insert into pgconductor._private_executions
				(task_key, queue, payload, run_at, waiting_on_execution_id, waiting_step_key)
			select 'fairness.task', 'default', '{}', 'infinity'::timestamptz,
				pgconductor._private_portable_uuidv7(), 'child-step'
			from generate_series(1, 101)
		`;
			const eventId = await database.client.emitEvent({
				eventKey: "fairness.event",
				payload: { id: "eligible" },
			});
			const [execution] = await database.sql<{ id: string }[]>`
			insert into pgconductor._private_executions
				(task_key, queue, payload, run_at, waiting_step_key)
			values ('fairness.task', 'default', '{}', 'infinity'::timestamptz, 'event-step')
			returning id
		`;
			await database.sql`
			insert into pgconductor._private_event_subscriptions
				(task_key, queue, event_key, kind, execution_id, step_key, wait_after_event_position)
			values ('fairness.task', 'default', 'fairness.event', 'execution_wait',
				${execution!.id}::uuid, 'event-step', 0)
		`;

			expect(await database.client.resolveEventWaits({ batchSize: 100 })).toBe(1);
			const [step] = await database.sql<{ result: { result: { payload: { id: string } } } }[]>`
			select result from pgconductor._private_steps
			where execution_id = ${execution!.id}::uuid and key = 'event-step'
		`;
			expect(step?.result.result.payload.id).toBe("eligible");
			expect(eventId).toBeString();
		},
	);

	test.serial("leaves attempts unchanged while an execution waits", async () => {
		const database = await db();
		const { conductor } = await setup(
			database,
			async (id, ctx) => {
				await ctx.waitForEvent(`attempts:${id}`, { event });
			},
			orchestrators,
		);
		const executionId = await conductor.invoke({ name: "wait.task" }, { id: "attempts" });
		if (!executionId) throw new Error("invoke did not return an execution id");
		await waiting(database);
		const rows = await database.sql<{ attempts: number }[]>`
			select attempts from pgconductor._private_executions where id = ${executionId}::uuid
		`;
		expect(Number(rows[0]?.attempts)).toBe(1);
	});

	test.serial("match and timeout produce exactly one terminal outcome", async () => {
		const database = await db();
		const outcomes: string[] = [];
		const { conductor } = await setup(
			database,
			async (id, ctx) => {
				try {
					const value = await ctx.waitForEvent(`outcome:${id}`, { event, timeout: "100ms" });
					outcomes.push(`match:${value.payload.id}`);
				} catch (error) {
					if (error instanceof WaitForEventTimeoutError) outcomes.push("timeout");
					else throw error;
				}
			},
			orchestrators,
		);
		await conductor.invoke({ name: "wait.task" }, { id: "outcome" });
		await waiting(database);
		await conductor.emit("wait.order", { id: "outcome", kind: "match" });
		await until(async () => outcomes.length === 1);
		await Bun.sleep(150);
		expect(outcomes).toEqual(["match:outcome"]);
		expect(
			Number(
				(
					await database.sql`select count(*)::int as n from pgconductor._private_event_subscriptions where kind = 'execution_wait'`
				)[0]?.n,
			),
		).toBe(0);
	});

	test.serial("cancellation removes the wait and prevents a resume", async () => {
		const database = await db();
		const entered: string[] = [];
		const result: string[] = [];
		const { conductor } = await setup(
			database,
			async (id, ctx) => {
				entered.push(id);
				const value = await ctx.waitForEvent(`cancel:${id}`, { event, timeout: "1h" });
				result.push(value.payload.id);
			},
			orchestrators,
		);
		const executionId = await conductor.invoke({ name: "wait.task" }, { id: "cancel" });
		if (!executionId) throw new Error("invoke did not return an execution id");
		await waiting(database);
		await database.client.cancelExecution(executionId);
		await waiting(database, 0);
		await conductor.emit("wait.order", { id: "cancel", kind: "match" });
		await Bun.sleep(150);
		expect(entered).toEqual(["cancel"]);
		expect(result).toEqual([]);
	});

	test.serial(
		"does not duplicate subscriptions or resume an execution twice",
		async () => {
			const database = await db();
			const entered: string[] = [];
			const result: string[] = [];
			const { conductor } = await setup(
				database,
				async (id, ctx) => {
					entered.push(id);
					const value = await ctx.waitForEvent(`once:${id}`, { event });
					result.push(value.payload.id);
				},
				orchestrators,
			);
			await conductor.invoke({ name: "wait.task" }, { id: "once" });
			await waiting(database);
			await Bun.sleep(100);
			expect(
				Number(
					(
						await database.sql`select count(*)::int as n from pgconductor._private_event_subscriptions where kind = 'execution_wait'`
					)[0]?.n,
				),
			).toBe(1);
			await conductor.emit("wait.order", { id: "once", kind: "match" });
			await until(async () => result.length === 1);
			await Bun.sleep(100);
			expect(entered).toEqual(["once", "once"]);
			expect(result).toEqual(["once"]);
		},
		60_000,
	);
});
