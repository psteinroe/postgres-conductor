import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import postgres, { type Sql } from "postgres";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const sessionDefinition = defineTask({ name: "session", payload: z.object({ id: z.string() }) });
const toolDefinition = defineTask({ name: "tool", returns: z.object({ done: z.boolean() }) });

type Handler = (id: string, ctx: any) => Promise<void>;

// Holds the response of the first statement with `marker` among its parameters until `until` resolves
function holdFirstResponse(sql: Sql, marker: string, until: () => Promise<void>): Sql {
	let held = false;
	return new Proxy(sql, {
		apply(target, thisArg, args) {
			const query = Reflect.apply(target, thisArg, args);
			if (held || !args.slice(1).includes(marker)) return query;
			held = true;
			return query.then(async (rows: unknown) => {
				await until();
				return rows;
			});
		},
	});
}

describe("ctx.waitForAny signal branches (Postgres integration)", () => {
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

	async function db() {
		const database = await pool.child();
		databases.push(database);
		await Conductor.create({ sql: database.sql, context: {} }).ensureInstalled();
		return database;
	}

	async function setup(
		database: TestDatabase,
		{
			session,
			tool = async () => {},
			orchestratorCount = 1,
			clock = false,
			sql = database.sql,
		}: {
			session: Handler;
			tool?: () => Promise<void>;
			orchestratorCount?: number;
			clock?: boolean;
			sql?: Sql;
		},
	) {
		const conductor = Conductor.create({
			sql,
			tasks: TaskSchemas.fromSchema([sessionDefinition, toolDefinition]),
			context: {},
			clock: clock ? database.clock : undefined,
		});
		const tasks = [
			conductor.createTask({ name: "session" }, { invocable: true }, async (event, ctx) =>
				session(event.payload.id, ctx),
			),
			conductor.createTask({ name: "tool" }, { invocable: true }, async () => {
				await tool();
				return { done: true };
			}),
		];
		const start = async () => {
			const orchestrator = Orchestrator.create({
				conductor,
				tasks,
				defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10, concurrency: 4 },
			});
			orchestrators.push(orchestrator);
			await orchestrator.start();
		};
		for (let i = 0; i < orchestratorCount; i++) await start();
		return { conductor, start };
	}

	async function stopOrchestrators() {
		await Promise.all(orchestrators.map((orchestrator) => orchestrator.stop()));
	}

	async function step(database: TestDatabase, executionId: string, key: string) {
		const [row] = await database.sql<{ result: unknown }[]>`
			select result from pgconductor._private_steps
			where execution_id = ${executionId}::uuid and key = ${key}
		`;
		return row?.result;
	}

	async function suspendedAt(database: TestDatabase, executionId: string, stepKey: string) {
		await waitForCondition(async () => {
			const [row] = await database.sql`
				select 1 from pgconductor._private_executions
				where id = ${executionId}::uuid and locked_by is null and waiting_step_key = ${stepKey}
			`;
			return row !== undefined;
		});
	}

	async function waits(database: TestDatabase) {
		const [row] = await database.sql<{ n: number }[]>`
			select count(*)::int as n from pgconductor._private_custom_event_subscriptions
			where kind = 'execution_wait'
		`;
		return row?.n;
	}

	// Claims a session without a worker and returns its claim token
	async function claim(database: TestDatabase, sessionId: string) {
		const orchestratorId = await database.registerOrchestrator();
		const { executions } = await database.client.getExecutions({
			orchestratorId,
			queueName: "default",
			batchSize: 10,
			taskKeys: ["session"],
		});
		const claimToken = executions.find((execution) => execution.id === sessionId)?.claim_token;
		return { orchestratorId, claimToken: claimToken || "" };
	}

	function registerIdle(
		sql: postgres.Sql,
		sessionId: string,
		{ orchestratorId, claimToken }: { orchestratorId: string; claimToken: string },
	) {
		return sql<{ result: unknown }[]>`
			select pgconductor._private_register_event_race(
				${sessionId}::uuid, 'default', 'session', ${orchestratorId}::uuid, ${claimToken}::uuid,
				'idle',
				${sql.json([{ key: "input", stepKey: "idle:input", signal: "input" }])}::jsonb,
				null
			) as result
		`;
	}

	async function awake(database: TestDatabase, sessionId: string) {
		const [row] = await database.sql<{ waiting_step_key: string | null; due: boolean }[]>`
			select waiting_step_key, run_at <= pgconductor._private_current_time() as due
			from pgconductor._private_executions where id = ${sessionId}::uuid
		`;
		return row;
	}

	test("a signal stored before the wait wins without suspending", async () => {
		const database = await db();
		let entered = 0;
		const winners: unknown[] = [];
		const { conductor, start } = await setup(database, {
			session: async (_id, ctx) => {
				entered++;
				winners.push(await ctx.waitForAny("idle", { input: { signal: "input" } }));
			},
			orchestratorCount: 0,
		});

		const sessionId = await conductor.invoke({ name: "session" }, { id: "before" });
		await conductor.signal(sessionId, { name: "input", id: "first" });
		await conductor.signal(sessionId, { name: "input", id: "second" });
		await start();
		await conductor.waitForResult(sessionId, { pollIntervalMs: 10 });

		expect(entered).toBe(1);
		expect(winners).toEqual([{ key: "input", signal: { id: "first", name: "input" } }]);
		expect(await waits(database)).toBe(0);
	});

	test("a signal whose handler is running when the wait starts wins", async () => {
		const database = await db();
		const handling = new Deferred<void>();
		const release = new Deferred<void>();
		const received: string[] = [];
		const winners: unknown[] = [];
		const { conductor } = await setup(database, {
			session: async (_id, ctx) => {
				ctx.onSignal("input", async (_payload: unknown, { id }: { id: string }) => {
					handling.resolve();
					await release.promise;
					received.push(id);
				});
				await handling.promise;
				const winner = ctx.waitForAny("idle", { input: { signal: "input" } });
				release.resolve();
				winners.push(await winner);
			},
		});

		const sessionId = await conductor.invoke({ name: "session" }, { id: "running" });
		await conductor.signal(sessionId, { name: "input", id: "first" });
		await conductor.waitForResult(sessionId, { pollIntervalMs: 10 });

		expect(received).toEqual(["first"]);
		expect(winners).toEqual([{ key: "input", signal: { id: "first", name: "input" } }]);
	});

	test("a signal whose handler returns while the wait reads its step still wins", async () => {
		const database = await db();
		const handling = new Deferred<void>();
		const release = new Deferred<void>();
		const winners: unknown[] = [];
		// The step read returns only once the handler returned and its signal is acknowledged
		const sql = holdFirstResponse(database.sql, "idle-after-ack", () =>
			waitForCondition(async () => {
				const [row] = await database.sql`
					select 1 from pgconductor._private_execution_signals
					where id = 'first' and delivered_at is not null
				`;
				return row !== undefined;
			}),
		);
		const { conductor } = await setup(database, {
			session: async (_id, ctx) => {
				ctx.onSignal("input", async () => {
					handling.resolve();
					await release.promise;
				});
				await handling.promise;
				const winner = ctx.waitForAny("idle-after-ack", { input: { signal: "input" } });
				release.resolve();
				winners.push(await winner);
			},
			sql,
		});

		const sessionId = await conductor.invoke({ name: "session" }, { id: "acked" });
		await conductor.signal(sessionId, { name: "input", id: "first" });
		await conductor.waitForResult(sessionId, { pollIntervalMs: 10 });

		expect(winners).toEqual([{ key: "input", signal: { id: "first", name: "input" } }]);
	});

	test("a signal wakes a suspended wait, and replays return the memoized winner", async () => {
		const database = await db();
		const winners: unknown[] = [];
		const received: string[] = [];
		const { conductor } = await setup(database, {
			session: async (_id, ctx) => {
				ctx.onSignal("input", (_payload: unknown, { id }: { id: string }) => {
					received.push(id);
				});
				winners.push(await ctx.waitForAny("idle", { input: { signal: "input" } }));
				await ctx.sleep("pause", 0);
			},
		});

		const sessionId = await conductor.invoke({ name: "session" }, { id: "wake" });
		await suspendedAt(database, sessionId, "idle");
		expect(await step(database, sessionId, "idle:input")).toBeUndefined();
		expect(await conductor.signal(sessionId, { name: "input", id: "first", payload: 1 })).toBe(
			true,
		);
		await conductor.waitForResult(sessionId, { pollIntervalMs: 10 });

		const winner = { key: "input", signal: { id: "first", name: "input" } };
		expect(winners).toEqual([winner, winner]);
		expect(await step(database, sessionId, "idle")).toEqual(winner);
		// the wait does not consume the signal
		expect([...new Set(received)]).toEqual(["first"]);
		expect(await waits(database)).toBe(0);
	});

	test("a signal sent while the wait registers wakes it", async () => {
		const database = await db();
		const { conductor } = await setup(database, { session: async () => {} });
		await stopOrchestrators();
		const sessionId = await conductor.invoke({ name: "session" }, { id: "race" });
		const claimed = await claim(database, sessionId);
		const registration = postgres(database.url, { max: 1 });
		const sender = postgres(database.url, { max: 1 });
		const observer = postgres(database.url, { max: 1 });
		const registered = new Deferred<void>();
		const commit = new Deferred<void>();

		try {
			// The registration holds the session row until the signal waits for it
			const registering = registration.begin(async (sql) => {
				const [row] = await registerIdle(sql, sessionId, claimed);
				registered.resolve();
				await commit.promise;
				return row?.result;
			});
			await registered.promise;
			const signalling = sender`
				select pgconductor.signal_execution(${sessionId}::uuid, 'input', 'first') as stored
			`.execute();
			await waitForCondition(async () => {
				const rows = await observer`
					select 1 from pg_stat_activity
					where datname = current_database() and wait_event_type = 'Lock'
				`;
				return rows.length === 1;
			});
			commit.resolve();
			expect(await registering).toBeNull();
			expect((await signalling)[0]?.stored).toBe(true);
		} finally {
			await registration.end();
			await sender.end();
			await observer.end();
		}

		expect(await step(database, sessionId, "idle:input")).toEqual({
			status: "resolved",
			signal: { id: "first", name: "input" },
		});
		expect(await awake(database, sessionId)).toEqual({ waiting_step_key: null, due: true });
		expect(await waits(database)).toBe(0);
	});

	test("a wait registered while a signal is stored wins without suspending", async () => {
		const database = await db();
		const { conductor } = await setup(database, { session: async () => {} });
		await stopOrchestrators();
		const sessionId = await conductor.invoke({ name: "session" }, { id: "race" });
		const claimed = await claim(database, sessionId);
		const registration = postgres(database.url, { max: 1 });
		const sender = postgres(database.url, { max: 1 });
		const observer = postgres(database.url, { max: 1 });
		const stored = new Deferred<void>();
		const commit = new Deferred<void>();

		try {
			// The signal holds the session row until the registration waits for it
			const signalling = sender.begin(async (sql) => {
				await sql`select pgconductor.signal_execution(${sessionId}::uuid, 'input', 'first')`;
				stored.resolve();
				await commit.promise;
			});
			await stored.promise;
			const registering = registerIdle(registration, sessionId, claimed).execute();
			await waitForCondition(async () => {
				const rows = await observer`
					select 1 from pg_stat_activity
					where datname = current_database() and wait_event_type = 'Lock'
				`;
				return rows.length === 1;
			});
			commit.resolve();
			await signalling;
			expect((await registering)[0]?.result).toEqual({
				key: "input",
				signal: { id: "first", name: "input" },
			});
		} finally {
			await registration.end();
			await sender.end();
			await observer.end();
		}

		expect(await waits(database)).toBe(0);
	});

	test("waitForAny picks one winner when a signal and an execution settle at once", async () => {
		const database = await db();
		const release = new Deferred<void>();
		const winners: Record<string, unknown[]> = {};
		const received: Record<string, string[]> = {};
		const { conductor } = await setup(database, {
			session: async (id, ctx) => {
				ctx.onSignal("input", (_payload: unknown, info: { id: string }) => {
					received[id] = [...(received[id] || []), info.id];
				});
				const toolId = await ctx.invoke("start", { name: "tool" });
				const winner = await ctx.waitForAny("idle", {
					input: { signal: "input" },
					tool: { execution: toolId },
				});
				winners[id] = [...(winners[id] || []), winner];
			},
			tool: () => release.promise,
			orchestratorCount: 2,
		});

		// fewer sessions than worker slots, so the blocked tools leave room for every session
		const ids = Array.from({ length: 6 }, (_, index) => String(index));
		const sessions = new Map<string, string>();
		for (const id of ids) {
			sessions.set(id, await conductor.invoke({ name: "session" }, { id }));
		}
		for (const sessionId of sessions.values()) {
			await suspendedAt(database, sessionId, "idle");
		}
		release.resolve();
		await Promise.all(
			[...sessions.values()].map((sessionId) =>
				conductor.signal(sessionId, { name: "input", id: "first" }),
			),
		);
		for (const sessionId of sessions.values()) {
			await conductor.waitForResult(sessionId, { pollIntervalMs: 10 });
		}

		for (const [id, sessionId] of sessions) {
			expect(winners[id]).toEqual([await step(database, sessionId, "idle")]);
		}
		expect(await waits(database)).toBe(0);
	});

	test("signalWithStart wakes a keyed session waiting for input or its tool", async () => {
		const database = await db();
		const release = new Deferred<void>();
		const inputs: string[] = [];
		const winners: { key: string }[] = [];
		const { conductor } = await setup(database, {
			session: async (_id, ctx) => {
				ctx.onSignal("input", (_payload: unknown, { id }: { id: string }) => {
					inputs.push(id);
				});
				const toolId = await ctx.invoke("start-tool", { name: "tool" });
				for (let n = 0; ; n++) {
					const winner = await ctx.waitForAny(`idle:${n}`, {
						input: { signal: "input" },
						tool: { execution: toolId },
					});
					winners.push(winner);
					if (winner.key === "tool") return;
				}
			},
			tool: () => release.promise,
		});
		const start = (id: string) =>
			conductor.signalWithStart({
				task: "session",
				key: "thread-1",
				signal: { name: "input", id },
				input: { id: "keyed" },
			});
		// The wait the session suspends at depends on whether "first" was delivered before it
		const suspended = async (sessionId: string) => {
			let key: string | null = null;
			await waitForCondition(async () => {
				const [row] = await database.sql<{ waiting_step_key: string }[]>`
					select waiting_step_key from pgconductor._private_executions
					where id = ${sessionId}::uuid and locked_by is null and waiting_step_key like 'idle:%'
				`;
				key = row?.waiting_step_key || null;
				return key !== null;
			});
			return key || "";
		};

		const sessionId = await start("first");
		const idle = await suspended(sessionId);
		expect(await start("second")).toBe(sessionId);
		expect(await step(database, sessionId, `${idle}:input`)).toEqual({
			status: "resolved",
			signal: { id: "second", name: "input" },
		});
		await waitForCondition(() => inputs.includes("second"));
		await suspended(sessionId);
		release.resolve();
		await conductor.waitForResult(sessionId, { pollIntervalMs: 10 });

		expect(await step(database, sessionId, idle)).toEqual({
			key: "input",
			signal: { id: "second", name: "input" },
		});
		expect(winners.at(-1)?.key).toBe("tool");
		expect(new Set(inputs)).toEqual(new Set(["first", "second"]));
		expect(await waits(database)).toBe(0);
	});

	test("a signal for another name does not wake the wait", async () => {
		const database = await db();
		const { conductor } = await setup(database, {
			session: async (_id, ctx) => {
				await ctx.waitForAny("idle", { input: { signal: "input" } });
			},
		});

		const sessionId = await conductor.invoke({ name: "session" }, { id: "other" });
		await suspendedAt(database, sessionId, "idle");
		expect(await conductor.signal(sessionId, { name: "other", id: "o1" })).toBe(true);

		const [row] = await database.sql<{ waiting_step_key: string; run_at: unknown }[]>`
			select waiting_step_key, run_at::text from pgconductor._private_executions
			where id = ${sessionId}::uuid
		`;
		expect(row).toEqual({ waiting_step_key: "idle", run_at: "infinity" });
		expect(await step(database, sessionId, "idle:input")).toBeUndefined();

		await conductor.signal(sessionId, { name: "input", id: "i1" });
		await conductor.waitForResult(sessionId, { pollIntervalMs: 10 });
		expect(await step(database, sessionId, "idle")).toEqual({
			key: "input",
			signal: { id: "i1", name: "input" },
		});
	});

	test("under a set test clock, a signal wakes the wait and the timeout wins once it passed", async () => {
		const database = await db();
		await database.clock.set(new Date("2024-01-01T12:00:00Z"));
		const { conductor, start } = await setup(database, {
			session: async (_id, ctx) => {
				await ctx.waitForAny("idle", { input: { signal: "input" } }, { timeout: "1h" });
			},
			clock: true,
		});

		const woken = await conductor.invoke({ name: "session" }, { id: "woken" });
		await suspendedAt(database, woken, "idle");
		await conductor.signal(woken, { name: "input", id: "i1" });
		await conductor.waitForResult(woken, { pollIntervalMs: 10 });
		expect(await step(database, woken, "idle")).toEqual({
			key: "input",
			signal: { id: "i1", name: "input" },
		});

		const late = await conductor.invoke({ name: "session" }, { id: "late" });
		await suspendedAt(database, late, "idle");
		await stopOrchestrators();
		await database.clock.advance("2h");
		await conductor.signal(late, { name: "input", id: "i1" });
		await start();
		await conductor.waitForResult(late, { pollIntervalMs: 10 });
		expect(await step(database, late, "idle")).toEqual({ key: "timeout" });
	});
});
