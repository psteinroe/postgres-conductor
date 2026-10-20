import { setTimeout as sleep } from "node:timers/promises";
import postgres from "postgres";
import { z } from "zod";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineEvent } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

let pool: TestDatabasePool;

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterAll(async () => {
	await pool?.destroy();
});

const ping = defineEvent({ name: "ping", payload: z.object({}) });

// An outcome that settles after the deadline of a result wait, but before the waiter resumes,
// does not win it
describe.each([
	{
		name: "waitForResult with a completion",
		cancel: false,
		wait: (ctx: any, id: string) => ctx.waitForResult("result", id, { timeout: "1s" }),
		timeout: { status: "timeout" },
	},
	{
		name: "waitForResult with a cancellation",
		cancel: true,
		wait: (ctx: any, id: string) => ctx.waitForResult("result", id, { timeout: "1s" }),
		timeout: { status: "timeout" },
	},
	{
		name: "waitForAny with an event and a completion",
		cancel: false,
		wait: async (ctx: any, id: string) => {
			const sub = await ctx.subscribe("ping", { event: ping });
			return ctx.waitForAny("result", { ping: sub, work: { execution: id } }, { timeout: "1s" });
		},
		timeout: { key: "timeout" },
	},
])("$name settled after the deadline", ({ cancel, wait, timeout }) => {
	test("times out", async () => {
		const db = await pool.child();
		await db.clock.set(new Date("2024-01-01T12:00:00Z"));

		const conductor = Conductor.create({
			sql: db.sql,
			clock: db.clock,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "waiter", payload: z.object({}) }),
				defineTask({
					name: "work",
					queue: "work",
					payload: z.object({}),
					returns: z.object({ value: z.number() }),
				}),
			]),
			events: [ping],
			context: {},
		});
		await conductor.ensureInstalled();

		const release = new Deferred<void>();
		const outcomes: unknown[] = [];
		const workIds: string[] = [];
		const waiter = conductor.createTask({ name: "waiter" }, { invocable: true }, async (_, ctx) => {
			const id = await ctx.invoke("start", { name: "work", queue: "work" }, {});
			workIds.push(id);
			outcomes.push(await wait(ctx, id));
		});
		const work = conductor.createTask(
			{ name: "work", queue: "work" },
			{ invocable: true },
			async () => {
				await release.promise;
				return { value: 42 };
			},
		);

		const config = { pollIntervalMs: 10, flushIntervalMs: 10 };
		const workers = Orchestrator.create({
			conductor,
			workers: [conductor.createWorker({ queue: "work", tasks: [work], config })],
		});
		const waiters = () =>
			Orchestrator.create({ conductor, tasks: [waiter], defaultWorker: config });

		await workers.start();
		const first = waiters();
		await first.start();
		try {
			const waiterId = await conductor.invoke({ name: "waiter" }, {});

			// The waiter suspends with its deadline at 12:00:01, and nothing polls it afterwards
			await waitForCondition(async () => {
				const [row] = await db.sql`
					select 1 from pgconductor._private_executions
					where id = ${waiterId} and locked_by is null and waiting_step_key = 'result'
				`;
				return row !== undefined;
			});
			await first.stop();

			// The deadline passes, then the awaited execution settles
			await db.clock.set(new Date("2024-01-01T12:00:02Z"));
			const workId = workIds[0] || "";
			if (cancel) await conductor.cancel(workId);
			else release.resolve();
			await waitForCondition(async () => {
				const [row] = await db.sql`
					select 1 from pgconductor._private_executions
					where id = ${workId} and (completed_at is not null or failed_at is not null)
				`;
				return row !== undefined;
			});

			// The waiter resumes at 12:00:03, after its deadline
			await db.clock.set(new Date("2024-01-01T12:00:03Z"));
			const second = waiters();
			await second.start();
			try {
				await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });
			} finally {
				await second.stop();
			}

			expect(outcomes).toEqual([timeout]);
		} finally {
			release.resolve();
			await first.stop();
			await workers.stop();
		}
	});
});

// A race without an open branch has nowhere to keep its deadline, so it fails instead of waiting
describe("waitForAny without an open branch", () => {
	async function race(handler: (ctx: any) => Promise<unknown>) {
		const db = await pool.child();
		await db.clock.set(new Date("2024-01-01T12:00:00Z"));
		const conductor = Conductor.create({
			sql: db.sql,
			clock: db.clock,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "racer", payload: z.object({}) })]),
			events: [ping],
			context: {},
		});
		await conductor.ensureInstalled();
		const racer = conductor.createTask(
			{ name: "racer", maxAttempts: 1 },
			{ invocable: true },
			async (_, ctx) => {
				await handler(ctx);
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [racer],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		const id = await conductor.invoke({ name: "racer" }, {});
		const read = async () => {
			const [row] = await db.sql<
				{ run_at: Date; waiting_step_key: string | null; failed: boolean; last_error: string }[]
			>`
				select run_at, waiting_step_key, failed_at is not null as failed, last_error
				from pgconductor._private_executions where id = ${id}
			`;
			if (!row) throw new Error("execution not found");
			return row;
		};
		return { db, orchestrator, read };
	}

	test("with no branches", async () => {
		const { orchestrator, read } = await race((ctx) =>
			ctx.waitForAny("race", {}, { timeout: "1s" }),
		);
		try {
			await waitForCondition(async () => (await read()).failed);
			expect((await read()).last_error).toBe('waitForAny "race" has no open branch');
		} finally {
			await orchestrator.stop();
		}
	});

	test("with only a branch an earlier race closed", async () => {
		const { db, orchestrator, read } = await race(async (ctx) => {
			const sub = await ctx.subscribe("sub", { event: ping });
			await ctx.waitForAny("first", { a: sub }, { timeout: "1s" });
			await ctx.waitForAny("second", { a: sub }, { timeout: "1s" });
		});
		try {
			await waitForCondition(async () => (await read()).waiting_step_key === "first");
			await db.clock.set(new Date((await read()).run_at.getTime() + 1000));
			await waitForCondition(async () => (await read()).failed);
			expect((await read()).last_error).toBe('waitForAny "second" has no open branch');
		} finally {
			await orchestrator.stop();
		}
	});
});

const parentDefinition = defineTask({
	name: "parent",
	returns: z.object({ via: z.string() }),
});
const primaryDefinition = defineTask({ name: "primary" });
const fallbackDefinition = defineTask({
	name: "fallback",
	returns: z.object({ ok: z.boolean() }),
});
const tasks = TaskSchemas.fromSchema([parentDefinition, primaryDefinition, fallbackDefinition]);

test("a caught invokeAndWait timeout does not fail the next child invocation", async () => {
	const db = await pool.child();
	const start = new Date("2024-01-01T00:00:00Z");
	await db.clock.set(start);
	const conductor = Conductor.create({ sql: db.sql, tasks, context: {}, clock: db.clock });
	const errors: string[] = [];

	const primary = conductor.createTask(
		{ name: "primary" },
		{ invocable: true },
		async (_e, ctx) => {
			await ctx.sleep("long", 60_000);
		},
	);
	const fallback = conductor.createTask({ name: "fallback" }, { invocable: true }, async () => ({
		ok: true,
	}));
	const parent = conductor.createTask(
		{ name: "parent", maxAttempts: 1 },
		{ invocable: true },
		async (_e, ctx) => {
			try {
				await ctx.invokeAndWait("primary", { name: "primary" }, {}, { timeout: 1000 });
				return { via: "primary" };
			} catch (err) {
				errors.push((err as Error).message);
			}
			try {
				await ctx.invokeAndWait("fallback", { name: "fallback" }, {});
			} catch (err) {
				errors.push((err as Error).message);
				throw err;
			}
			return { via: "fallback" };
		},
	);

	await conductor.ensureInstalled();
	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [parent, primary, fallback],
		defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
	});
	try {
		await orchestrator.start();
		const parentId = await conductor.invoke({ name: "parent" }, {});

		// The primary child is asleep and the parent waits on it
		await waitForCondition(async () => {
			const [r] = await db.sql<{ sleeping: boolean }[]>`
				select exists (
					select 1 from pgconductor._private_steps s
					join pgconductor._private_executions e on e.id = s.execution_id
					where e.task_key = 'primary' and s.key = 'long'
				) and exists (
					select 1 from pgconductor._private_executions
					where id = ${parentId}::uuid and waiting_on_execution_id is not null and locked_by is null
				) as sleeping
			`;
			return r?.sleeping === true;
		});
		await db.clock.advance("1500ms");

		await waitForCondition(async () => {
			const [r] = await db.sql<{ settled: boolean }[]>`
				select completed_at is not null or failed_at is not null as settled
				from pgconductor._private_executions where id = ${parentId}::uuid
			`;
			return r?.settled === true;
		});

		const [settled] = await db.sql<{ result: unknown; last_error: string | null }[]>`
			select result, last_error from pgconductor._private_executions where id = ${parentId}::uuid
		`;
		const fallbackRuns = await db.sql`
			select id from pgconductor._private_executions where task_key = 'fallback'
		`;
		// The memoized timeout is thrown again when the handler replays after the fallback
		expect({
			errors,
			fallbackExecutions: fallbackRuns.length,
			result: settled?.result,
			lastError: settled?.last_error,
		}).toEqual({
			errors: ["Child execution timed out after 1000ms", "Child execution timed out after 1000ms"],
			fallbackExecutions: 1,
			result: { via: "fallback" },
			lastError: null,
		});
	} finally {
		await orchestrator.stop();
	}
});

// Found while reproducing C23: the timeout is not memoized, so a replay after a caught timeout
// (here, resuming from a later sleep) invokes the timed-out child again and waits for it.
test("a replay after a caught invokeAndWait timeout does not invoke the child again", async () => {
	const db = await pool.child();
	const start = new Date("2024-01-01T00:00:00Z");
	await db.clock.set(start);
	const conductor = Conductor.create({ sql: db.sql, tasks, context: {}, clock: db.clock });
	const errors: string[] = [];

	const primary = conductor.createTask(
		{ name: "primary" },
		{ invocable: true },
		async (_e, ctx) => {
			await ctx.sleep("long", 60_000);
		},
	);
	const parent = conductor.createTask(
		{ name: "parent", maxAttempts: 1 },
		{ invocable: true },
		async (_e, ctx) => {
			try {
				await ctx.invokeAndWait("primary", { name: "primary" }, {}, { timeout: 1000 });
			} catch (err) {
				errors.push((err as Error).message);
			}
			await ctx.sleep("after", 1000);
			return { via: "sleep" };
		},
	);

	await conductor.ensureInstalled();
	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [parent, primary],
		defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
	});
	const parentState = async (id: string) => {
		const [r] = await db.sql<{ completed: boolean; sleeping: boolean; primaries: number }[]>`
			select
				completed_at is not null as completed,
				locked_by is null and exists (
					select 1 from pgconductor._private_steps s where s.execution_id = e.id and s.key = 'after'
				) as sleeping,
				(select count(*)::int from pgconductor._private_executions where task_key = 'primary') as primaries
			from pgconductor._private_executions e where id = ${id}::uuid
		`;
		return r;
	};
	try {
		await orchestrator.start();
		const parentId = await conductor.invoke({ name: "parent" }, {});
		await waitForCondition(async () => {
			const [r] = await db.sql<{ waiting: boolean }[]>`
				select waiting_on_execution_id is not null and locked_by is null as waiting
				from pgconductor._private_executions where id = ${parentId}::uuid
			`;
			return r?.waiting === true;
		});
		await db.clock.advance("1500ms");
		await waitForCondition(async () => (await parentState(parentId))?.sleeping === true);
		await db.clock.advance("1500ms");
		// Either the replay completes, or it invoked the primary child again and waits on it
		await waitForCondition(async () => {
			const state = await parentState(parentId);
			return state?.completed === true || (state?.primaries || 0) > 1;
		});

		const { completed, primaries } = (await parentState(parentId)) || {};
		expect({ errors, completed, primaries }).toEqual({
			errors: ["Child execution timed out after 1000ms", "Child execution timed out after 1000ms"],
			completed: true,
			primaries: 1,
		});
	} finally {
		await orchestrator.stop();
	}
});

test("conductor.waitForResult times out while its read waits on a lock", async () => {
	const db = await pool.child();
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([primaryDefinition]),
		context: {},
	});
	await conductor.ensureInstalled();
	const primary = conductor.createTask({ name: "primary" }, { invocable: true }, async () => {});
	const id = await conductor.invoke({ name: "primary" }, {});
	await Orchestrator.create({ conductor, tasks: [primary] }).drain();

	const locker = postgres(db.url, { max: 1 });
	const locked = new Deferred<void>();
	const unlock = new Deferred<void>();
	const holding = locker.begin(async (tx) => {
		await tx`lock table pgconductor._private_executions in access exclusive mode`;
		locked.resolve();
		await unlock.promise;
	});
	try {
		await locked.promise;
		const outcome = await Promise.race([
			conductor.waitForResult(id, { timeout: 100 }).then(
				() => "resolved",
				(error: Error) => error.name,
			),
			sleep(5000).then(() => "blocked"),
		]);
		expect(outcome).toBe("TimeoutError");
	} finally {
		unlock.resolve();
		await holding;
		await locker.end();
	}
	// The read was cancelled, so the connection is free again
	expect(await conductor.waitForResult(id)).toBe(null);
});

describe("durations", () => {
	test("an invokeAndWait timeout of 0 times out at once", async () => {
		const db = await pool.child();
		const conductor = Conductor.create({ sql: db.sql, tasks, context: {} });
		const primary = conductor.createTask(
			{ name: "primary" },
			{ invocable: true },
			async (_e, ctx) => {
				await ctx.sleep("long", "1h");
			},
		);
		const parent = conductor.createTask(
			{ name: "parent", maxAttempts: 1 },
			{ invocable: true },
			async (_e, ctx) => {
				try {
					await ctx.invokeAndWait("primary", { name: "primary" }, {}, { timeout: 0 });
					return { via: "primary" };
				} catch (err) {
					return { via: (err as Error).message };
				}
			},
		);
		await conductor.ensureInstalled();
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parent, primary],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});
		await orchestrator.start();
		try {
			const parentId = await conductor.invoke({ name: "parent" }, {});
			expect(await conductor.waitForResult(parentId, { pollIntervalMs: 50 })).toEqual({
				via: "Child execution timed out after 0ms",
			});
			const [child] = await db.sql<{ cancelled: boolean }[]>`
				select cancelled from pgconductor._private_executions where task_key = 'primary'
			`;
			expect(child?.cancelled).toBe(true);
		} finally {
			await orchestrator.stop();
		}
	});

	test("conductor.waitForResult with a timeout of 0 rejects at once", async () => {
		const db = await pool.child();
		const conductor = Conductor.create({ sql: db.sql, tasks, context: {} });
		await conductor.ensureInstalled();
		const id = await conductor.invoke({ name: "primary" }, {});
		await expect(conductor.waitForResult(id, { timeout: 0 })).rejects.toThrow(
			expect.objectContaining({ name: "TimeoutError" }),
		);
	});

	test("ctx.sleep takes a duration string", async () => {
		const db = await pool.child();
		await db.clock.set(new Date("2024-01-01T00:00:00Z"));
		const conductor = Conductor.create({ sql: db.sql, tasks, context: {}, clock: db.clock });
		const primary = conductor.createTask(
			{ name: "primary" },
			{ invocable: true },
			async (_e, ctx) => {
				await ctx.sleep("nap", "1m");
			},
		);
		await conductor.ensureInstalled();
		const id = await conductor.invoke({ name: "primary" }, {});
		await Orchestrator.create({ conductor, tasks: [primary] }).drain();
		const [row] = await db.sql<{ run_at: Date }[]>`
			select run_at from pgconductor._private_executions where id = ${id}::uuid
		`;
		expect(row?.run_at).toEqual(new Date("2024-01-01T00:01:00Z"));
	});
});
