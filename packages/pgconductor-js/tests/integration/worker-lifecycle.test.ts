import { setTimeout as sleep } from "node:timers/promises";
import type { Sql } from "postgres";
import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import type { JsonValue } from "../../src/database-client";
import { Orchestrator } from "../../src/orchestrator";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { connectionError, failStatements, waitForCondition, within } from "../test-utils";

let pool: TestDatabasePool;
const databases: TestDatabase[] = [];

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterEach(async () => {
	await Promise.all(databases.map((db) => db.destroy()));
	databases.length = 0;
});

afterAll(async () => {
	await pool?.destroy();
});

async function child(): Promise<TestDatabase> {
	const db = await pool.child();
	databases.push(db);
	return db;
}

type Row = {
	completed: boolean;
	failed: boolean;
	locked: boolean;
	attempts: number;
	last_error: string | null;
	result: JsonValue;
};

async function row(db: TestDatabase, id: string): Promise<Row | undefined> {
	const [r] = await db.sql<Row[]>`
		select completed_at is not null as completed, failed_at is not null as failed,
			locked_by is not null as locked, attempts, last_error, result
		from pgconductor._private_executions where id = ${id}::uuid
	`;
	return r;
}

const sessionDefinition = defineTask({ name: "session" });

describe("signal draining races the attempt's abort", () => {
	async function setup(gate: Promise<void>) {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([sessionDefinition]),
			context: {},
		});
		const handlerStarted = new Deferred<void>();
		const timedOut = new Deferred<void>();
		const task = conductor.createTask(
			{ name: "session", timeoutMs: 200, maxAttempts: 1 },
			{ invocable: true },
			async (_event, ctx) => {
				ctx.onSignal("input", async () => {
					ctx.signal.addEventListener("abort", () => timedOut.resolve());
					handlerStarted.resolve();
					await gate;
				});
				// The main handler returns at once, so the signal handler runs during the drain
			},
		);
		await conductor.ensureInstalled();
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});
		const id = await conductor.invoke({ name: "session" }, {});
		// Stored before the claim, so the inbox is primed when the handler registers
		await conductor.signal(id, { name: "input", id: "s1" });
		await orchestrator.start();
		return { db, id, orchestrator, handlerStarted, timedOut };
	}

	test("a signal handler that outlives timeoutMs does not let the attempt complete", async () => {
		const gate = new Deferred<void>();
		const { db, id, orchestrator, handlerStarted, timedOut } = await setup(gate.promise);
		try {
			await handlerStarted.promise;
			await timedOut.promise;
			gate.resolve();
			await waitForCondition(async () => (await row(db, id))?.locked === false);
			expect(await row(db, id)).toMatchObject({
				completed: false,
				last_error: "Task timed out after 200ms",
			});
		} finally {
			gate.resolve();
			await orchestrator.stop();
		}
	});

	test("a signal handler that never returns does not hold the execution past timeoutMs", async () => {
		const gate = new Deferred<void>();
		const { db, id, orchestrator, handlerStarted, timedOut } = await setup(gate.promise);
		try {
			await handlerStarted.promise;
			await timedOut.promise;
			await waitForCondition(async () => (await row(db, id))?.locked === false, 5000);
			expect(await row(db, id)).toMatchObject({
				completed: false,
				last_error: "Task timed out after 200ms",
			});
		} finally {
			gate.resolve();
			await orchestrator.stop();
		}
	});
});

// Holds the response of the loadStep for `key` until `release` resolves
function delayLoadStep(sql: Sql, key: string, release: Promise<void>, delivered: Deferred<void>) {
	return new Proxy(sql, {
		apply(target, thisArg, args) {
			const query = Reflect.apply(target, thisArg, args);
			const text = String(args[0]);
			if (!text.includes("select result from pgconductor._private_steps") || !args.includes(key)) {
				return query;
			}
			return query.then(async (rows: unknown) => {
				await release;
				delivered.resolve();
				return rows;
			});
		},
	});
}

test("a step whose loadStep returns after the attempt timed out does not run its callback", async () => {
	const db = await child();
	const release = new Deferred<void>();
	const delivered = new Deferred<void>();
	const conductor = Conductor.create({
		sql: delayLoadStep(db.sql, "side-effect", release.promise, delivered),
		tasks: TaskSchemas.fromSchema([sessionDefinition]),
		context: {},
	});
	let callbackRuns = 0;
	const task = conductor.createTask(
		{ name: "session", timeoutMs: 200, maxAttempts: 1 },
		{ invocable: true },
		async (_e, ctx) => {
			await ctx.step("side-effect", () => {
				callbackRuns += 1;
				return "done";
			});
		},
	);
	await conductor.ensureInstalled();
	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [task],
		defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
	});
	try {
		await orchestrator.start();
		const id = await conductor.invoke({ name: "session" }, {});

		// The worker settled the timed-out attempt while loadStep is held
		await waitForCondition(async () => (await row(db, id))?.failed === true);
		release.resolve();
		await delivered.promise;
		// The callback would start within microtasks of the loadStep response
		await sleep(200);

		expect(callbackRuns).toBe(0);
	} finally {
		release.resolve();
		await orchestrator.stop();
	}
});

test("a step whose save is rejected by the claim fence does not continue the handler", async () => {
	const db = await child();
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([sessionDefinition]),
		context: {},
	});
	const stepRunning = new Deferred<void>();
	const finishStep = new Deferred<void>();
	const saved = new Deferred<void>();
	const saveStep = conductor.db.saveStep.bind(conductor.db);
	conductor.db.saveStep = async (args, opts) => {
		const claimed = await saveStep(args, opts);
		saved.resolve();
		return claimed;
	};
	let continued = false;
	const task = conductor.createTask({ name: "session" }, { invocable: true }, async (_e, ctx) => {
		await ctx.step("side-effect", async () => {
			stepRunning.resolve();
			await finishStep.promise;
		});
		continued = true;
	});
	await conductor.ensureInstalled();
	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [task],
		defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
	});
	try {
		await orchestrator.start();
		const id = await conductor.invoke({ name: "session" }, {});
		await stepRunning.promise;

		// Another orchestrator claimed the execution while the step ran
		await db.sql`
			update pgconductor._private_executions
			set locked_by = ${await db.registerOrchestrator()}::uuid, claim_token = gen_random_uuid()
			where id = ${id}::uuid
		`;
		finishStep.resolve();
		await saved.promise;
		await sleep(50);

		const [step] = await db.sql`select 1 from pgconductor._private_steps where key = 'side-effect'`;
		expect({ continued, step }).toEqual({ continued: false, step: undefined });
	} finally {
		finishStep.resolve();
		await orchestrator.stop();
	}
});

const outputs: Record<string, Record<string, JsonValue>> = {
	suspended: { __pgconductorTaskAborted: true, reason: "suspended" },
	released: { __pgconductorTaskAborted: true, reason: "released", reschedule_in_ms: 0 },
	cancelled: { __pgconductorTaskAborted: true, reason: "cancelled" },
	"timed-out": { __pgconductorTaskAborted: true, reason: "timed-out" },
};

test.each(Object.keys(outputs))(
	"a handler returning a %s-shaped object completes with it as the result",
	async (reason) => {
		const output = outputs[reason] || {};
		const db = await child();
		const definition = defineTask({ name: "session", returns: z.record(z.string(), z.json()) });
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		const task = conductor.createTask(
			{ name: "session", maxAttempts: 1 },
			{ invocable: true },
			async () => output,
		);
		await conductor.ensureInstalled();
		const id = await conductor.invoke({ name: "session" }, {});
		await Orchestrator.create({ conductor, tasks: [task] }).drain();

		expect(await row(db, id)).toMatchObject({
			completed: true,
			result: output,
			last_error: null,
			locked: false,
			attempts: 1,
		});
	},
);

test("cancelling a member of a running batch aborts the batch and releases the others", async () => {
	const db = await child();
	const definition = defineTask({ name: "batch", payload: z.object({ n: z.number() }) });
	const conductor = Conductor.create({
		sql: db.sql,
		tasks: TaskSchemas.fromSchema([definition]),
		context: {},
	});
	await conductor.ensureInstalled();
	const started = new Deferred<void>();
	const aborted = new Deferred<void>();
	let stopping = false;
	const task = conductor.createTask(
		{ name: "batch", batch: { size: 2, timeoutMs: 50 } },
		{ invocable: true },
		async (_events, ctx) => {
			started.resolve();
			await new Promise((r) => ctx.signal.addEventListener("abort", r));
			if (!stopping) aborted.resolve();
		},
	);
	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [task],
		defaultWorker: { concurrency: 1, pollIntervalMs: 20, flushIntervalMs: 10 },
	});
	const cancelled = await conductor.invoke(definition, { n: 0 });
	const other = await conductor.invoke(definition, { n: 1 });
	await orchestrator.start();
	try {
		await started.promise;
		expect(await conductor.db.cancelExecution(cancelled)).toBe(true);
		expect(await within(aborted.promise, 5000)).toBe("done");

		await waitForCondition(async () => (await row(db, cancelled))?.failed === true);
		// The other member runs again without being charged the interrupted attempt
		await waitForCondition(async () => (await row(db, other))?.locked === true);
		expect(await row(db, other)).toMatchObject({ failed: false, attempts: 1, last_error: null });
	} finally {
		stopping = true;
		await orchestrator.stop();
	}
});

describe("drain()", () => {
	async function setup(
		handler: (ctx: {
			signal: AbortSignal;
			onSignal: (name: string, fn: () => void) => void;
		}) => Promise<void>,
		concurrency: number,
	) {
		const db = await child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([sessionDefinition]),
			context: {},
		});
		await conductor.ensureInstalled();
		const started = new Deferred<void>();
		const task = conductor.createTask(sessionDefinition, { invocable: true }, async (_e, ctx) => {
			started.resolve();
			await handler(ctx);
		});
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { concurrency, pollIntervalMs: 20, flushIntervalMs: 10 },
		});
		const id = await conductor.invoke({ name: "session" }, {});
		return { db, conductor, orchestrator, started, id };
	}

	test("delivers a signal to a running execution with spare concurrency", async () => {
		const { conductor, orchestrator, started, id } = await setup(async (ctx) => {
			const got = new Deferred<void>();
			ctx.onSignal("go", () => got.resolve());
			await got.promise;
		}, 2);
		const drained = orchestrator.drain();
		await started.promise;
		// The next claim finds no other work
		await sleep(200);
		expect(await conductor.signal(id, { name: "go", id: "g1" })).toBe(true);
		expect(await within(drained, 5000)).toBe("done");
	});

	test("delivers cancellation to a running execution with spare concurrency", async () => {
		const { conductor, orchestrator, started, id } = await setup(
			(ctx) => new Promise((r) => ctx.signal.addEventListener("abort", () => r())),
			2,
		);
		const drained = orchestrator.drain();
		await started.promise;
		await sleep(200);
		expect(await conductor.db.cancelExecution(id)).toBe(true);
		expect(await within(drained, 5000)).toBe("done");
	});

	test("is interrupted by stop()", async () => {
		const { orchestrator, started } = await setup(
			(ctx) => new Promise((r) => ctx.signal.addEventListener("abort", () => r())),
			1,
		);
		const drained = orchestrator.drain();
		await started.promise;
		expect(await within(orchestrator.stop(), 5000)).toBe("done");
		expect(await within(drained, 1000)).toBe("done");
	});
});

describe("stop() while a handler runs a step", () => {
	const ackStatement = "set delivered_at = pgconductor._private_current_time()";

	// `ack` replaces the response of every signal acknowledgement
	async function setup({
		signal,
		ack = (query) => query,
		onInput,
	}: {
		signal: boolean;
		ack?: (query: Promise<unknown>) => Promise<unknown>;
		onInput?: (signal: AbortSignal) => Promise<void>;
	}) {
		const db = await child();
		const conductor = Conductor.create({
			sql: new Proxy(db.sql, {
				apply(target, thisArg, args) {
					const query = Reflect.apply(target, thisArg, args);
					return String(args[0]).includes(ackStatement) ? ack(query) : query;
				},
			}),
			tasks: TaskSchemas.fromSchema([sessionDefinition]),
			context: {},
		});
		const received = new Deferred<void>();
		const stepRunning = new Deferred<void>();
		const gate = new Deferred<void>();
		let finished = false;
		const task = conductor.createTask(sessionDefinition, { invocable: true }, async (_e, ctx) => {
			ctx.onSignal("input", async () => {
				received.resolve();
				await onInput?.(ctx.signal);
			});
			if (signal) await received.promise;
			await ctx.step("work", async () => {
				stepRunning.resolve();
				await gate.promise;
			});
			finished = true;
		});
		await conductor.ensureInstalled();
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 50, flushIntervalMs: 50 },
		});
		const id = await conductor.invoke({ name: "session" }, {});
		if (signal) await conductor.signal(id, { name: "input", id: "s1" });
		await orchestrator.start();
		await stepRunning.promise;
		const delivered = async () => {
			const [s] = await db.sql<{ delivered: boolean }[]>`
				select delivered_at is not null as delivered from pgconductor._private_execution_signals
			`;
			return s?.delivered;
		};
		return { db, id, orchestrator, gate, delivered, finished: () => finished };
	}

	test("waits for the handler when no signal is involved", async () => {
		const { db, id, orchestrator, gate, finished } = await setup({ signal: false });
		try {
			const stopped = orchestrator.stop();
			expect(await within(stopped, 300)).toBe("timeout");
			gate.resolve();
			await stopped;
			expect(finished()).toBe(true);
			expect(await row(db, id)).toMatchObject({ completed: true, failed: false });
		} finally {
			gate.resolve();
			await orchestrator.stop();
		}
	});

	test("completes once a slow acknowledgement returns", async () => {
		const release = new Deferred<void>();
		const { db, id, orchestrator, gate, delivered } = await setup({
			signal: true,
			ack: (query) => query.then(async (rows) => (await release.promise, rows)),
		});
		try {
			gate.resolve();
			await sleep(200);
			expect(await row(db, id)).toMatchObject({ completed: false, locked: true });
			release.resolve();
			await waitForCondition(async () => (await row(db, id))?.completed === true);
			expect(await delivered()).toBe(true);
		} finally {
			release.resolve();
			gate.resolve();
			await orchestrator.stop();
		}
	});

	test("waits for the handler while an acknowledgement is in flight", async () => {
		const release = new Deferred<void>();
		const { db, id, orchestrator, gate, delivered, finished } = await setup({
			signal: true,
			ack: (query) => query.then(async (rows) => (await release.promise, rows)),
		});
		try {
			const stopped = orchestrator.stop();
			expect(await within(stopped, 300)).toBe("timeout");
			release.resolve();
			expect(await within(stopped, 300)).toBe("timeout");
			gate.resolve();
			await stopped;
			expect(finished()).toBe(true);
			expect(await row(db, id)).toMatchObject({ completed: true, failed: false });
			expect(await delivered()).toBe(true);
		} finally {
			release.resolve();
			gate.resolve();
			await orchestrator.stop();
		}
	});

	test.each([
		["stop() ends the acknowledgement's retries", { ack: () => Promise.reject(connectionError()) }],
		[
			"the signal handler stops on ctx.signal",
			{
				onInput: (signal: AbortSignal) =>
					new Promise<void>((_, reject) =>
						signal.addEventListener("abort", () => reject(new Error("stopped"))),
					),
			},
		],
	])("waits for the handler and releases it when %s", async (_name, options) => {
		const { db, id, orchestrator, gate, delivered, finished } = await setup({
			signal: true,
			...options,
		});
		try {
			const stopped = orchestrator.stop();
			expect(await within(stopped, 300)).toBe("timeout");
			gate.resolve();
			await stopped;
			expect(finished()).toBe(true);
			expect(await row(db, id)).toMatchObject({
				completed: false,
				failed: false,
				locked: false,
				attempts: 0,
				last_error: null,
			});
			expect(await delivered()).toBe(false);
		} finally {
			gate.resolve();
			await orchestrator.stop();
		}
	});
});

test("stop() interrupts a worker registration that keeps retrying", async () => {
	const db = await child();
	let attempts = 0;
	const conductor = Conductor.create({
		sql: failStatements(db.sql, "pgconductor._private_register_worker(", () => {
			attempts++;
			return connectionError("ECONNREFUSED");
		}),
		tasks: TaskSchemas.fromSchema([sessionDefinition]),
		context: {},
	});
	await conductor.ensureInstalled();
	const task = conductor.createTask(sessionDefinition, { invocable: true }, async () => {});
	const orchestrator = Orchestrator.create({ conductor, tasks: [task] });
	const started = orchestrator.start();
	started.catch(() => {});
	await waitForCondition(() => attempts >= 1);
	expect(await within(orchestrator.stop(), 5000)).toBe("done");
	await expect(started).rejects.toThrow();
});

describe("flush", () => {
	const definition = defineTask({ name: "flush", payload: z.object({ n: z.number() }) });

	async function setup(
		sql: (sql: Sql) => Sql,
		worker: { concurrency: number; flushIntervalMs: number },
	) {
		const db = await child();
		const conductor = Conductor.create({
			sql: sql(db.sql),
			tasks: TaskSchemas.fromSchema([definition]),
			context: {},
		});
		await conductor.ensureInstalled();
		const gates = [new Deferred<void>(), new Deferred<void>()];
		const finished = [new Deferred<void>(), new Deferred<void>()];
		let runs = 0;
		const task = conductor.createTask(definition, { invocable: true }, async ({ payload }) => {
			runs++;
			await gates[payload.n]?.promise;
			finished[payload.n]?.resolve();
		});
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { ...worker, pollIntervalMs: 20, flushBatchSize: 100, fetchBatchSize: 2 },
		});
		return { db, conductor, task, orchestrator, gates, finished, runs: () => runs };
	}

	test("stop() waits for an earlier flush still in flight", async () => {
		const t = await setup((sql) => sql, { concurrency: 2, flushIntervalMs: 10 });
		const flushA = new Deferred<void>();
		let flushes = 0;
		let flushADone = false;
		const returnExecutions = t.conductor.db.returnExecutions.bind(t.conductor.db);
		t.conductor.db.returnExecutions = async (args, opts) => {
			if (++flushes === 1) {
				await flushA.promise;
				await returnExecutions(args, opts);
				flushADone = true;
				return;
			}
			await returnExecutions(args, opts);
		};
		// Cleanup releases flush A, unless stop() waited for it
		let flushADoneAtCleanup: boolean | null = null;
		const orchestratorShutdown = t.conductor.db.orchestratorShutdown.bind(t.conductor.db);
		t.conductor.db.orchestratorShutdown = async (args, opts) => {
			flushADoneAtCleanup = flushADone;
			await orchestratorShutdown(args, opts);
			flushA.resolve();
		};

		await t.orchestrator.start();
		const first = await t.conductor.invoke(definition, { n: 0 });
		const second = await t.conductor.invoke(definition, { n: 1 });
		t.gates[0]?.resolve();
		await waitForCondition(() => flushes === 1);
		t.gates[1]?.resolve();
		await t.finished[1]?.promise;
		// A timer flush of the second result starts while the first is in flight
		await sleep(100);

		const stopped = t.orchestrator.stop();
		setTimeout(() => flushA.resolve(), 500);
		await stopped;

		expect({
			flushADoneAtCleanup,
			first: (await row(t.db, first))?.completed,
			second: (await row(t.db, second))?.completed,
		}).toEqual({ flushADoneAtCleanup: true, first: true, second: true });
	});

	test("a completed result survives a transient failure of the final flush", async () => {
		let failed = false;
		const t = await setup(
			(sql) =>
				failStatements(sql, "from (select count(*) as writes from writes) w", () => {
					if (failed) return null;
					failed = true;
					return connectionError();
				}),
			{ concurrency: 1, flushIntervalMs: 60_000 },
		);
		t.gates[0]?.resolve();
		await t.orchestrator.start();
		const id = await t.conductor.invoke(definition, { n: 0 });
		await t.finished[0]?.promise;
		await t.orchestrator.stop();
		const completedAfterStop = (await row(t.db, id))?.completed;

		// A later orchestrator does not run the completed execution again
		await Orchestrator.create({ conductor: t.conductor, tasks: [t.task] }).drain();

		expect({ failed, completedAfterStop, runs: t.runs() }).toEqual({
			failed: true,
			completedAfterStop: true,
			runs: 1,
		});
	});
});

describe("orchestrator cleanup", () => {
	async function setup(fail: () => Error | null) {
		const db = await child();
		let attempts = 0;
		const conductor = Conductor.create({
			sql: failStatements(db.sql, "_private_remove_orchestrators(p_id", () => {
				attempts++;
				return fail();
			}),
			context: {},
		});
		await conductor.ensureInstalled();
		return { orchestrator: Orchestrator.create({ conductor }), attempts: () => attempts };
	}

	test("stop() finishes when the database stays unreachable", async () => {
		const { orchestrator, attempts } = await setup(() => connectionError("ECONNREFUSED"));
		await orchestrator.start();
		expect(await within(orchestrator.stop(), 20_000)).toBe("done");
		expect(attempts()).toBeGreaterThan(1);
	});

	test("a failed cleanup resets the lifecycle and removes process listeners", async () => {
		const baseline = process.listenerCount("SIGTERM");
		let failed = false;
		const { orchestrator } = await setup(() => {
			if (failed) return null;
			failed = true;
			return Object.assign(new Error("permission denied"), { code: "42501" });
		});
		await orchestrator.start();
		expect(process.listenerCount("SIGTERM")).toBe(baseline + 1);
		await orchestrator.stop();

		expect({
			isRunning: orchestrator.info.isRunning,
			listeners: process.listenerCount("SIGTERM"),
		}).toEqual({ isRunning: false, listeners: baseline });
		await orchestrator.start();
		await orchestrator.stop();
	});
});
