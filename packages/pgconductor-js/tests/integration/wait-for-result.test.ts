import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import postgres from "postgres";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineEvent } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import { Deferred } from "../../src/lib/deferred";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const click = defineEvent({
	name: "result.click",
	payload: z.object({ id: z.string() }),
	filterable: ["id"],
});
const waiterDefinition = defineTask({ name: "waiter", payload: z.object({ id: z.string() }) });
const workDefinition = defineTask({
	name: "work",
	payload: z.object({ value: z.number() }),
	returns: z.object({ doubled: z.number() }),
});
const leafDefinition = defineTask({ name: "leaf" });

type Handler = (id: string, ctx: any) => Promise<void>;

describe("ctx.waitForResult and execution branches (Postgres integration)", () => {
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
			waiter,
			work = async () => {},
			workOptions = {},
			orchestratorCount = 1,
		}: {
			waiter: Handler;
			work?: (value: number, ctx: any) => Promise<unknown>;
			workOptions?: { maxAttempts?: number; removeOnComplete?: { days: number } };
			orchestratorCount?: number;
		},
	) {
		const conductor = Conductor.create({
			sql: database.sql,
			tasks: TaskSchemas.fromSchema([waiterDefinition, workDefinition, leafDefinition]),
			events: [click],
			context: {},
		});
		const tasks = [
			conductor.createTask({ name: "waiter" }, { invocable: true }, async (event, ctx) =>
				waiter(event.payload.id, ctx),
			),
			conductor.createTask(
				{ name: "work", ...workOptions },
				{ invocable: true },
				async (event, ctx) => {
					await work(event.payload.value, ctx);
					return { doubled: event.payload.value * 2 };
				},
			),
			conductor.createTask({ name: "leaf", maxAttempts: 1 }, { invocable: true }, async () => {
				throw new Error("leaf failed");
			}),
		];
		for (let i = 0; i < orchestratorCount; i++) {
			const orchestrator = Orchestrator.create({
				conductor,
				tasks,
				defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10, concurrency: 4 },
			});
			orchestrators.push(orchestrator);
			await orchestrator.start();
		}
		return conductor;
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

	async function completed(database: TestDatabase, executionId: string) {
		await waitForCondition(async () => {
			const [row] = await database.sql`
				select 1 from pgconductor._private_executions
				where id = ${executionId}::uuid and completed_at is not null
			`;
			return row !== undefined;
		});
	}

	async function resultWaiters(database: TestDatabase, executionId: string) {
		const [row] = await database.sql<{ result_waiters: unknown }[]>`
			select result_waiters from pgconductor._private_executions where id = ${executionId}::uuid
		`;
		return row?.result_waiters;
	}

	async function stopOrchestrators() {
		await Promise.all(orchestrators.map((orchestrator) => orchestrator.stop()));
	}

	// Suspends a waiter at ctx.waitForResult("result", awaitedId) without a worker
	async function suspendWaiter(
		database: TestDatabase,
		conductor: Awaited<ReturnType<typeof setup>>,
		awaitedId: string,
	) {
		const waiterId = await conductor.invoke({ name: "waiter" }, { id: awaitedId });
		const orchestratorId = await database.registerOrchestrator();
		const { executions } = await database.client.getExecutions({
			orchestratorId,
			queueName: "default",
			batchSize: 10,
			taskKeys: ["waiter"],
		});
		const claimToken = executions.find((execution) => execution.id === waiterId)?.claim_token;
		await database.sql`
			select pgconductor._private_register_event_race(
				${waiterId}::uuid, 'default', 'waiter', ${orchestratorId}::uuid, ${claimToken || null}::uuid,
				'result',
				${database.sql.json([{ key: "result", stepKey: "result:result", executionId: awaitedId }])}::jsonb,
				null
			)
		`;
		await suspendedAt(database, waiterId, "result");
		return waiterId;
	}

	async function woken(database: TestDatabase, waiterId: string) {
		const [row] = await database.sql<{ result: unknown; due: boolean }[]>`
			select s.result, e.waiting_step_key is null
				and e.run_at <= pgconductor._private_current_time() as due
			from pgconductor._private_executions e
			left join pgconductor._private_steps s on s.execution_id = e.id and s.key = 'result:result'
			where e.id = ${waiterId}::uuid
		`;
		return row;
	}

	async function waits(database: TestDatabase) {
		const [row] = await database.sql<{ n: number }[]>`
			select count(*)::int as n from pgconductor._private_custom_event_subscriptions
			where kind = 'execution_wait'
		`;
		return row?.n;
	}

	test("a completed execution wakes its waiter, and replays return the memoized outcome", async () => {
		const database = await db();
		const release = new Deferred<void>();
		const outcomes: unknown[] = [];
		const ids: string[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				const id = await ctx.invoke("start", { name: "work" }, { value: 2 });
				ids.push(id);
				outcomes.push(await ctx.waitForResult("result", id));
				await ctx.sleep("pause", 0);
			},
			work: () => release.promise,
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "completed" });
		await suspendedAt(database, waiterId, "result");
		expect(outcomes).toEqual([]);
		release.resolve();
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });

		const outcome = { status: "completed", result: { doubled: 4 } };
		expect(outcomes).toEqual([outcome, outcome]);
		expect(await step(database, waiterId, "result")).toEqual({ key: "result", outcome });
		expect(await waits(database)).toBe(0);
	});

	test("an execution that fails after its retries resolves the wait with the failure", async () => {
		const database = await db();
		const outcomes: unknown[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				const id = await ctx.invoke("start", { name: "work" }, { value: 1 });
				outcomes.push(await ctx.waitForResult("result", id));
			},
			work: async () => {
				throw new Error("boom");
			},
			workOptions: { maxAttempts: 2 },
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "failed" });
		await suspendedAt(database, waiterId, "result");
		await waitForCondition(async () => {
			const rows = await database.sql`
				update pgconductor._private_executions
				set run_at = pgconductor._private_current_time()
				where task_key = 'work' and attempts = 1 and last_error is not null and locked_by is null
				returning id
			`;
			return rows.length === 1;
		});
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });

		expect(outcomes).toEqual([{ status: "failed", error: "boom" }]);
	});

	test("a failure cascading from a child of the awaited execution resolves the wait", async () => {
		const database = await db();
		const outcomes: unknown[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				const id = await ctx.invoke("start", { name: "work" }, { value: 1 });
				outcomes.push(await ctx.waitForResult("result", id));
			},
			work: (_value, ctx) => ctx.invokeAndWait("leaf", { name: "leaf" }),
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "cascade" });
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });

		expect(outcomes).toEqual([{ status: "failed", error: "Child execution failed: leaf failed" }]);
	});

	test("a wait registered while the awaited execution settles is not missed", async () => {
		const database = await db();
		const conductor = await setup(database, { waiter: async () => {} });
		await Promise.all(orchestrators.map((orchestrator) => orchestrator.stop()));
		const workId = await conductor.invoke({ name: "work" }, { value: 1 });
		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "race" });
		const orchestratorId = await database.registerOrchestrator();
		const { executions } = await database.client.getExecutions({
			orchestratorId,
			queueName: "default",
			batchSize: 10,
			taskKeys: ["work", "waiter"],
		});
		const claimToken = (id: string) =>
			executions.find((execution) => execution.id === id)?.claim_token || "";
		const registration = postgres(database.url, { max: 1 });
		const observer = postgres(database.url, { max: 1 });
		const registered = new Deferred<void>();
		const commit = new Deferred<void>();

		try {
			// The registration holds the awaited row until the settlement statement has taken its snapshot
			const registering = registration.begin(async (sql) => {
				await sql`
					select pgconductor._private_register_event_race(
						${waiterId}::uuid, 'default', 'waiter', ${orchestratorId}::uuid,
						${claimToken(waiterId)}::uuid, 'result',
						${sql.json([{ key: "result", stepKey: "result:result", executionId: workId }])}::jsonb,
						null
					)
				`;
				registered.resolve();
				await commit.promise;
			});
			await registered.promise;
			const settling = database.client.returnExecutions({
				orchestratorId,
				results: [
					{
						execution_id: workId,
						queue: "default",
						task_key: "work",
						claim_token: claimToken(workId),
						status: "completed",
						result: { doubled: 2 },
					},
				],
			});
			await waitForCondition(async () => {
				const rows = await observer`
					select 1 from pg_stat_activity
					where datname = current_database() and wait_event_type = 'Lock'
				`;
				return rows.length === 1;
			});
			commit.resolve();
			await registering;
			await settling;
		} finally {
			await registration.end();
			await observer.end();
		}

		expect(await step(database, waiterId, "result:result")).toEqual({
			status: "resolved",
			outcome: { status: "completed", result: { doubled: 2 } },
		});
		const [waiter] = await database.sql<{ waiting_step_key: string | null; due: boolean }[]>`
			select waiting_step_key, run_at <= pgconductor._private_current_time() as due
			from pgconductor._private_executions where id = ${waiterId}::uuid
		`;
		expect(waiter).toEqual({ waiting_step_key: null, due: true });
	});

	test("a waiter removed in the settlement that settles the awaited execution", async () => {
		const database = await db();
		const conductor = await setup(database, {
			waiter: async () => {},
			workOptions: { removeOnComplete: { days: 0 } },
		});
		await Promise.all(orchestrators.map((orchestrator) => orchestrator.stop()));
		const awaitedId = await conductor.invoke({ name: "waiter" }, { id: "awaited" });
		const waiterId = await conductor.invoke({ name: "work" }, { value: 1 });
		const orchestratorId = await database.registerOrchestrator();
		const { executions } = await database.client.getExecutions({
			orchestratorId,
			queueName: "default",
			batchSize: 10,
			taskKeys: ["work", "waiter"],
		});
		const claimToken = (id: string) =>
			executions.find((execution) => execution.id === id)?.claim_token || "";
		await database.sql`
			update pgconductor._private_executions
			set result_waiters = ${database.sql.json([
				{ execution_id: waiterId, queue: "default", step_key: "result" },
			])}::jsonb
			where id = ${awaitedId}::uuid
		`;

		await database.client.returnExecutions({
			orchestratorId,
			results: [
				{
					execution_id: waiterId,
					queue: "default",
					task_key: "work",
					claim_token: claimToken(waiterId),
					status: "completed",
					result: { doubled: 2 },
				},
				{
					execution_id: awaitedId,
					queue: "default",
					task_key: "waiter",
					claim_token: claimToken(awaitedId),
					status: "completed",
				},
			],
		});

		expect(await conductor.getExecution(waiterId)).toBeNull();
		expect((await conductor.getExecution(awaitedId))?.status).toBe("completed");
		const steps = await database.sql`select 1 from pgconductor._private_steps`;
		expect(steps.length).toBe(0);
	});

	test("an execution cancelled while pending resolves the wait as cancelled", async () => {
		const database = await db();
		const outcomes: unknown[] = [];
		const ids: string[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				const id = await ctx.invoke(
					"start",
					{ name: "work" },
					{ value: 1 },
					{ run_at: new Date(Date.now() + 3_600_000) },
				);
				ids.push(id);
				outcomes.push(await ctx.waitForResult("result", id));
			},
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "pending" });
		await suspendedAt(database, waiterId, "result");
		expect(await conductor.cancel(ids[0] || "", { reason: "no longer needed" })).toBe(true);
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });

		expect(outcomes).toEqual([{ status: "cancelled", reason: "no longer needed" }]);
	});

	test("an execution cancelled while running resolves the wait as cancelled", async () => {
		const database = await db();
		const started = new Deferred<void>();
		const outcomes: unknown[] = [];
		const ids: string[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				const id = await ctx.invoke("start", { name: "work" }, { value: 1 });
				ids.push(id);
				outcomes.push(await ctx.waitForResult("result", id));
			},
			work: async (_value, ctx) => {
				started.resolve();
				await new Promise((resolve) => ctx.signal.addEventListener("abort", resolve));
			},
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "running" });
		await started.promise;
		await suspendedAt(database, waiterId, "result");
		expect(await conductor.cancel(ids[0] || "", { reason: "stop" })).toBe(true);
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });

		expect(outcomes).toEqual([{ status: "cancelled", reason: "stop" }]);
	});

	test("an execution settled before the wait returns at once", async () => {
		const database = await db();
		let entered = 0;
		const outcomes: unknown[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				entered++;
				const id = await ctx.invoke("start", { name: "work" }, { value: 3 });
				await ctx.step("await-work", () => completed(database, id));
				outcomes.push(await ctx.waitForResult("result", id));
			},
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "settled" });
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });

		expect(entered).toBe(1);
		expect(outcomes).toEqual([{ status: "completed", result: { doubled: 6 } }]);
		expect(await waits(database)).toBe(0);
	});

	test("an execution removed when it completes still resolves the wait", async () => {
		const database = await db();
		const release = new Deferred<void>();
		const outcomes: unknown[] = [];
		const ids: string[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				const id = await ctx.invoke("start", { name: "work" }, { value: 5 });
				ids.push(id);
				outcomes.push(await ctx.waitForResult("result", id));
			},
			work: () => release.promise,
			workOptions: { removeOnComplete: { days: 0 } },
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "removed" });
		await suspendedAt(database, waiterId, "result");
		release.resolve();
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });

		expect(outcomes).toEqual([{ status: "completed", result: { doubled: 10 } }]);
		expect(await conductor.getExecution(ids[0] || "")).toBeNull();
	});

	test("an unknown execution fails the wait", async () => {
		const database = await db();
		const errors: string[] = [];
		const unknown = crypto.randomUUID();
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				await ctx.waitForResult("result", unknown).catch((error: Error) => {
					errors.push(error.message);
				});
			},
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "unknown" });
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });

		expect(errors).toEqual([`Execution ${unknown} not found`]);
	});

	test("a timeout resolves the wait and leaves the execution running", async () => {
		const database = await db();
		const release = new Deferred<void>();
		const outcomes: unknown[] = [];
		const ids: string[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				const id = await ctx.invoke("start", { name: "work" }, { value: 1 });
				ids.push(id);
				outcomes.push(await ctx.waitForResult("result", id, { timeout: "1s" }));
			},
			work: () => release.promise,
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "timeout" });
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });
		const workId = ids[0] || "";

		expect(outcomes).toEqual([{ status: "timeout" }]);
		expect((await conductor.getExecution(workId))?.status).toBe("running");
		expect(await resultWaiters(database, workId)).toBeNull();
		release.resolve();
		expect(await conductor.waitForResult(workId, { pollIntervalMs: 10 })).toEqual({ doubled: 2 });
		expect(await step(database, waiterId, "result")).toEqual({ key: "timeout" });
	});

	test("waitForAny resolves with an execution branch that settles first", async () => {
		const database = await db();
		const release = new Deferred<void>();
		const winners: unknown[] = [];
		const handles: string[] = [];
		const ids: string[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				const id = await ctx.invoke("start", { name: "work" }, { value: 4 });
				ids.push(id);
				const clicked = await ctx.subscribe("click", { event: click, filter: { id: [id] } });
				winners.push(await ctx.waitForAny("any", { work: { execution: id }, click: clicked }));
				handles.push(
					await clicked.wait().then(
						() => "resolved",
						(error: Error) => error.message,
					),
				);
			},
			work: () => release.promise,
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "execution-wins" });
		await suspendedAt(database, waiterId, "any");
		release.resolve();
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });
		await conductor.emit("result.click", { id: ids[0] || "" });

		const winner = { key: "work", outcome: { status: "completed", result: { doubled: 8 } } };
		expect(winners).toEqual([winner]);
		expect(await step(database, waiterId, "any")).toEqual(winner);
		expect(handles).toEqual(['Event subscription "click" was closed by ctx.waitForAny()']);
		expect(await waits(database)).toBe(0);
	});

	test("waitForAny keeps an event winner when the execution branch settles later", async () => {
		const database = await db();
		const release = new Deferred<void>();
		const winners: unknown[] = [];
		const ids: string[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				const id = await ctx.invoke("start", { name: "work" }, { value: 4 });
				ids.push(id);
				const clicked = await ctx.subscribe("click", { event: click, filter: { id: [id] } });
				winners.push(await ctx.waitForAny("any", { work: { execution: id }, click: clicked }));
			},
			work: () => release.promise,
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "event-wins" });
		await suspendedAt(database, waiterId, "any");
		const workId = ids[0] || "";
		await conductor.emit("result.click", { id: workId });
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });
		expect(await resultWaiters(database, workId)).toBeNull();
		release.resolve();
		await conductor.waitForResult(workId, { pollIntervalMs: 10 });

		const winner = { key: "click", event: { name: "result.click", payload: { id: workId } } };
		expect(winners).toEqual([winner]);
		expect(await step(database, waiterId, "any")).toEqual(winner);
		expect(await step(database, waiterId, "any:work")).toEqual({ status: "closed" });
	});

	test("waitForAny picks one winner when an execution and an event settle at once", async () => {
		const database = await db();
		const release = new Deferred<void>();
		const winners: Record<string, unknown[]> = {};
		const works = new Map<string, string>();
		const conductor = await setup(database, {
			waiter: async (id, ctx) => {
				const workId = await ctx.invoke("start", { name: "work" }, { value: 1 });
				works.set(id, workId);
				const clicked = await ctx.subscribe("click", { event: click, filter: { id: [workId] } });
				const winner = await ctx.waitForAny("any", { work: { execution: workId }, click: clicked });
				winners[id] = [...(winners[id] || []), winner];
			},
			work: () => release.promise,
			orchestratorCount: 2,
		});

		const ids = Array.from({ length: 10 }, (_, index) => String(index));
		const waiters = new Map<string, string>();
		for (const id of ids) {
			waiters.set(id, await conductor.invoke({ name: "waiter" }, { id }));
		}
		for (const [id, waiterId] of waiters) {
			await waitForCondition(() => works.has(id));
			await suspendedAt(database, waiterId, "any");
		}
		release.resolve();
		await Promise.all(ids.map((id) => conductor.emit("result.click", { id: works.get(id) || "" })));
		for (const waiterId of waiters.values()) {
			await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });
		}

		for (const [id, waiterId] of waiters) {
			expect(winners[id]).toEqual([await step(database, waiterId, "any")]);
		}
		expect(await waits(database)).toBe(0);
	});

	test("waitForAny returns a settled execution branch without suspending", async () => {
		const database = await db();
		let entered = 0;
		const winners: unknown[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				entered++;
				const id = await ctx.invoke("start", { name: "work" }, { value: 3 });
				const clicked = await ctx.subscribe("click", { event: click, filter: { id: [id] } });
				await ctx.step("await-work", () => completed(database, id));
				winners.push(
					await ctx.waitForAny(
						"any",
						{ work: { execution: id }, click: clicked },
						{ timeout: "1h" },
					),
				);
			},
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "settled-branch" });
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });

		expect(entered).toBe(1);
		expect(winners).toEqual([
			{ key: "work", outcome: { status: "completed", result: { doubled: 6 } } },
		]);
	});

	test("waitForAny over an execution branch times out and leaves the execution running", async () => {
		const database = await db();
		const release = new Deferred<void>();
		const winners: unknown[] = [];
		const ids: string[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				const id = await ctx.invoke("start", { name: "work" }, { value: 1 });
				ids.push(id);
				winners.push(await ctx.waitForAny("any", { work: { execution: id } }, { timeout: "1s" }));
			},
			work: () => release.promise,
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "any-timeout" });
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });
		const workId = ids[0] || "";

		expect(winners).toEqual([{ key: "timeout" }]);
		expect(await waits(database)).toBe(0);
		expect((await conductor.getExecution(workId))?.status).toBe("running");
		release.resolve();
		await conductor.waitForResult(workId, { pollIntervalMs: 10 });
		expect(await step(database, waiterId, "any:work")).toEqual({ status: "closed" });
	});

	test("a wait for a task finds only executions of that task", async () => {
		const database = await db();
		const errors = new Set<string>();
		const outcomes: unknown[] = [];
		const ids: string[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				const id = await ctx.invoke("start", { name: "work" }, { value: 2 });
				ids.push(id);
				await ctx.waitForResult("other", { name: "leaf" }, id).catch((error: Error) => {
					errors.add(error.message);
				});
				outcomes.push(await ctx.waitForResult("typed", { name: "work" }, id));
			},
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "typed" });
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });

		expect([...errors]).toEqual([`Execution ${ids[0]} not found`]);
		expect(outcomes).toEqual([{ status: "completed", result: { doubled: 4 } }]);
	});

	test("an execution superseded by a reinvoke resolves the wait as failed", async () => {
		const database = await db();
		const conductor = await setup(database, { waiter: async () => {} });
		await stopOrchestrators();
		const workId = await conductor.invoke({ name: "work" }, { value: 1 }, { dedupe_key: "same" });
		await database.client.getExecutions({
			orchestratorId: await database.registerOrchestrator(),
			queueName: "default",
			batchSize: 10,
			taskKeys: ["work"],
		});
		const waiterId = await suspendWaiter(database, conductor, workId);

		await conductor.invoke({ name: "work" }, { value: 2 }, { dedupe_key: "same" });

		expect(await woken(database, waiterId)).toEqual({
			result: {
				status: "resolved",
				outcome: { status: "failed", error: "superseded by reinvoke" },
			},
			due: true,
		});
	});

	test("a pending child failed when its parent's wait times out resolves the wait", async () => {
		const database = await db();
		const conductor = await setup(database, { waiter: async () => {} });
		await stopOrchestrators();
		const parentId = await conductor.invoke({ name: "work" }, { value: 1 });
		const childId = await conductor.invoke(
			{ name: "work" },
			{ value: 2 },
			{ run_at: new Date(Date.now() + 3_600_000) },
		);
		const orchestratorId = await database.registerOrchestrator();
		const { executions } = await database.client.getExecutions({
			orchestratorId,
			queueName: "default",
			batchSize: 10,
			taskKeys: ["work"],
		});
		await database.sql`
			update pgconductor._private_executions set waiting_on_execution_id = ${childId}::uuid
			where id = ${parentId}::uuid
		`;
		const waiterId = await suspendWaiter(database, conductor, childId);

		await database.client.clearWaitingState({
			executionId: parentId,
			queue: "default",
			orchestratorId,
			claimToken: executions.find((execution) => execution.id === parentId)?.claim_token || "",
		});

		expect(await woken(database, waiterId)).toEqual({
			result: {
				status: "resolved",
				outcome: { status: "failed", error: "Cancelled: parent timed out" },
			},
			due: true,
		});
	});

	test("removing pending schedule executions resolves their waits as cancelled", async () => {
		const database = await db();
		const conductor = await setup(database, { waiter: async () => {} });
		await stopOrchestrators();
		const schedule = async (dedupeKey: string) => {
			const [row] = await database.sql<{ id: string }[]>`
				insert into pgconductor._private_executions (task_key, queue, dedupe_key, cron_expression, run_at)
				values ('work', 'default', ${dedupeKey}, '0 0 * * *', now() + interval '1 hour')
				returning id
			`;
			return suspendWaiter(database, conductor, row?.id || "");
		};
		const unscheduled = await schedule("dynamic::unscheduled::1");
		const rescheduled = await schedule("dynamic::rescheduled::1");
		const removed = await schedule("scheduled::removed::1");

		await database.client.unscheduleCronExecution({
			taskKey: "work",
			queue: "default",
			scheduleName: "unscheduled",
		});
		await database.client.scheduleCronExecution({
			spec: {
				task_key: "work",
				queue: "default",
				run_at: new Date(Date.now() + 7_200_000),
				cron_expression: "0 1 * * *",
			},
			scheduleName: "rescheduled",
		});
		await database.client.registerWorker({
			queueName: "default",
			taskSpecs: [{ key: "waiter" }, { key: "work" }, { key: "leaf" }],
			cronSchedules: [],
			eventSubscriptions: [],
		});

		const cancelled = {
			result: {
				status: "resolved",
				outcome: { status: "cancelled", reason: "Schedule removed" },
			},
			due: true,
		};
		expect(await woken(database, unscheduled)).toEqual(cancelled);
		expect(await woken(database, rescheduled)).toEqual(cancelled);
		expect(await woken(database, removed)).toEqual(cancelled);
	});

	test("a linked child waiting on its parent is cancelled when the parent fails", async () => {
		const database = await db();
		const children: string[] = [];
		const outcomes: unknown[] = [];
		const conductor = await setup(database, {
			waiter: async (id, ctx) => {
				outcomes.push(await ctx.waitForResult("parent", id));
			},
			work: async (_value, ctx) => {
				const childId = await ctx.invoke(
					"child",
					{ name: "waiter" },
					{ id: ctx.executionId },
					{ cancelWithParent: true },
				);
				children.push(childId);
				await suspendedAt(database, childId, "parent");
				throw new Error("parent failed");
			},
			workOptions: { maxAttempts: 1 },
		});

		const parentId = await conductor.invoke({ name: "work" }, { value: 1 });
		await waitForCondition(
			async () => (await conductor.getExecution(parentId))?.status === "failed",
		);
		const child = await conductor.getExecution(children[0] || "");

		expect(outcomes).toEqual([]);
		expect({ status: child?.status, error: child?.error }).toEqual({
			status: "cancelled",
			error: "parent failed",
		});
	});

	test("a pending linked child cancelled with its parent resolves its waiters", async () => {
		const database = await db();
		const ids: string[] = [];
		const outcomes: unknown[] = [];
		const conductor = await setup(database, {
			waiter: async (id, ctx) => {
				outcomes.push(await ctx.waitForResult("result", id));
			},
			work: async (value, ctx) => {
				if (value === 2) return;
				const childId = await ctx.invoke(
					"child",
					{ name: "work" },
					{ value: 2 },
					{ cancelWithParent: true, run_at: new Date(Date.now() + 3_600_000) },
				);
				ids.push(await ctx.invoke("waiter", { name: "waiter" }, { id: childId }));
				await suspendedAt(database, ids[0] || "", "result");
				throw new Error("parent failed");
			},
			workOptions: { maxAttempts: 1 },
		});

		await conductor.invoke({ name: "work" }, { value: 1 });
		await waitForCondition(() => outcomes.length === 1);

		expect(outcomes).toEqual([{ status: "cancelled", reason: "parent failed" }]);
	});

	test("ctx.executionId and ctx.queue identify the execution", async () => {
		const database = await db();
		const seen: unknown[] = [];
		const conductor = await setup(database, {
			waiter: async (_id, ctx) => {
				seen.push({ executionId: ctx.executionId, queue: ctx.queue });
			},
		});

		const waiterId = await conductor.invoke({ name: "waiter" }, { id: "identity" });
		await conductor.waitForResult(waiterId, { pollIntervalMs: 10 });

		expect(seen).toEqual([{ executionId: waiterId, queue: "default" }]);
	});
});
