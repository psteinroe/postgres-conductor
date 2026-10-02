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
	payload: z.object({
		id: z.string(),
		kind: z.enum(["match", "other"]),
		amount: z.number().optional(),
		code: z.string().optional(),
	}),
	filterable: ["kind", "amount", "code"],
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

async function setup(
	db: TestDatabase,
	fn: Handler,
	orchestrators: Orchestrator[] = [],
	flushIntervalMs = 10,
) {
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
		defaultWorker: { pollIntervalMs: 10, flushIntervalMs },
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
		await until(async () => {
			const [row] = await database.sql<{ n: number }[]>`
				select count(*)::int as n
				from pgconductor._private_custom_event_subscriptions
				where kind = 'execution_wait'
			`;
			return row?.n === count;
		});
	}

	async function waitingAt(database: TestDatabase, stepKey: string) {
		await until(async () => {
			const [row] = await database.sql<{ present: boolean }[]>`
				select exists (
					select 1
					from pgconductor._private_custom_event_subscriptions
					where kind = 'execution_wait' and step_key = ${stepKey}
				) as present
			`;
			return row?.present === true;
		});
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
			if (!one) throw new Error("invoke did not return an execution id");
			const [step] = await database.sql<{ result: unknown }[]>`
				select result from pgconductor._private_steps
				where execution_id = ${one}::uuid and key = 'order:one'
			`;
			expect(step?.result).toEqual({
				status: "resolved",
				event: { name: "wait.order", payload: { id: "yes", kind: "match" } },
			});
		},
	);

	test.serial("uses the shared typed filter operators", async () => {
		const database = await db();
		const result: string[] = [];
		const { conductor } = await setup(
			database,
			async (id, ctx) => {
				const value = await ctx.waitForEvent(`operators:${id}`, {
					event,
					filter: {
						kind: [{ "anything-but": "other" }],
						code: [{ prefix: "ord-" }],
						amount: [{ numeric: [">=", 10, "<", 20] }],
					},
				});
				result.push(value.payload.id);
			},
			orchestrators,
		);
		await conductor.invoke({ name: "wait.task" }, { id: "operators" });
		await waiting(database);
		await conductor.emit("wait.order", {
			id: "outside",
			kind: "match",
			code: "ord-1",
			amount: 25,
		});
		await Bun.sleep(100);
		expect(result).toEqual([]);
		await conductor.emit("wait.order", {
			id: "inside",
			kind: "match",
			code: "ord-2",
			amount: 15,
		});
		await until(async () => result.length === 1);
		expect(result).toEqual(["inside"]);
	});

	test.serial("rejects events outside the runtime catalog", async () => {
		const database = await db();
		const errors: Error[] = [];
		const unknownEvent = defineEvent({
			name: "wait.unknown",
			payload: z.object({ id: z.string() }),
		});
		const { conductor } = await setup(
			database,
			async (_id, ctx) => {
				try {
					await ctx.waitForEvent("unknown", { event: unknownEvent });
				} catch (error) {
					errors.push(error as Error);
				}
			},
			orchestrators,
		);
		await conductor.invoke({ name: "wait.task" }, { id: "unknown" });
		await until(async () => errors.length === 1);
		expect(errors[0]?.message).toBe(
			'Event "wait.unknown" is not defined in the conductor event catalog',
		);
		await waiting(database, 0);
	});

	test.serial(
		"supports more waits than the retry budget",
		async () => {
			const database = await db();
			const received: string[] = [];
			const { conductor } = await setup(
				database,
				async (id, ctx) => {
					for (let index = 0; index < 5; index++) {
						const value = await ctx.waitForEvent(`many:${id}:${index}`, { event });
						received[index] = value.payload.id;
					}
				},
				orchestrators,
			);
			const executionId = await conductor.invoke({ name: "wait.task" }, { id: "many" });
			if (!executionId) throw new Error("invoke did not return an execution id");
			for (let index = 0; index < 5; index++) {
				await waitingAt(database, `many:many:${index}`);
				await conductor.emit("wait.order", { id: String(index), kind: "match" });
				await until(async () => received.length === index + 1);
			}
			await until(async () => {
				const [row] = await database.sql<{ completed: boolean }[]>`
				select completed_at is not null as completed
				from pgconductor._private_executions where id = ${executionId}::uuid
			`;
				return row?.completed === true;
			});
			expect(received).toEqual(["0", "1", "2", "3", "4"]);
		},
		60_000,
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

		const executionId = await conductor.invoke({ name: "wait.task" }, { id: "one" });
		if (!executionId) throw new Error("invoke did not return an execution id");
		await until(async () => errors.length === 1);
		expect(entered).toEqual(["one", "one"]);
		expect(errors).toHaveLength(1);
		expect(errors[0]).toBeInstanceOf(WaitForEventTimeoutError);
		const [step] = await database.sql<{ result: unknown }[]>`
			select result from pgconductor._private_steps
			where execution_id = ${executionId}::uuid and key = 'timeout:one'
		`;
		expect(step?.result).toEqual({ status: "timed_out" });
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

	test.serial(
		"events emitted before the worker flushes resolve the wait",
		async () => {
			const database = await db();
			const { conductor } = await setup(
				database,
				async (id, ctx) => {
					await ctx.waitForEvent(`unflushed:${id}`, { event });
				},
				orchestrators,
				1_000,
			);

			const executionId = await conductor.invoke({ name: "wait.task" }, { id: "race" });
			await waiting(database);
			await conductor.emit("wait.order", { id: "race", kind: "match" });
			await until(async () => {
				const [row] = await database.sql<{ completed: boolean }[]>`
				select completed_at is not null as completed
				from pgconductor._private_executions where id = ${executionId}::uuid
			`;
				return row?.completed === true;
			}, 5_000);
		},
		10_000,
	);

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
		const result = database.client.registerEventWait({
			executionId,
			queue: "default",
			taskKey: "wait.task",
			eventKey: "wait.order",
			stepKey: "stale:stale",
			requiredFieldCount: 0,
			terms: [],
			timeoutMs: null,
			orchestratorId: crypto.randomUUID(),
			suspend: true,
			claimToken: crypto.randomUUID(),
		});
		await expect(result).rejects.toThrow("is not claimed");
		const rows = await database.sql<{ n: number; waiting_step_key: string | null }[]>`
			select count(s.*)::int as n, max(e.waiting_step_key) as waiting_step_key
			from pgconductor._private_custom_event_subscriptions s
			right join pgconductor._private_executions e on e.id = ${executionId}::uuid
			where s.execution_id = ${executionId}::uuid
		`;
		expect(Number(rows[0]?.n)).toBe(0);
		expect(rows[0]?.waiting_step_key).toBeNull();
	});

	test.serial("preserves the original deadline when registration is retried", async () => {
		const database = await db();
		const { conductor, orchestrator } = await setup(database, async () => {}, orchestrators);
		await orchestrator.stop();
		const executionId = await conductor.invoke({ name: "wait.task" }, { id: "retry" });
		if (!executionId) throw new Error("invoke did not return an execution id");

		const orchestratorId = crypto.randomUUID();
		const [claimed] = await database.client.getExecutions({
			orchestratorId,
			queueName: "default",
			batchSize: 1,
			taskKeys: ["wait.task"],
		});
		if (!claimed) throw new Error("expected claim");
		const args = {
			executionId,
			queue: "default",
			taskKey: "wait.task",
			eventKey: event.name,
			stepKey: "retry:deadline",
			requiredFieldCount: 0,
			terms: [],
			timeoutMs: 5_000,
			orchestratorId,
			suspend: true,
			claimToken: claimed.claim_token,
		};
		const execution = async () => {
			const [row] = await database.sql<{ run_at: Date; locked_by: string | null }[]>`
				select run_at, locked_by from pgconductor._private_executions
				where id = ${executionId}::uuid
			`;
			return row;
		};

		expect(await database.client.registerEventWait(args)).toBeNull();
		const first = await execution();
		expect(first?.locked_by).toBeNull();

		await Bun.sleep(50);
		const claimToken = crypto.randomUUID();
		await database.sql`
			update pgconductor._private_executions
			set locked_by = ${orchestratorId}::uuid, claim_token = ${claimToken}::uuid,
				run_at = pgconductor._private_current_time()
			where id = ${executionId}::uuid
		`;
		expect(await database.client.registerEventWait({ ...args, claimToken })).toBeNull();
		const second = await execution();
		expect(second?.locked_by).toBeNull();
		expect(second?.run_at).toEqual(first?.run_at);
	});

	test.serial("does not let a replayed registration release a later claim", async () => {
		const database = await db();
		const { conductor, orchestrator } = await setup(database, async () => {}, orchestrators);
		await orchestrator.stop();
		const executionId = await conductor.invoke({ name: "wait.task" }, { id: "replayed" });
		if (!executionId) throw new Error("invoke did not return an execution id");

		const orchestratorId = crypto.randomUUID();
		const claim = async () => {
			const [execution] = await database.client.getExecutions({
				orchestratorId,
				queueName: "default",
				batchSize: 1,
				taskKeys: ["wait.task"],
			});
			if (!execution) throw new Error("expected claim");
			return execution;
		};
		const register = (claimToken: string) =>
			database.client.registerEventWait({
				executionId,
				queue: "default",
				taskKey: "wait.task",
				eventKey: event.name,
				stepKey: "replayed:wait",
				requiredFieldCount: 0,
				terms: [],
				timeoutMs: 1_000,
				orchestratorId,
				claimToken,
				suspend: true,
			});

		await database.client.setFakeTime({ date: new Date("2100-01-01T12:00:00Z") });
		const first = await claim();
		expect(await register(first.claim_token)).toBeNull();

		await database.client.setFakeTime({ date: new Date("2100-01-01T12:00:02Z") });
		const second = await claim();
		expect(await register(second.claim_token)).toEqual({ status: "timed_out" });

		// A retry of the first registration after a lost commit acknowledgement.
		await expect(register(first.claim_token)).rejects.toThrow("is not claimed");
		await database.client.clearFakeTime();

		const [row] = await database.sql<{ locked_by: string | null; subscriptions: number }[]>`
			select e.locked_by, (
				select count(*)::int from pgconductor._private_custom_event_subscriptions s
				where s.execution_id = e.id
			) as subscriptions
			from pgconductor._private_executions e
			where e.id = ${executionId}::uuid
		`;
		expect(row?.locked_by).toBe(orchestratorId);
		expect(row?.subscriptions).toBe(0);
	});

	test.serial("cleans abandoned waits when executions settle", async () => {
		const database = await db();
		const { conductor, orchestrator } = await setup(database, async () => {}, orchestrators);
		await orchestrator.stop();

		for (const status of ["completed", "permanently_failed"] as const) {
			const executionId = await conductor.invoke({ name: "wait.task" }, { id: status });
			if (!executionId) throw new Error("invoke did not return an execution id");

			const orchestratorId = crypto.randomUUID();
			const [execution] = await database.client.getExecutions({
				orchestratorId,
				queueName: "default",
				batchSize: 1,
				taskKeys: ["wait.task"],
			});
			if (!execution) throw new Error("expected claim");
			expect(execution.id).toBe(executionId);
			expect(
				await database.client.registerEventWait({
					executionId,
					queue: "default",
					taskKey: "wait.task",
					eventKey: event.name,
					stepKey: `terminal:${status}`,
					requiredFieldCount: 0,
					terms: [],
					timeoutMs: null,
					orchestratorId,
					suspend: true,
					claimToken: execution.claim_token,
				}),
			).toBeNull();
			await database.sql`
				update pgconductor._private_executions
				set locked_by = ${orchestratorId}::uuid
				where id = ${executionId}::uuid
			`;

			const result = {
				execution_id: executionId,
				claim_token: execution.claim_token,
				queue: "default",
				task_key: "wait.task",
			};
			await database.client.returnExecutions({
				orchestratorId,
				completed: status === "completed" ? [{ ...result, status }] : [],
				failed: status === "permanently_failed" ? [{ ...result, status, error: "failed" }] : [],
				released: [],
				invokeChild: [],
			});

			const [row] = await database.sql<{ n: number }[]>`
				select count(*)::int as n
				from pgconductor._private_custom_event_subscriptions
				where kind = 'execution_wait' and execution_id = ${executionId}::uuid
			`;
			expect(Number(row?.n ?? 0)).toBe(0);
		}
	});

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
		expect(Number(rows[0]?.attempts)).toBe(0);
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
					await database.sql`select count(*)::int as n from pgconductor._private_custom_event_subscriptions where kind = 'execution_wait'`
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
						await database.sql`select count(*)::int as n from pgconductor._private_custom_event_subscriptions where kind = 'execution_wait'`
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

	async function step(database: TestDatabase, executionId: string, key: string) {
		const [row] = await database.sql<{ result: unknown }[]>`
			select result from pgconductor._private_steps
			where execution_id = ${executionId}::uuid and key = ${key}
		`;
		return row?.result;
	}

	async function suspendedAt(database: TestDatabase, stepKey: string) {
		await until(async () => {
			const [row] = await database.sql<{ present: boolean }[]>`
				select exists (
					select 1 from pgconductor._private_executions
					where locked_by is null and waiting_step_key = ${stepKey}
				) as present
			`;
			return row?.present === true;
		});
	}

	test.serial(
		"subscribe buffers an event emitted before wait while the execution runs",
		async () => {
			const database = await db();
			const entered: string[] = [];
			const claimed: (string | null)[] = [];
			const result: string[] = [];
			const { conductor } = await setup(
				database,
				async (id, ctx) => {
					entered.push(id);
					const subscription = await ctx.subscribe(`approve:${id}`, {
						event,
						filter: { kind: ["match"] },
					});
					await ctx.step("post-card", async () => {
						await ctx.emit("wait.order", { id: "click", kind: "match" });
						await until(async () => {
							const [row] = await database.sql<{ locked_by: string | null }[]>`
							select e.locked_by
							from pgconductor._private_steps s
							join pgconductor._private_executions e on e.id = s.execution_id
							where s.key = ${`approve:${id}`}
						`;
							if (!row) return false;
							claimed.push(row.locked_by);
							return true;
						});
					});
					const value = await subscription.wait({ timeout: "1h" });
					result.push(value.payload.id);
				},
				orchestrators,
			);

			const executionId = await conductor.invoke({ name: "wait.task" }, { id: "buffered" });
			if (!executionId) throw new Error("invoke did not return an execution id");
			await until(async () => result.length === 1);
			expect(entered).toEqual(["buffered"]);
			expect(result).toEqual(["click"]);
			expect(claimed[0]).not.toBeNull();
			expect(await step(database, executionId, "approve:buffered")).toEqual({
				status: "resolved",
				event: { name: "wait.order", payload: { id: "click", kind: "match" } },
			});
			await waiting(database, 0);
		},
	);

	test.serial("subscribe ignores events emitted before it", async () => {
		const database = await db();
		const result: string[] = [];
		const { conductor } = await setup(
			database,
			async (id, ctx) => {
				await ctx.step("stale", () => ctx.emit("wait.order", { id: "stale", kind: "match" }));
				const subscription = await ctx.subscribe(`fresh:${id}`, { event });
				await ctx.step("post-card", () => ctx.emit("wait.order", { id: "fresh", kind: "match" }));
				const value = await subscription.wait();
				result.push(value.payload.id);
			},
			orchestrators,
		);

		await conductor.invoke({ name: "wait.task" }, { id: "boundary" });
		await until(async () => result.length === 1);
		expect(result).toEqual(["fresh"]);
	});

	test.serial("subscribe resolves a wait that already suspended", async () => {
		const database = await db();
		const entered: string[] = [];
		const result: string[] = [];
		const { conductor } = await setup(
			database,
			async (id, ctx) => {
				entered.push(id);
				const subscription = await ctx.subscribe(`later:${id}`, { event });
				const value = await subscription.wait();
				result.push(value.payload.id);
			},
			orchestrators,
		);

		await conductor.invoke({ name: "wait.task" }, { id: "later" });
		await suspendedAt(database, "later:later");
		expect(result).toEqual([]);
		await conductor.emit("wait.order", { id: "later", kind: "match" });
		await until(async () => result.length === 1);
		expect(entered).toEqual(["later", "later"]);
		expect(result).toEqual(["later"]);
		await waiting(database, 0);
	});

	test.serial("subscribe times out from the start of wait", async () => {
		const database = await db();
		const errors: unknown[] = [];
		const { conductor } = await setup(
			database,
			async (id, ctx) => {
				const subscription = await ctx.subscribe(`deadline:${id}`, { event });
				await ctx.step("slow", () => Bun.sleep(200));
				try {
					await subscription.wait({ timeout: "1s" });
				} catch (error) {
					errors.push(error);
				}
			},
			orchestrators,
		);

		const executionId = await conductor.invoke({ name: "wait.task" }, { id: "timeout" });
		if (!executionId) throw new Error("invoke did not return an execution id");
		await suspendedAt(database, "deadline:timeout");
		const [subscription] = await database.sql<{ timeout_ms: number }[]>`
			select extract(epoch from expires_at - created_at) * 1000 as timeout_ms
			from pgconductor._private_custom_event_subscriptions
			where execution_id = ${executionId}::uuid
		`;
		expect(Number(subscription?.timeout_ms)).toBeGreaterThanOrEqual(1200);
		await until(async () => errors.length === 1);
		expect(errors[0]).toBeInstanceOf(WaitForEventTimeoutError);
		expect(await step(database, executionId, "deadline:timeout")).toEqual({
			status: "timed_out",
		});
		await waiting(database, 0);
	});

	test.serial(
		"subscribe produces exactly one outcome when an event races the timeout",
		async () => {
			const database = await db();
			const outcomes: string[] = [];
			const { conductor } = await setup(
				database,
				async (id, ctx) => {
					const subscription = await ctx.subscribe(`race:${id}`, { event });
					try {
						const value = await subscription.wait({ timeout: "300ms" });
						outcomes.push(`${id}:match:${value.payload.id}`);
					} catch (error) {
						if (!(error instanceof WaitForEventTimeoutError)) throw error;
						outcomes.push(`${id}:timeout`);
					}
				},
				orchestrators,
			);

			const ids = Array.from({ length: 10 }, (_, index) => String(index));
			const executions = new Map<string, string>();
			for (const id of ids) {
				const executionId = await conductor.invoke({ name: "wait.task" }, { id });
				if (!executionId) throw new Error("invoke did not return an execution id");
				executions.set(id, executionId);
			}
			await waiting(database, ids.length);
			await Bun.sleep(250);
			await conductor.emit("wait.order", { id: "racer", kind: "match" });
			await until(async () => outcomes.length === ids.length);
			await Bun.sleep(100);
			const expected: string[] = [];
			for (const [id, executionId] of executions) {
				const result = await step(database, executionId, `race:${id}`);
				expected.push(
					(result as { status: string }).status === "timed_out"
						? `${id}:timeout`
						: `${id}:match:racer`,
				);
			}
			expect(outcomes.toSorted()).toEqual(expected.toSorted());
			await waiting(database, 0);
		},
	);

	test.serial("retries after subscribe keep one subscription and its buffered event", async () => {
		const database = await db();
		const attempts: number[] = [];
		const result: string[] = [];
		const { conductor } = await setup(
			database,
			async (id, ctx) => {
				const subscription = await ctx.subscribe(`retry:${id}`, { event });
				attempts.push(attempts.length + 1);
				if (attempts.length < 3) throw new Error("crash after subscribe");
				const value = await subscription.wait();
				result.push(value.payload.id);
			},
			orchestrators,
		);

		const executionId = await conductor.invoke({ name: "wait.task" }, { id: "crash" });
		if (!executionId) throw new Error("invoke did not return an execution id");
		const execution = async () => {
			const [row] = await database.sql<{ run_at: Date; retrying: boolean }[]>`
				select run_at, last_error is not null and locked_by is null as retrying
				from pgconductor._private_executions where id = ${executionId}::uuid
			`;
			return row;
		};
		const subscriptions = () => database.sql<{ id: string }[]>`
			select id from pgconductor._private_custom_event_subscriptions
			where execution_id = ${executionId}::uuid
		`;
		const retryNow = () => database.sql`
			update pgconductor._private_executions
			set run_at = pgconductor._private_current_time(), last_error = null
			where id = ${executionId}::uuid
		`;

		await until(async () => (await execution())?.retrying === true);
		const [first] = await subscriptions();
		await retryNow();
		await until(async () => attempts.length === 2 && (await execution())?.retrying === true);
		expect((await subscriptions()).map((row) => row.id)).toEqual([String(first?.id)]);

		const backoff = (await execution())?.run_at;
		await conductor.emit("wait.order", { id: "while-retrying", kind: "match" });
		await until(async () => (await step(database, executionId, "retry:crash")) !== undefined);
		expect((await execution())?.run_at).toEqual(backoff);

		await retryNow();
		await until(async () => result.length === 1);
		expect(attempts).toEqual([1, 2, 3]);
		expect(result).toEqual(["while-retrying"]);
		expect(await subscriptions()).toHaveLength(0);
	});

	test.serial("an event dispatched while a timed out waiter is claimed wins the wait", async () => {
		const database = await db();
		const { conductor, orchestrator } = await setup(database, async () => {}, orchestrators);
		await orchestrator.stop();
		const executionId = await conductor.invoke({ name: "wait.task" }, { id: "claimed" });
		if (!executionId) throw new Error("invoke did not return an execution id");

		const orchestratorId = crypto.randomUUID();
		const claim = (queueName: string, taskKey: string) =>
			database.client.getExecutions({
				orchestratorId,
				queueName,
				batchSize: 10,
				taskKeys: [taskKey],
			});
		const args = {
			executionId,
			queue: "default",
			taskKey: "wait.task",
			eventKey: event.name,
			stepKey: "claimed:timeout",
			requiredFieldCount: 0,
			terms: [],
			timeoutMs: 100,
			orchestratorId,
			suspend: true,
		};

		const [first] = await claim("default", "wait.task");
		if (!first) throw new Error("expected claim");
		expect(
			await database.client.registerEventWait({ ...args, claimToken: first.claim_token }),
		).toBeNull();
		await database.client.emitEvent({
			eventKey: event.name,
			payload: { id: "claimed", kind: "match" },
		});
		await Bun.sleep(150);
		const [second] = await claim("default", "wait.task");
		expect(second?.id).toBe(executionId);
		if (!second) throw new Error("expected claim");
		const events = await claim("pgconductor.internal", "pgconductor.event-dispatch");
		await database.client.dispatchCustomEvents({
			eventIds: events.map((execution) => execution.id),
			orchestratorId,
		});

		const [row] = await database.sql<{ locked_by: string | null }[]>`
			select locked_by from pgconductor._private_executions where id = ${executionId}::uuid
		`;
		expect(row?.locked_by).toBe(orchestratorId);
		expect(
			await database.client.registerEventWait({ ...args, claimToken: second.claim_token }),
		).toEqual({
			status: "resolved",
			event: { name: "wait.order", payload: { id: "claimed", kind: "match" } },
		});
		await waiting(database, 0);
	});

	test.serial(
		"a delayed event does not wake a timed out waiter that failed before waiting again",
		async () => {
			const database = await db();
			const { conductor, orchestrator } = await setup(database, async () => {}, orchestrators);
			await orchestrator.stop();
			const executionId = await conductor.invoke({ name: "wait.task" }, { id: "backoff" });
			if (!executionId) throw new Error("invoke did not return an execution id");

			const orchestratorId = crypto.randomUUID();
			const claim = (queueName: string, taskKey: string) =>
				database.client.getExecutions({
					orchestratorId,
					queueName,
					batchSize: 10,
					taskKeys: [taskKey],
				});
			const runAt = async () => {
				const [row] = await database.sql<{ run_at: Date }[]>`
				select run_at from pgconductor._private_executions where id = ${executionId}::uuid
			`;
				return row?.run_at;
			};

			const [first] = await claim("default", "wait.task");
			if (!first) throw new Error("expected claim");
			expect(
				await database.client.registerEventWait({
					executionId,
					queue: "default",
					taskKey: "wait.task",
					eventKey: event.name,
					stepKey: "backoff:wait",
					requiredFieldCount: 0,
					terms: [],
					timeoutMs: 100,
					orchestratorId,
					claimToken: first.claim_token,
					suspend: true,
				}),
			).toBeNull();
			await database.client.emitEvent({
				eventKey: event.name,
				payload: { id: "late", kind: "match" },
			});
			await Bun.sleep(150);
			const [second] = await claim("default", "wait.task");
			expect(second?.id).toBe(executionId);
			if (!second) throw new Error("expected claim");
			await database.client.returnExecutions({
				orchestratorId,
				completed: [],
				failed: [
					{
						execution_id: executionId,
						claim_token: second.claim_token,
						queue: "default",
						task_key: "wait.task",
						status: "failed",
						error: "failed before waiting again",
					},
				],
				released: [],
				invokeChild: [],
			});
			const backoff = await runAt();

			const events = await claim("pgconductor.internal", "pgconductor.event-dispatch");
			await database.client.dispatchCustomEvents({
				eventIds: events.map((execution) => execution.id),
				orchestratorId,
			});
			expect(await step(database, executionId, "backoff:wait")).toEqual({
				status: "resolved",
				event: { name: "wait.order", payload: { id: "late", kind: "match" } },
			});
			expect(await runAt()).toEqual(backoff);
		},
	);
});
