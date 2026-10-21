import { afterAll, afterEach, beforeAll, describe, expect, test } from "vitest";
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { defineEvent } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { TaskSchemas } from "../../src/schemas";
import type { Task } from "../../src/task";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { waitForCondition } from "../test-utils";

const metadataSchema = z.object({
	tenant: z.string().optional(),
	caller: z.string().optional(),
	stamp: z.string().optional(),
});
type Metadata = z.infer<typeof metadataSchema>;
type MetadataOption = Metadata | ((metadata: Readonly<Metadata> | undefined) => Metadata);

const ping = defineEvent({ name: "wrap.ping", payload: z.object({}) });
const audit = defineEvent({ name: "wrap.audit", payload: z.object({}) });
const labelled = z.object({ label: z.string() });
const definitions = [
	defineTask({ name: "wrap.parent" }),
	defineTask({ name: "wrap.from-middleware" }),
	defineTask({ name: "wrap.child", payload: labelled }),
	defineTask({ name: "wrap.internal", payload: labelled }),
	defineTask({ name: "wrap.report" }),
];

const reservedPrefix = "internal.";

function reserveKey(key: string) {
	if (key.startsWith(reservedPrefix)) throw new Error(`Step key "${key}" is reserved`);
}

function reserveDestination(name: string) {
	if (name === "wrap.internal" || name === "wrap.audit" || name.startsWith(reservedPrefix)) {
		throw new Error(`"${name}" is reserved`);
	}
}

function stamped(option: MetadataOption | undefined) {
	return (metadata: Readonly<Metadata> | undefined): Metadata => ({
		...(typeof option === "function" ? option(metadata) : option || metadata),
		stamp: "policy",
	});
}

describe("Wrapping context methods in middleware", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];
	const orchestrators: Orchestrator[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		await Promise.allSettled(orchestrators.map((orchestrator) => orchestrator.stop()));
		orchestrators.length = 0;
		await Promise.all(databases.map((db) => db.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	async function setup() {
		const db = await pool.child();
		databases.push(db);
		const calls: string[] = [];
		const rejected: string[] = [];
		const seen: Record<string, unknown> = {};
		const keyed = <T extends (key: string, ...args: never[]) => unknown>(
			name: string,
			method: T,
		): T =>
			(async (key: string, ...args: never[]) => {
				reserveKey(key);
				calls.push(`${name}:${key}`);
				return method(key, ...args);
			}) as T;

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(definitions),
			events: [ping, audit],
			metadata: metadataSchema,
			context: {},
			middleware: [
				async ({ ctx }, next) => {
					const invoke = ctx.invoke.bind(ctx);
					const invokeAndWait = ctx.invokeAndWait.bind(ctx);
					const signalWithStart = ctx.signalWithStart.bind(ctx);
					const sendSignal = ctx.sendSignal.bind(ctx);
					const emit = ctx.emit.bind(ctx);
					const schedule = ctx.schedule.bind(ctx);
					const step = ctx.step.bind(ctx);
					const checkpoint = ctx.checkpoint.bind(ctx);
					const sleep = ctx.sleep.bind(ctx);
					const waitForEvent = ctx.waitForEvent.bind(ctx);
					const subscribe = ctx.subscribe.bind(ctx);
					const waitForAny = ctx.waitForAny.bind(ctx);
					const waitForResult = ctx.waitForResult.bind(ctx);
					return next({
						step: keyed<typeof step>("step", step),
						sleep: keyed<typeof sleep>("sleep", sleep),
						waitForEvent: keyed<typeof waitForEvent>("waitForEvent", waitForEvent),
						subscribe: keyed<typeof subscribe>("subscribe", subscribe),
						waitForAny: keyed<typeof waitForAny>("waitForAny", waitForAny),
						waitForResult: keyed<typeof waitForResult>("waitForResult", waitForResult),
						checkpoint: async () => {
							calls.push("checkpoint");
							return checkpoint();
						},
						invoke: async (key, task, payload, options = {}) => {
							reserveKey(key);
							reserveDestination(task.name);
							calls.push(`invoke:${key}`);
							return invoke(key, task, payload, {
								...options,
								metadata: stamped(options.metadata),
							});
						},
						invokeAndWait: async (key, task, payload, options = {}) => {
							reserveKey(key);
							reserveDestination(task.name);
							return invokeAndWait(key, task, payload, {
								...options,
								metadata: stamped(options.metadata),
							});
						},
						signalWithStart: async (key, options) => {
							reserveKey(key);
							reserveDestination(options.task);
							reserveDestination(options.signal.name);
							return signalWithStart(key, { ...options, metadata: stamped(options.metadata) });
						},
						sendSignal: async (executionId, signal) => {
							reserveDestination(signal.name);
							return sendSignal(executionId, signal);
						},
						emit: async (event, payload, options = {}) => {
							reserveDestination(event);
							return emit(event, payload, { ...options, metadata: stamped(options.metadata) });
						},
						schedule: async (task, scheduleName, options, payload) => {
							reserveDestination(task.name);
							return schedule(
								task,
								scheduleName,
								{ ...options, metadata: stamped(options.metadata) },
								payload,
							);
						},
						// Internal helpers call the originals, which skip the policy
						audit: (label: string) =>
							invoke(`${reservedPrefix}audit.${label}`, { name: "wrap.internal" }, { label }),
						notify: () => emit("wrap.audit", {}),
					});
				},
				async ({ execution, ctx }, next) => {
					if (execution.task_key === "wrap.from-middleware") {
						await ctx.invoke("from-middleware", { name: "wrap.child" }, { label: "middleware" });
						await ctx
							.step(`${reservedPrefix}middleware`, () => null)
							.catch((error: Error) => rejected.push(error.message));
					}
					return next(ctx);
				},
			],
		});
		await conductor.ensureInstalled();

		const tasks: Task<any, any, any, any, any, any>[] = [
			conductor.createTask({ name: "wrap.child" }, { invocable: true }, async (event, ctx) => {
				seen[event.payload.label] = ctx.metadata;
			}),
			conductor.createTask({ name: "wrap.internal" }, { invocable: true }, async (event, ctx) => {
				seen[`internal.${event.payload.label}`] = ctx.metadata;
			}),
			conductor.createTask({ name: "wrap.report" }, { invocable: true }, async () => {}),
		];

		async function run(...handlers: Task<any, any, any, any, any, any>[]) {
			const orchestrator = Orchestrator.create({
				conductor,
				tasks: [...tasks, ...handlers],
				defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
			});
			orchestrators.push(orchestrator);
			await orchestrator.start();
		}

		async function completed(id: string) {
			await waitForCondition(async () => {
				const [row] = await db.sql<{ done: boolean }[]>`
					select completed_at is not null as done
					from pgconductor._private_executions where id = ${id}
				`;
				return row?.done === true;
			});
		}

		async function metadataOf(where: string) {
			return (
				await db.sql<{ metadata: Metadata | null }[]>`
					select metadata from pgconductor._private_executions where ${db.sql.unsafe(where)}
				`
			).map((row) => row.metadata);
		}

		return { db, conductor, calls, rejected, seen, run, completed, metadataOf };
	}

	test("merges metadata of every operation that sets it", async () => {
		const { conductor, seen, run, completed, metadataOf } = await setup();
		const parent = conductor.createTask(
			{ name: "wrap.parent" },
			{ invocable: true },
			async (_event, ctx) => {
				seen.parent = ctx.metadata;
				await ctx.invoke("inherited", { name: "wrap.child" }, { label: "inherited" });
				await ctx.invoke(
					"override",
					{ name: "wrap.child" },
					{ label: "override" },
					{ metadata: { tenant: "other" } },
				);
				await ctx.invokeAndWait("wait", { name: "wrap.child" }, { label: "wait" });
				await ctx.signalWithStart("start", {
					task: "wrap.child",
					key: "session",
					signal: { name: "go", id: "1" },
					input: { label: "start" },
					metadata: (metadata) => ({ ...metadata, caller: "start" }),
				});
				await ctx.emit("wrap.ping", {});
				await ctx.schedule({ name: "wrap.report" }, "yearly", { cron: "0 0 1 1 *" });
			},
		);
		await run(parent);

		const id = await conductor.invoke(
			{ name: "wrap.parent" },
			{},
			{ metadata: { tenant: "acme" } },
		);
		await completed(id);
		await waitForCondition(() => Object.keys(seen).length === 5);

		const policy = { tenant: "acme", stamp: "policy" };
		expect(seen).toEqual({
			parent: { tenant: "acme" },
			inherited: policy,
			override: { tenant: "other", stamp: "policy" },
			wait: policy,
			start: { ...policy, caller: "start" },
		});
		expect(await metadataOf("task_key = 'pgconductor.event-dispatch'")).toEqual([policy]);
		expect(await metadataOf("task_key = 'wrap.report'")).toEqual([policy]);
	}, 30000);

	test("rejects reserved keys and destinations before anything is stored", async () => {
		const { db, conductor, rejected, run, completed } = await setup();
		let stepRan = false;
		const parent = conductor.createTask(
			{ name: "wrap.parent" },
			{ invocable: true },
			async (_event, ctx) => {
				const reject = (operation: Promise<unknown>) =>
					operation.catch((error: Error) => rejected.push(error.message));
				const child = { name: "wrap.child" } as const;
				await reject(ctx.step("internal.step", () => void (stepRan = true)));
				await reject(ctx.sleep("internal.sleep", 1));
				await reject(ctx.invoke("internal.invoke", child, { label: "x" }));
				await reject(ctx.invoke("invoke", { name: "wrap.internal" }, { label: "x" }));
				await reject(ctx.invokeAndWait("internal.wait", child, { label: "x" }));
				await reject(ctx.waitForResult("internal.result", ctx.executionId));
				await reject(ctx.waitForEvent("internal.event", { event: ping }));
				await reject(ctx.subscribe("internal.subscribe", { event: ping }));
				await reject(ctx.waitForAny("internal.any", { go: { signal: "go" } }));
				await reject(
					ctx.signalWithStart("internal.start", {
						task: "wrap.child",
						key: "session",
						signal: { name: "go", id: "1" },
					}),
				);
				await reject(
					ctx.signalWithStart("start", {
						task: "wrap.child",
						key: "session",
						signal: { name: "internal.go", id: "1" },
					}),
				);
				await reject(ctx.sendSignal(ctx.executionId, { name: "internal.go", id: "1" }));
				await reject(ctx.emit("wrap.audit", {}));
				await reject(ctx.schedule({ name: "wrap.internal" }, "yearly", { cron: "0 0 1 1 *" }));
			},
		);
		await run(parent);

		const id = await conductor.invoke({ name: "wrap.parent" }, {});
		await completed(id);

		expect(rejected).toEqual([
			'Step key "internal.step" is reserved',
			'Step key "internal.sleep" is reserved',
			'Step key "internal.invoke" is reserved',
			'"wrap.internal" is reserved',
			'Step key "internal.wait" is reserved',
			'Step key "internal.result" is reserved',
			'Step key "internal.event" is reserved',
			'Step key "internal.subscribe" is reserved',
			'Step key "internal.any" is reserved',
			'Step key "internal.start" is reserved',
			'"internal.go" is reserved',
			'"internal.go" is reserved',
			'"wrap.audit" is reserved',
			'"wrap.internal" is reserved',
		]);
		expect(stepRan).toBe(false);
		const [stored] = await db.sql<
			{ executions: number; steps: number; signals: number; subscriptions: number }[]
		>`
			select
				(select count(*)::int from pgconductor._private_executions
					where task_key <> 'pgconductor.maintenance') as executions,
				(select count(*)::int from pgconductor._private_steps) as steps,
				(select count(*)::int from pgconductor._private_execution_signals) as signals,
				(select count(*)::int from pgconductor._private_custom_event_subscriptions
					where kind = 'execution_wait') as subscriptions
		`;
		expect(stored).toEqual({ executions: 1, steps: 0, signals: 0, subscriptions: 0 });
	}, 30000);

	test("internal helpers bypass the policy with the captured originals", async () => {
		const { db, conductor, seen, run, completed, metadataOf } = await setup();
		const parent = conductor.createTask(
			{ name: "wrap.parent" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.audit("parent");
				await ctx.notify();
			},
		);
		await run(parent);

		const id = await conductor.invoke(
			{ name: "wrap.parent" },
			{},
			{ metadata: { tenant: "acme" } },
		);
		await completed(id);
		await waitForCondition(() => "internal.parent" in seen);

		expect(seen["internal.parent"]).toEqual({ tenant: "acme" });
		expect(
			await db.sql`select key from pgconductor._private_steps where execution_id = ${id}`,
		).toEqual([{ key: "internal.audit.parent" }]);
		expect(await metadataOf("task_key = 'pgconductor.event-dispatch'")).toEqual([
			{ tenant: "acme" },
		]);
	}, 30000);

	test("wraps again on resume and keeps memoized results", async () => {
		const { db, conductor, calls, run, completed } = await setup();
		const attempts: { value: number; child: string }[] = [];
		let computed = 0;
		const parent = conductor.createTask(
			{ name: "wrap.parent" },
			{ invocable: true },
			async (_event, ctx) => {
				const value = await ctx.step("value", () => ++computed);
				const child = await ctx.invoke("child", { name: "wrap.child" }, { label: "child" });
				attempts.push({ value, child });
				await ctx.sleep("nap", 1);
			},
		);
		await run(parent);

		const id = await conductor.invoke({ name: "wrap.parent" }, {});
		await completed(id);

		expect(calls).toEqual([
			"step:value",
			"invoke:child",
			"sleep:nap",
			"step:value",
			"invoke:child",
			"sleep:nap",
		]);
		expect(computed).toBe(1);
		expect(attempts).toHaveLength(2);
		expect(attempts[1]).toEqual(attempts[0]);
		const [children] = await db.sql<{ count: number }[]>`
			select count(*)::int from pgconductor._private_executions where task_key = 'wrap.child'
		`;
		expect(children?.count).toBe(1);
	}, 30000);

	test("runs once per call: context methods do not call each other's wrappers", async () => {
		const { conductor, calls, run, completed } = await setup();
		const parent = conductor.createTask(
			{ name: "wrap.parent" },
			{ invocable: true },
			async (_event, ctx) => {
				const child = await ctx.invoke("child", { name: "wrap.child" }, { label: "child" });
				await ctx.waitForResult("result", child);
				const subscription = await ctx.subscribe("pinged", { event: ping });
				await ctx.emit("wrap.ping", {});
				await subscription.wait();
				await ctx.step("step", () => null);
				await ctx.signalWithStart("start", {
					task: "wrap.child",
					key: "session",
					signal: { name: "go", id: "1" },
					input: { label: "start" },
				});
			},
		);
		await run(parent);

		const id = await conductor.invoke({ name: "wrap.parent" }, {});
		await completed(id);

		expect(new Set(calls)).toEqual(
			new Set(["invoke:child", "waitForResult:result", "subscribe:pinged", "step:step"]),
		);
	}, 30000);

	test("applies to calls made by later middleware", async () => {
		const { conductor, calls, rejected, seen, run, completed } = await setup();
		const task = conductor.createTask(
			{ name: "wrap.from-middleware" },
			{ invocable: true },
			async () => {},
		);
		await run(task);

		const id = await conductor.invoke(
			{ name: "wrap.from-middleware" },
			{},
			{ metadata: { tenant: "acme" } },
		);
		await completed(id);
		await waitForCondition(() => "middleware" in seen);

		expect(calls).toContain("invoke:from-middleware");
		expect(rejected).toEqual(['Step key "internal.middleware" is reserved']);
		expect(seen.middleware).toEqual({ tenant: "acme", stamp: "policy" });
	}, 30000);
});
