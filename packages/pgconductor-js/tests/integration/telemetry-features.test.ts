import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { AsyncLocalStorage } from "node:async_hooks";
import {
	context,
	propagation,
	ROOT_CONTEXT,
	SpanKind,
	trace,
	type Attributes,
	type Context,
	type ContextManager,
	type Span,
} from "@opentelemetry/api";
import {
	BasicTracerProvider,
	InMemorySpanExporter,
	SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import type { Sql } from "postgres";
import { Conductor } from "../../src/conductor";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { Task } from "../../src/task";
import { Orchestrator } from "../../src/orchestrator";
import { Worker } from "../../src/worker";
import { DefaultLogger } from "../../src/lib/logger";
import { Telemetry, type TraceContextCarrier } from "../../src/telemetry";
import { InMemoryDatabaseClient } from "../mocks/in-memory-database-client";
import { TestDatabasePool, type TestDatabase } from "../fixtures/test-database";
import { defineEvent, type EventDefinition } from "../../src/event-definition";
import { defineTask } from "../../src/task-definition";
import { z } from "zod";
import { waitForCondition } from "../test-utils";

const logger = new DefaultLogger();
const fakeSql = Object.assign((async () => []) as unknown as Sql, {
	json: (value: unknown) => JSON.stringify(value),
});

class TestContextManager implements ContextManager {
	private readonly storage = new AsyncLocalStorage<Context>();
	active(): Context {
		return this.storage.getStore() || ROOT_CONTEXT;
	}
	with<A extends unknown[], F extends (...args: A) => ReturnType<F>>(
		ctx: Context,
		fn: F,
		thisArg?: ThisParameterType<F>,
		...args: A
	): ReturnType<F> {
		return this.storage.run(ctx, () => fn.apply(thisArg, args));
	}
	bind<T>(_: Context, target: T): T {
		return target;
	}
	enable(): this {
		return this;
	}
	disable(): this {
		this.storage.disable();
		return this;
	}
}

const activeProviders = new Set<BasicTracerProvider>();
const activeOrchestrators = new Set<Orchestrator>();

function installProvider() {
	context.disable();
	trace.disable();
	const exporter = new InMemorySpanExporter();
	const provider = new BasicTracerProvider();
	provider.addSpanProcessor(new SimpleSpanProcessor(exporter));
	provider.register();
	context.setGlobalContextManager(new TestContextManager());
	activeProviders.add(provider);
	return { exporter, provider };
}

async function cleanupProvider(provider: BasicTracerProvider) {
	activeProviders.delete(provider);
	await provider.shutdown();
	context.disable();
	trace.disable();
}

async function dispatchPendingEvents(db: InMemoryDatabaseClient): Promise<void> {
	const orchestratorId = crypto.randomUUID();
	const executions = await db.getExecutions({
		orchestratorId,
		queueName: "pgconductor.internal",
		batchSize: 100,
		filterTaskKeys: [],
	});
	await db.dispatchCustomEvents({
		eventIds: executions.map((execution) => execution.id),
		orchestratorId,
	});
	await db.returnExecutions(
		executions.map((execution) => ({
			execution_id: execution.id,
			orchestrator_id: orchestratorId,
			queue: execution.queue,
			task_key: execution.task_key,
			status: "completed" as const,
		})),
	);
}

function namedSpans(exporter: InMemorySpanExporter, name: string) {
	return exporter.getFinishedSpans().filter((span) => span.name === name);
}

function makeTask(
	name: string,
	execute: (event: any, ctx: any) => Promise<any>,
	definition: Record<string, unknown> = {},
	trigger: Record<string, unknown> = { invocable: true },
	eventDefinitions: readonly EventDefinition<string, any, any>[] = [],
) {
	return Task.create(
		{ name, ...definition } as any,
		trigger as any,
		execute as any,
		eventDefinitions,
	);
}

function makeWorker(
	db: InMemoryDatabaseClient,
	tasks: any[],
	telemetry = true,
	eventDefinitions: any[] = [],
) {
	return new Worker(
		"default",
		tasks,
		db as any,
		logger,
		{ pollIntervalMs: 1, flushIntervalMs: 1, fetchBatchSize: 10, flushBatchSize: 10 },
		{},
		eventDefinitions,
		new Telemetry(telemetry),
	);
}

function startSpan(name: string, kind: SpanKind, attributes?: Attributes): Span {
	return trace.getTracer("telemetry-features-test").startSpan(name, { kind, attributes });
}

function endSpan(span: Span): void {
	span.end();
}

function runWithSpan<T>(span: Span, run: () => T): T {
	return context.with(trace.setSpan(context.active(), span), run);
}

function externalProducer(name: string, destination = "default") {
	const span = startSpan(name, SpanKind.PRODUCER, {
		"messaging.system": "postgres_conductor",
		"messaging.destination.name": destination,
		"messaging.operation.name": "send",
		"messaging.operation.type": "send",
	});
	const carrier: Record<string, string> = {};
	propagation.inject(trace.setSpan(context.active(), span), carrier);
	return { span, carrier: carrier as TraceContextCarrier };
}

describe.serial("OpenTelemetry trace propagation feature paths", () => {
	test("Conductor.emit producer becomes the event-trigger consumer parent", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		const event = defineEvent({ name: "feature.event", payload: z.object({ id: z.string() }) });
		const conductor = Conductor.create({
			sql: fakeSql,
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		(conductor as any).db = db;
		const task = makeTask("event-consumer", async () => undefined, {}, { event: "feature.event" }, [
			event,
		]);
		const worker = makeWorker(db, [task], true, [event]);
		await worker.drain("event-worker-register");

		const root = externalProducer("event-root");
		await runWithSpan(root.span, () => conductor.emit("feature.event", { id: "1" }));
		endSpan(root.span);
		await dispatchPendingEvents(db);
		await worker.drain("event-worker");

		const send = namedSpans(exporter, "send event feature.event")[0]!;
		const process = namedSpans(exporter, "process default")[0]!;
		expect(send.parentSpanId).toBe(root.span?.spanContext().spanId);
		expect(process.parentSpanId).toBe(send.spanContext().spanId);
		expect(process.spanContext().traceId).toBe(send.spanContext().traceId);
		await cleanupProvider(provider);
	});

	test("waitForEvent keeps the task producer parent and links the event producer (real Postgres)", async () => {
		const db = await postgresDatabases.child();
		postgresChildren.push(db);
		const event = defineEvent({ name: "feature.wait", payload: z.object({ id: z.string() }) });
		const taskDefinition = defineTask({ name: "feature.waiter", payload: z.object({}) });
		const { exporter, provider } = installProvider();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const task = conductor.createTask(
			{ name: "feature.waiter" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.waitForEvent("feature-wait", { event });
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		activeOrchestrators.add(orchestrator);
		await orchestrator.start();
		const invocation = await conductor.invoke({ name: "feature.waiter" }, {});
		await waitForCondition(async () => {
			const rows = await db.sql<{ n: number }[]>`
				select count(*)::int as n from pgconductor._private_custom_event_subscriptions
				where kind = 'execution_wait'
			`;
			return Number(rows[0]?.n || 0) === 1;
		});
		const eventId = await conductor.emit("feature.wait", { id: "event" });
		await waitForCondition(async () => {
			const rows = await db.sql<{ completed_at: Date | null }[]>`
				select completed_at from pgconductor._private_executions where id = ${invocation}
			`;
			return Boolean(rows[0]?.completed_at);
		});

		const invocationSend = namedSpans(exporter, "send default").find(
			(span) => span.attributes["pgconductor.task.name"] === "feature.waiter",
		)!;
		const eventSend = namedSpans(exporter, "send event feature.wait")[0]!;
		const processes = namedSpans(exporter, "process default");
		const resumed = processes.find((process) =>
			process.links.some((link) => link.context.spanId === eventSend.spanContext().spanId),
		)!;
		expect(resumed).toBeTruthy();
		expect(resumed.parentSpanId).toBe(invocationSend.spanContext().spanId);
		expect(resumed.spanContext().traceId).toBe(invocationSend.spanContext().traceId);
		expect(resumed.links.map((link) => link.context.spanId)).toContain(
			eventSend.spanContext().spanId,
		);
		expect(eventId).toBeTruthy();
		await cleanupProvider(provider);
	});

	test("a matched wait followed by a timeout has no stale event link (real Postgres)", async () => {
		const db = await postgresDatabases.child();
		postgresChildren.push(db);
		const event = defineEvent({
			name: "feature.match-timeout",
			payload: z.object({ id: z.string() }),
		});
		const taskDefinition = defineTask({
			name: "feature.match-timeout-task",
			payload: z.object({}),
		});
		const { exporter, provider } = installProvider();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([taskDefinition]),
			events: EventSchemas.fromSchema([event]),
			context: {},
		});
		const task = conductor.createTask(
			{ name: "feature.match-timeout-task" },
			{ invocable: true },
			async (_taskEvent, ctx) => {
				await ctx.waitForEvent("matched", { event });
				try {
					await ctx.waitForEvent("timed-out", { event, timeout: "20ms" });
				} catch {
					// The second wait intentionally times out.
				}
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		activeOrchestrators.add(orchestrator);
		await orchestrator.start();
		const invocation = await conductor.invoke({ name: task.name }, {});
		await waitForCondition(async () => {
			const rows = await db.sql<{ n: number }[]>`
				select count(*)::int as n from pgconductor._private_custom_event_subscriptions
				where execution_id = ${invocation}::uuid
			`;
			return Number(rows[0]?.n || 0) === 1;
		});
		await conductor.emit(event.name, { id: "matched" });
		await waitForCondition(async () => {
			const rows = await db.sql<{ step_key: string }[]>`
				select step_key from pgconductor._private_custom_event_subscriptions
				where execution_id = ${invocation}::uuid
			`;
			return rows[0]?.step_key === "timed-out";
		});
		await waitForCondition(async () => {
			const rows = await db.sql<{ completed_at: Date | null }[]>`
				select completed_at from pgconductor._private_executions where id = ${invocation}::uuid
			`;
			return Boolean(rows[0]?.completed_at);
		});

		const processes = namedSpans(exporter, "process default").filter(
			(span) => span.attributes["pgconductor.task.name"] === task.name,
		);
		const eventSend = namedSpans(exporter, "send event feature.match-timeout")[0]!;
		const resumed = processes.find((span) =>
			span.links.some((link) => link.context.spanId === eventSend.spanContext().spanId),
		)!;
		expect(resumed).toBeTruthy();
		expect(
			processes
				.at(-1)
				?.links.some((link) => link.context.spanId === eventSend.spanContext().spanId),
		).toBe(false);
		await cleanupProvider(provider);
	}, 60_000);

	test("an event link is consumed by the first resumed attempt", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		const event = defineEvent({
			name: "feature.retry-wait",
			payload: z.object({ id: z.string() }),
		});
		let resumes = 0;
		const task = makeTask(
			"feature.retry-waiter",
			async (_event, ctx) => {
				await ctx.waitForEvent("event", { event });
				if (++resumes === 1) throw new Error("retry after event");
			},
			{ maxAttempts: 2 },
			{ invocable: true },
			[event],
		);

		const invocation = externalProducer("retry-wait invocation");
		await db.invoke({
			task_key: task.name,
			queue: "default",
			payload: {},
			trace_context: { parent: invocation.carrier },
		});
		endSpan(invocation.span);
		await makeWorker(db, [task], true, [event]).drain("register-wait");
		await db.setFakeTime({ date: new Date(db.getCurrentTimeSync().getTime() + 1) });

		const eventProducer = externalProducer("retry-wait event");
		await db.emitEvent({
			eventKey: event.name,
			payload: { id: "event" },
			trace_context: { parent: eventProducer.carrier },
		});
		endSpan(eventProducer.span);
		await dispatchPendingEvents(db);
		await makeWorker(db, [task], true, [event]).drain("resume-wait");

		const execution = db.getAllExecutions().find((item) => item.task_key === task.name)!;
		await db.setFakeTime({ date: new Date(execution.run_at.getTime() + 1) });
		await makeWorker(db, [task], true, [event]).drain("retry-wait");

		const processes = namedSpans(exporter, "process default").filter(
			(span) => span.attributes["pgconductor.task.name"] === task.name,
		);
		const eventSpanId = eventProducer.span?.spanContext().spanId;
		expect(
			processes.filter((span) => span.links.some((link) => link.context.spanId === eventSpanId)),
		).toHaveLength(1);
		expect(processes.at(-1)?.links.some((link) => link.context.spanId === eventSpanId)).toBe(false);
		await cleanupProvider(provider);
	});

	test("child invocation creates producer-parented child process spans", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		const child = makeTask("child-feature", async () => undefined);
		const parent = makeTask("parent-feature", async (_event, ctx) =>
			ctx.invoke("child", child, {}),
		);
		await db.invoke({ task_key: parent.name, queue: "default", payload: {} });
		await makeWorker(db, [parent, child]).drain("child-first");
		await makeWorker(db, [parent, child]).drain("child-second");

		const parentProcess = namedSpans(exporter, "process default")[0]!;
		const childSend = namedSpans(exporter, "send default").find(
			(span) => span.attributes["pgconductor.task.name"] === child.name,
		)!;
		const childProcess = namedSpans(exporter, "process default").find(
			(span) => span.attributes["pgconductor.task.name"] === child.name,
		)!;
		expect(childSend.parentSpanId).toBe(parentProcess.spanContext().spanId);
		expect(childProcess.parentSpanId).toBe(childSend.spanContext().spanId);
		expect(childProcess.spanContext().traceId).toBe(parentProcess.spanContext().traceId);
		await cleanupProvider(provider);
	});

	test("child and parent dead letters preserve distinct producer carriers (real Postgres)", async () => {
		const db = await postgresDatabases.child();
		postgresChildren.push(db);
		const payload = z.object({});
		const definitions = [
			defineTask({ name: "feature.dlq-parent", payload }),
			defineTask({ name: "feature.dlq-child", payload }),
			defineTask({ name: "feature.dlq-child-target", queue: "feature.child-dlq", payload }),
			defineTask({ name: "feature.dlq-parent-target", queue: "feature.parent-dlq", payload }),
		];
		const { exporter, provider } = installProvider();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(definitions),
			context: {},
		});
		const childTarget = conductor.createTask(
			{ name: "feature.dlq-child-target", queue: "feature.child-dlq" },
			{ invocable: true },
			async () => undefined,
		);
		const parentTarget = conductor.createTask(
			{ name: "feature.dlq-parent-target", queue: "feature.parent-dlq" },
			{ invocable: true },
			async () => undefined,
		);
		const child = conductor.createTask(
			{
				name: "feature.dlq-child",
				maxAttempts: 1,
				deadLetter: { queue: "feature.child-dlq", task: childTarget },
			},
			{ invocable: true },
			async () => {
				throw new Error("child terminal failure");
			},
		);
		const parent = conductor.createTask(
			{
				name: "feature.dlq-parent",
				maxAttempts: 1,
				deadLetter: { queue: "feature.parent-dlq", task: parentTarget },
			},
			{ invocable: true },
			async (_event, ctx) => ctx.invoke("child", child, {}),
		);
		const sourceOrchestrator = Orchestrator.create({
			conductor,
			tasks: [parent, child],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		activeOrchestrators.add(sourceOrchestrator);
		await sourceOrchestrator.start();
		await conductor.invoke({ name: parent.name }, {});
		await waitForCondition(async () => {
			const rows = await db.sql<{ queue: string; count: string }[]>`
				select queue, count(*)::text as count from pgconductor._private_executions
				where queue in ('feature.child-dlq', 'feature.parent-dlq') group by queue
			`;
			return rows.length === 2 && rows.every((row) => row.count === "1");
		});
		await sourceOrchestrator.stop();

		const destinationOrchestrator = Orchestrator.create({
			conductor,
			workers: [
				conductor.createWorker({
					queue: "feature.child-dlq",
					tasks: [childTarget],
					config: { pollIntervalMs: 10, flushIntervalMs: 10 },
				}),
				conductor.createWorker({
					queue: "feature.parent-dlq",
					tasks: [parentTarget],
					config: { pollIntervalMs: 10, flushIntervalMs: 10 },
				}),
			],
		});
		activeOrchestrators.add(destinationOrchestrator);
		await destinationOrchestrator.start();
		await waitForCondition(async () => {
			const rows = await db.sql<{ count: string }[]>`
				select count(*)::text as count from pgconductor._private_executions
				where queue in ('feature.child-dlq', 'feature.parent-dlq') and completed_at is not null
			`;
			return rows[0]?.count === "2";
		});

		const sends = namedSpans(exporter, "send feature.child-dlq").filter(
			(span) => span.attributes["pgconductor.task.name"] === childTarget.name,
		);
		const parentSends = namedSpans(exporter, "send feature.parent-dlq").filter(
			(span) => span.attributes["pgconductor.task.name"] === parentTarget.name,
		);
		const childProcess = namedSpans(exporter, "process feature.child-dlq").find(
			(span) => span.attributes["pgconductor.task.name"] === childTarget.name,
		);
		const parentProcess = namedSpans(exporter, "process feature.parent-dlq").find(
			(span) => span.attributes["pgconductor.task.name"] === parentTarget.name,
		);
		expect(sends).toHaveLength(1);
		expect(parentSends).toHaveLength(1);
		expect(childProcess?.parentSpanId).toBe(sends[0]?.spanContext().spanId);
		expect(parentProcess?.parentSpanId).toBe(parentSends[0]?.spanContext().spanId);
		expect(childProcess?.parentSpanId).not.toBe(parentProcess?.parentSpanId);
		await cleanupProvider(provider);
	});

	test("initial and recurring cron executions use producer then consumer carriers", async () => {
		const { exporter, provider } = installProvider();
		const now = new Date("2024-01-01T00:00:00.000Z");
		const db = new InMemoryDatabaseClient(now);
		const cron = makeTask(
			"cron-feature",
			async () => undefined,
			{},
			{ cron: "* * * * *", name: "minute" },
		);
		const worker = makeWorker(db, [cron]);
		await worker.drain("cron-register");
		db.advanceTime(60_000);
		await worker.drain("cron-first");
		db.advanceTime(60_000);
		await worker.drain("cron-second");

		const sends = namedSpans(exporter, "send default").filter(
			(span) => span.attributes["pgconductor.task.name"] === cron.name,
		);
		const processes = namedSpans(exporter, "process default").filter(
			(span) => span.attributes["pgconductor.task.name"] === cron.name,
		);
		expect(processes.length).toBeGreaterThanOrEqual(2);
		expect(sends.length).toBeGreaterThanOrEqual(2);
		const sendIds = new Set(sends.map((span) => span.spanContext().spanId));
		const processParents = processes.slice(0, 2).map((span) => span.parentSpanId);
		expect(processParents.every((parent) => parent !== undefined && sendIds.has(parent))).toBe(
			true,
		);
		expect(new Set(processParents)).toHaveLength(2);
		await cleanupProvider(provider);
	});

	test("final DLQ has a destination producer and consumer, but retry/cancel/no-DLQ do not", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		const dlq = makeTask("feature-dlq", async () => undefined);
		const final = makeTask(
			"feature-final",
			async () => {
				throw new Error("terminal");
			},
			{ maxAttempts: 1, deadLetter: { queue: "default", task: dlq } },
		);
		const retry = makeTask(
			"feature-retry",
			async () => {
				throw new Error("retry");
			},
			{ maxAttempts: 2 },
		);
		const cancel = makeTask("feature-cancel", async () => undefined, { maxAttempts: 1 });
		const noDlq = makeTask(
			"feature-no-dlq",
			async () => {
				throw new Error("failure");
			},
			{ maxAttempts: 1 },
		);
		const cancelledId = await db.invoke({ task_key: cancel.name, queue: "default", payload: {} });
		await db.cancelExecution(cancelledId!, { reason: "cancelled" });
		await db.invoke({ task_key: final.name, queue: "default", payload: {} });
		await db.invoke({ task_key: retry.name, queue: "default", payload: {} });
		await db.invoke({ task_key: noDlq.name, queue: "default", payload: {} });
		const worker = makeWorker(db, [dlq, final, retry, cancel, noDlq]);
		await worker.drain("lifecycle");
		await worker.drain("dead-letter");

		const dlqSend = namedSpans(exporter, "send default").find(
			(span) => span.attributes["pgconductor.task.name"] === dlq.name,
		);
		const dlqProcess = namedSpans(exporter, "process default").find(
			(span) => span.attributes["pgconductor.task.name"] === dlq.name,
		);
		expect(dlqSend).toBeTruthy();
		expect(dlqProcess).toBeTruthy();
		expect(dlqProcess?.parentSpanId).toBe(dlqSend?.spanContext().spanId);
		expect(
			namedSpans(exporter, "send default").filter((span) =>
				[retry.name, cancel.name, noDlq.name].includes(
					String(span.attributes["pgconductor.task.name"]),
				),
			),
		).toHaveLength(0);
		await cleanupProvider(provider);
	});

	test("dedupe replacement propagates the latest accepted producer context", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		const task = makeTask("feature-dedupe", async () => undefined);
		const first = externalProducer("accepted-first");
		const firstId = await runWithSpan(first.span, () =>
			db.invoke({
				task_key: task.name,
				queue: "default",
				payload: { value: 1 },
				dedupe_key: "same",
				trace_context: { parent: first.carrier },
			}),
		);
		endSpan(first.span);
		const second = externalProducer("accepted-latest");
		await runWithSpan(second.span, () =>
			db.invoke({
				task_key: task.name,
				queue: "default",
				payload: { value: 2 },
				dedupe_key: "same",
				trace_context: { parent: second.carrier },
			}),
		);
		endSpan(second.span);
		await makeWorker(db, [task]).drain("dedupe");
		const process = namedSpans(exporter, "process default")[0]!;
		expect(process.parentSpanId).toBe(second.span?.spanContext().spanId);
		expect(process.parentSpanId).not.toBe(first.span?.spanContext().spanId);
		expect(db.getExecution(firstId!)?.payload).toEqual({ value: 2 });
		await cleanupProvider(provider);
	});

	test("telemetry false suppresses event, child, cron, DLQ, and carrier spans", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient(new Date("2024-01-01T00:00:00.000Z"));
		const child = makeTask("disabled-child", async () => undefined);
		const parent = makeTask("disabled-parent", async (_event, ctx) =>
			ctx.invoke("child", child, {}),
		);
		const bad = makeTask(
			"disabled-bad",
			async () => {
				throw new Error("bad");
			},
			{ maxAttempts: 1, deadLetter: { queue: "default", task: child } },
		);
		await db.invoke({ task_key: parent.name, queue: "default", payload: {} });
		await db.invoke({ task_key: bad.name, queue: "default", payload: {} });
		await makeWorker(db, [parent, child, bad], false).drain("disabled");
		expect(exporter.getFinishedSpans()).toHaveLength(0);
		expect(db.getAllExecutions().every((execution) => execution.trace_context == null)).toBe(true);
		await cleanupProvider(provider);
	});

	test("settlement failure still closes durable producer spans", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		const child = makeTask("settle-child", async () => undefined);
		const parent = makeTask("settle-parent", async (_event, ctx) => ctx.invoke("child", child, {}));
		await db.invoke({ task_key: parent.name, queue: "default", payload: {} });
		(db as any).returnExecutions = async () => {
			throw new Error("settlement failed");
		};
		await makeWorker(db, [parent, child]).drain("settle-failure");
		expect(
			namedSpans(exporter, "send default").some(
				(span) => span.attributes["pgconductor.task.name"] === child.name,
			),
		).toBe(true);
		expect(exporter.getFinishedSpans().every((span) => span.endTime[0] !== 0)).toBe(true);
		await cleanupProvider(provider);
	});
});

let postgresDatabases: TestDatabasePool;
const postgresChildren: TestDatabase[] = [];
beforeAll(async () => {
	postgresDatabases = await TestDatabasePool.create();
}, 60000);
afterEach(async () => {
	await Promise.all([...activeOrchestrators].map((orchestrator) => orchestrator.stop()));
	activeOrchestrators.clear();
	await Promise.all([...activeProviders].map((provider) => cleanupProvider(provider)));
	await Promise.all(postgresChildren.map((db) => db.destroy()));
	postgresChildren.length = 0;
});
afterAll(async () => {
	await postgresDatabases?.destroy();
});
