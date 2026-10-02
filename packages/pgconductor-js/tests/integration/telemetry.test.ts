import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { AsyncLocalStorage } from "node:async_hooks";
import {
	context,
	propagation,
	ROOT_CONTEXT,
	SpanKind,
	SpanStatusCode,
	trace,
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
import { z } from "zod";
import { Conductor } from "../../src/conductor";
import { defineEvent } from "../../src/event-definition";
import { EventSchemas, TaskSchemas } from "../../src/schemas";
import { Orchestrator } from "../../src/orchestrator";
import { Task } from "../../src/task";
import { Worker } from "../../src/worker";
import { DefaultLogger } from "../../src/lib/logger";
import type { DatabaseClient } from "../../src/database-client";
import { TestDatabasePool } from "../fixtures/test-database";
import { Telemetry, type TraceContextCarrier } from "../../src/telemetry";
import { defineTask } from "../../src/task-definition";
import { waitForCondition } from "../test-utils";

const logger = new DefaultLogger();
const orchestratorId = crypto.randomUUID();

let pool: TestDatabasePool;

beforeAll(async () => {
	pool = await TestDatabasePool.create();
}, 60000);

afterAll(async () => {
	await pool?.destroy();
});

async function createDb() {
	const db = await pool.child();
	await Conductor.create({ sql: db.sql, context: {} }).ensureInstalled();
	return db;
}

async function getExecution(sql: Sql, id: string) {
	const [execution] = await sql`
		select completed_at, trace_context from pgconductor._private_executions where id = ${id}
	`;
	return execution;
}

// sdk-trace-base deliberately does not install a context manager. Use the
// platform async context manager so worker/user-child assertions exercise the
// same active-span behavior as a Node application.
class TestContextManager implements ContextManager {
	private readonly storage = new AsyncLocalStorage<Context>();
	active() {
		return this.storage.getStore() || ROOT_CONTEXT;
	}
	with<T>(ctx: Context, fn: (...args: any[]) => T, thisArg?: any, ...args: any[]) {
		return this.storage.run(ctx, () => fn.apply(thisArg, args));
	}
	bind<T>(ctx: Context, target: T): T {
		return target;
	}
	enable() {
		return this;
	}
	disable() {
		this.storage.disable();
		return this;
	}
}

const fakeSql = Object.assign((async () => [{ id: "fake-execution" }]) as unknown as Sql, {
	json: (value: unknown) => JSON.stringify(value),
});

function resetGlobals() {
	context.disable();
	propagation.disable();
	trace.disable();
}

function installProvider() {
	resetGlobals();
	const exporter = new InMemorySpanExporter();
	const provider = new BasicTracerProvider();
	provider.addSpanProcessor(new SimpleSpanProcessor(exporter));
	provider.register();
	context.setGlobalContextManager(new TestContextManager());
	return { exporter, provider };
}

async function cleanup(provider: BasicTracerProvider) {
	await provider.shutdown();
	resetGlobals();
}

function spans(exporter: InMemorySpanExporter, name: string) {
	return exporter.getFinishedSpans().filter((span) => span.name === name);
}

function makeTask(
	name: string,
	execute: (event: any, ctx: any) => Promise<any>,
	config: Record<string, unknown> = {},
	triggers: object = { invocable: true },
) {
	return Task.create({ name, ...config } as any, triggers as any, execute as any);
}

function makeWorker(db: DatabaseClient, task: any, telemetry = true) {
	return new Worker(
		"default",
		[task],
		db,
		logger,
		{ pollIntervalMs: 1, flushIntervalMs: 1, fetchBatchSize: 10, flushBatchSize: 10 },
		{},
		new Telemetry(telemetry),
	);
}

const producerCarrier = (name: string) => {
	const producer = trace.getTracer("test").startSpan(name, { kind: SpanKind.PRODUCER });
	const carrier: Record<string, string> = {};
	propagation.inject(trace.setSpan(context.active(), producer), carrier);
	producer.end();
	return { carrier: carrier as TraceContextCarrier, producer };
};

describe.serial("OpenTelemetry instrumentation", () => {
	test("a Conductor made before provider registration uses the later global provider", async () => {
		const conductor = Conductor.create({ sql: fakeSql, context: {} });
		const { exporter, provider } = installProvider();
		await conductor.invoke({ name: "later-provider" }, { value: "not an attribute" } as any);
		expect(spans(exporter, "send default")).toHaveLength(1);
		await cleanup(provider);
	});

	test("telemetry false emits no spans and persists null carrier", async () => {
		const { exporter, provider } = installProvider();
		const { sql } = await createDb();
		const conductor = Conductor.create({ sql, context: {}, telemetry: false });
		const id = (await conductor.invoke({ name: "disabled" }, {
			secret: "payload",
		} as any)) as unknown as string;
		expect(exporter.getFinishedSpans()).toHaveLength(0);
		expect((await getExecution(sql, id))?.trace_context).toBeNull();
		await cleanup(provider);
	});

	test("the orchestrator passes telemetry opt-out to its default worker", async () => {
		const { exporter, provider } = installProvider();
		const { sql } = await createDb();
		const conductor = Conductor.create({ sql, context: {}, telemetry: false });
		const task = makeTask("orchestrator-disabled", async () => undefined);
		const id = (await conductor.invoke({ name: "orchestrator-disabled" }, {
			secret: "payload",
		} as any)) as unknown as string;
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [task],
			defaultWorker: { pollIntervalMs: 1, flushIntervalMs: 1 },
		} as any);

		await orchestrator.drain();
		const execution = await getExecution(sql, id);
		expect(execution?.completed_at).not.toBeNull();
		expect(exporter.getFinishedSpans()).toHaveLength(0);
		expect(execution?.trace_context).toBeNull();
		await cleanup(provider);
	});

	test("telemetry opt-out includes the internal event-dispatch worker", async () => {
		const { exporter, provider } = installProvider();
		const { client: db, sql } = await createDb();
		const conductor = Conductor.create({
			sql,
			events: EventSchemas.fromSchema([
				defineEvent({ name: "disabled.event", payload: z.object({}) }),
			]),
			context: {},
			telemetry: false,
		});
		const eventId = await db.emitEvent({ eventKey: "disabled.event", payload: {} });
		expect((await getExecution(sql, eventId))?.completed_at).toBeNull();
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [],
			defaultWorker: { pollIntervalMs: 1, flushIntervalMs: 1 },
		} as any);

		await orchestrator.drain();
		expect(await getExecution(sql, eventId)).toBeUndefined();
		expect(exporter.getFinishedSpans()).toHaveLength(0);
		await cleanup(provider);
	});

	test("producer carrier becomes the real worker consumer parent", async () => {
		const { exporter, provider } = installProvider();
		const { client: db } = await createDb();
		const { carrier, producer } = producerCarrier("send-parent");
		await db.invoke({
			task_key: "parented",
			queue: "default",
			payload: {},
			trace_context: carrier,
		});
		const task = makeTask("parented", async () => undefined);
		await makeWorker(db, task).drain(orchestratorId);
		const consumer = spans(exporter, "process default")[0]!;
		expect(consumer.parentSpanId).toBe(producer.spanContext().spanId);
		expect(consumer.spanContext().traceId).toBe(producer.spanContext().traceId);
		expect(consumer.links.map((link) => link.context.spanId)).toEqual([
			producer.spanContext().spanId,
		]);
		await cleanup(provider);
	});

	test("batch consumer links every producer context", async () => {
		const { exporter, provider } = installProvider();
		const { client: db } = await createDb();
		const first = producerCarrier("batch-one");
		const second = producerCarrier("batch-two");
		await db.invokeBatch([
			{ task_key: "batched", queue: "default", payload: {}, trace_context: first.carrier },
			{ task_key: "batched", queue: "default", payload: {}, trace_context: second.carrier },
			{ task_key: "batched", queue: "default", payload: {}, trace_context: first.carrier },
		]);
		const task = makeTask("batched", async () => [], { batch: { size: 10, timeoutMs: 1 } });
		await makeWorker(db, task).drain(orchestratorId);
		const consumer = spans(exporter, "process default")[0]!;
		expect(consumer.links.map((link) => link.context.spanId)).toEqual(
			expect.arrayContaining([
				first.producer.spanContext().spanId,
				second.producer.spanContext().spanId,
			]),
		);
		expect(consumer.links).toHaveLength(2);
		await cleanup(provider);
	});

	test("retry attempts have finite, separate process spans", async () => {
		const { exporter, provider } = installProvider();
		const { client: db, sql } = await createDb();
		let attempts = 0;
		const task = makeTask("retry-span", async () => {
			if (++attempts === 1) throw new Error("try again");
		});
		(task as any).maxAttempts = 2;
		const id = await db.invoke({ task_key: "retry-span", queue: "default", payload: {} });
		const worker = makeWorker(db, task);
		await worker.drain(orchestratorId);
		await db.setFakeTime({ date: new Date(Date.now() + 16000) });
		await worker.drain(orchestratorId);
		const processSpans = spans(exporter, "process default");
		expect(processSpans).toHaveLength(2);
		expect(new Set(processSpans.map((span) => span.spanContext().spanId)).size).toBe(2);
		expect(processSpans[0]!.status.code).toBe(SpanStatusCode.ERROR);
		expect(processSpans[0]!.attributes["error.type"]).toBe("Error");
		expect((await getExecution(sql, id!))?.completed_at).not.toBeNull();
		await cleanup(provider);
	});

	test("a user-created active child is a child of process", async () => {
		const { exporter, provider } = installProvider();
		const { client: db } = await createDb();
		const task = makeTask("active-child", async () => {
			const child = trace.getTracer("user").startSpan("user child");
			child.end();
		});
		await db.invoke({ task_key: "active-child", queue: "default", payload: {} });
		await makeWorker(db, task).drain(orchestratorId);
		const parent = spans(exporter, "process default")[0]!;
		const child = spans(exporter, "user child")[0]!;
		expect(child.parentSpanId).toBe(parent.spanContext().spanId);
		await cleanup(provider);
	});

	test("process spans cover the application handler, not recurring-task scheduling", async () => {
		const { exporter, provider } = installProvider();
		const { client: db } = await createDb();
		await db.invoke({
			task_key: "handler-boundary",
			queue: "default",
			payload: {},
			cron_expression: "* * * * *",
			dedupe_key: "scheduled::boundary::1",
		});
		const originalInvoke = db.invoke.bind(db);
		let schedulingSpanId: string | undefined;
		(db as any).invoke = async (spec: any, options?: any) => {
			schedulingSpanId = trace.getSpan(context.active())?.spanContext().spanId;
			return originalInvoke(spec, options);
		};
		let handlerSpanId: string | undefined;
		await makeWorker(
			db,
			makeTask(
				"handler-boundary",
				async () => {
					handlerSpanId = trace.getSpan(context.active())?.spanContext().spanId;
				},
				{},
				{ cron: "* * * * *", name: "boundary" },
			),
		).drain(orchestratorId);
		const process = spans(exporter, "process default")[0]!;
		expect(schedulingSpanId).toBeUndefined();
		expect(handlerSpanId).toBe(process.spanContext().spanId);
		await cleanup(provider);
	});

	test("settle records database errors", async () => {
		const { exporter, provider } = installProvider();
		const { client: db } = await createDb();
		await db.invoke({ task_key: "settle-error", queue: "default", payload: {} });
		const original = db.returnExecutions.bind(db);
		(db as any).returnExecutions = async () => {
			throw new Error("database unavailable");
		};
		await makeWorker(
			db,
			makeTask("settle-error", async () => undefined),
		).drain(orchestratorId);
		const settle = spans(exporter, "settle default")[0]!;
		expect(settle.status.code).toBe(SpanStatusCode.ERROR);
		expect(settle.attributes["error.type"]).toBe("Error");
		(db as any).returnExecutions = original;
		await cleanup(provider);
	});

	test("step callback is one span and cached replay creates none", async () => {
		const { exporter, provider } = installProvider();
		const { client: db } = await createDb();
		let attempts = 0;
		const task = makeTask("step-cache", async (_event, ctx) => {
			const value = await ctx.step("once", async () => 42);
			if (++attempts === 1) throw new Error("retry");
			return { value };
		});
		await db.registerWorker({
			queueName: "default",
			taskSpecs: [{ key: "step-cache", maxAttempts: 2 }],
			cronSchedules: [],
			eventSubscriptions: [],
		});
		await db.invoke({ task_key: "step-cache", queue: "default", payload: {} });
		const worker = makeWorker(db, task);
		await worker.drain(orchestratorId);
		await db.setFakeTime({ date: new Date(Date.now() + 16000) });
		await worker.drain(orchestratorId);
		const stepSpan = spans(exporter, "step step-cache")[0]!;
		expect(spans(exporter, "step step-cache")).toHaveLength(1);
		expect(stepSpan.parentSpanId).toBe(spans(exporter, "process default")[0]!.spanContext().spanId);
		await cleanup(provider);
	});

	test("cron and event executions are root process spans", async () => {
		const { exporter, provider } = installProvider();
		const { client: db, sql } = await createDb();
		await db.invoke({
			task_key: "root-cron",
			queue: "default",
			payload: {},
			cron_expression: "* * * * *",
			dedupe_key: "scheduled::nightly::1",
		});
		const eventId = await db.invoke({
			task_key: "root-event",
			queue: "default",
			payload: { event: "user.created", payload: { id: 1 } },
		});
		await sql`
			update pgconductor._private_executions
			set subscription_id = ${crypto.randomUUID()}, parent_execution_id = ${crypto.randomUUID()}
			where id = ${eventId}
		`;
		const worker = new Worker(
			"default",
			[
				makeTask("root-cron", async () => undefined, {}, { cron: "* * * * *", name: "nightly" }),
				makeTask("root-event", async () => undefined),
			],
			db,
			logger,
			{ pollIntervalMs: 1, flushIntervalMs: 1 },
			{},
			new Telemetry(),
		);
		await worker.drain(orchestratorId);
		const processSpans = spans(exporter, "process default");
		expect(processSpans.map((span) => span.attributes["pgconductor.task.name"])).toEqual(
			expect.arrayContaining(["root-cron", "root-event"]),
		);
		for (const span of processSpans) expect(span.parentSpanId).toBeUndefined();
		await cleanup(provider);
	});

	test("malformed context starts a root span without recording payloads", async () => {
		const { exporter, provider } = installProvider();
		const { client: db } = await createDb();
		await db.invoke({
			task_key: "safe",
			queue: "default",
			payload: { secret: "do not record" },
			trace_context: { traceparent: "bad" } as any,
		});
		await makeWorker(
			db,
			makeTask("safe", async () => undefined),
		).drain(orchestratorId);
		const processSpan = spans(exporter, "process default")[0]!;
		expect(processSpan.parentSpanId).toBeUndefined();
		for (const span of exporter.getFinishedSpans()) {
			expect(span.attributes).not.toHaveProperty("payload");
			expect(JSON.stringify(span.attributes)).not.toContain("do not record");
		}
		await cleanup(provider);
	});
});

describe.serial("trace propagation across internal executions", () => {
	const processSpans = (exporter: InMemorySpanExporter, taskKey: string) =>
		exporter
			.getFinishedSpans()
			.filter(
				(span) =>
					span.name.startsWith("process ") && span.attributes["pgconductor.task.name"] === taskKey,
			);

	test("ctx.invoke children continue the parent's trace", async () => {
		const { exporter, provider } = installProvider();
		const db = await pool.child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "trace-parent" }),
				defineTask({ name: "trace-child" }),
			]),
			context: {},
		});
		const child = conductor.createTask(
			{ name: "trace-child" },
			{ invocable: true },
			async () => {},
		);
		const parent = conductor.createTask(
			{ name: "trace-parent" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.invoke("child", { name: "trace-child" });
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [parent, child],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await conductor.invoke({ name: "trace-parent" }, {});
		await waitForCondition(() => processSpans(exporter, "trace-parent").length === 2);
		await orchestrator.stop();

		const [parentSpan] = processSpans(exporter, "trace-parent");
		const [childSpan] = processSpans(exporter, "trace-child");
		expect(childSpan?.parentSpanId).toBe(parentSpan?.spanContext().spanId);
		expect(childSpan?.spanContext().traceId).toBe(parentSpan?.spanContext().traceId);
		await db.destroy();
		await cleanup(provider);
	}, 30000);

	test("emitted events continue the emitter's trace in triggered executions", async () => {
		const { exporter, provider } = installProvider();
		const db = await pool.child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([defineTask({ name: "trace-on-event" })]),
			events: EventSchemas.fromSchema([
				defineEvent({ name: "trace.event", payload: z.object({}) }),
			]),
			context: {},
		});
		const destination = conductor.createTask(
			{ name: "trace-on-event" },
			{ event: "trace.event" },
			async () => {},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [destination],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await conductor.emit("trace.event", {});
		await waitForCondition(() => processSpans(exporter, "trace-on-event").length === 1);
		await orchestrator.stop();

		const [send] = spans(exporter, "send pgconductor.internal");
		const [dispatch] = spans(exporter, "process pgconductor.internal");
		const [destinationSpan] = processSpans(exporter, "trace-on-event");
		expect(dispatch?.links).toHaveLength(1);
		expect(dispatch?.links[0]?.context.spanId).toBe(send?.spanContext().spanId);
		expect(destinationSpan?.parentSpanId).toBe(send?.spanContext().spanId);
		expect(destinationSpan?.spanContext().traceId).toBe(send?.spanContext().traceId);
		await db.destroy();
		await cleanup(provider);
	}, 30000);

	test("dead-letter deliveries continue the failed execution's trace", async () => {
		const { exporter, provider } = installProvider();
		const db = await pool.child();
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema([
				defineTask({ name: "trace-failing" }),
				defineTask({ name: "trace-dead-letter" }),
			]),
			context: {},
		});
		const deadLetter = conductor.createTask(
			{ name: "trace-dead-letter" },
			{ invocable: true },
			async () => {},
		);
		const failing = conductor.createTask(
			{ name: "trace-failing", maxAttempts: 1, deadLetter: { queue: "default", task: deadLetter } },
			{ invocable: true },
			async () => {
				throw new Error("boom");
			},
		);
		const orchestrator = Orchestrator.create({
			conductor,
			tasks: [failing, deadLetter],
			defaultWorker: { pollIntervalMs: 10, flushIntervalMs: 10 },
		});
		await orchestrator.start();
		await conductor.invoke({ name: "trace-failing" }, {});
		await waitForCondition(() => processSpans(exporter, "trace-dead-letter").length === 1);
		await orchestrator.stop();

		const [send] = spans(exporter, "send default");
		const [deadLetterSpan] = processSpans(exporter, "trace-dead-letter");
		expect(deadLetterSpan?.parentSpanId).toBe(send?.spanContext().spanId);
		expect(deadLetterSpan?.spanContext().traceId).toBe(send?.spanContext().traceId);
		await db.destroy();
		await cleanup(provider);
	}, 30000);
});
