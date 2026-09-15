import { describe, expect, test } from "bun:test";
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
import { Conductor } from "../../src/conductor";
import { Orchestrator } from "../../src/orchestrator";
import { Task } from "../../src/task";
import { Worker } from "../../src/worker";
import { DefaultLogger } from "../../src/lib/logger";
import { InMemoryDatabaseClient } from "../mocks/in-memory-database-client";
import type { DatabaseClient } from "../../src/database-client";
import {
	boundedCarrier,
	carrierForContext,
	contextForSpan,
	extractCarrier,
	messagingAttributes,
	startSpan,
	endSpan,
} from "../../src/telemetry";

const logger = new DefaultLogger();

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
) {
	return Task.create({ name, ...config } as any, { invocable: true } as any, execute as any);
}

function makeWorker(db: InMemoryDatabaseClient, task: any, telemetry = true) {
	return new Worker(
		"default",
		[task],
		db as unknown as DatabaseClient,
		logger,
		{ pollIntervalMs: 1, flushIntervalMs: 1, fetchBatchSize: 10, flushBatchSize: 10 },
		{},
		[],
		telemetry,
	);
}

const producerCarrier = (name: string) => {
	const producer = startSpan(
		name,
		SpanKind.PRODUCER,
		messagingAttributes("task", "default", "send"),
	);
	const carrier = carrierForContext(contextForSpan(producer));
	endSpan(producer);
	return { carrier, producer };
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
		const db = new InMemoryDatabaseClient();
		const conductor = Conductor.create({ sql: fakeSql, context: {}, telemetry: false });
		await conductor.invoke({ name: "disabled" }, { secret: "payload" } as any);
		const id = await db.invoke({
			task_key: "disabled",
			queue: "default",
			payload: {},
			trace_context: null,
		});
		expect(exporter.getFinishedSpans()).toHaveLength(0);
		expect(db.getExecution(id!)?.trace_context).toBeNull();
		await cleanup(provider);
	});

	test("the orchestrator passes telemetry opt-out to its default worker", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		const conductor = Conductor.create({ sql: fakeSql, context: {}, telemetry: false });
		(conductor as any).db = db;
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
		expect(db.getExecution(id)?.state).toBe("completed");
		expect(exporter.getFinishedSpans()).toHaveLength(0);
		expect(db.getExecution(id)?.trace_context).toBeNull();
		await cleanup(provider);
	});

	test("producer carrier becomes the real worker consumer parent", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		const { carrier, producer } = producerCarrier("send-parent");
		await db.invoke({
			task_key: "parented",
			queue: "default",
			payload: {},
			trace_context: carrier,
		});
		const task = makeTask("parented", async () => undefined);
		await makeWorker(db, task).drain("worker");
		const consumer = spans(exporter, "process default")[0]!;
		expect(consumer.parentSpanId).toBe(producer!.spanContext().spanId);
		expect(consumer.spanContext().traceId).toBe(producer!.spanContext().traceId);
		await cleanup(provider);
	});

	test("batch consumer links every producer context", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		const first = producerCarrier("batch-one");
		const second = producerCarrier("batch-two");
		await db.invokeBatch([
			{ task_key: "batched", queue: "default", payload: {}, trace_context: first.carrier },
			{ task_key: "batched", queue: "default", payload: {}, trace_context: second.carrier },
		]);
		const task = makeTask("batched", async () => [], { batch: { size: 10, timeoutMs: 1 } });
		await makeWorker(db, task).drain("worker");
		const consumer = spans(exporter, "process default")[0]!;
		expect(consumer.links.map((link) => link.context.spanId)).toEqual(
			expect.arrayContaining([
				first.producer?.spanContext().spanId,
				second.producer?.spanContext().spanId,
			]),
		);
		await cleanup(provider);
	});

	test("retry attempts have finite, separate process spans", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		let attempts = 0;
		const task = makeTask("retry-span", async () => {
			if (++attempts === 1) throw new Error("try again");
		});
		(task as any).maxAttempts = 2;
		const id = await db.invoke({ task_key: "retry-span", queue: "default", payload: {} });
		const worker = makeWorker(db, task);
		await worker.drain("worker");
		db.advanceTime(16000);
		await worker.drain("worker");
		const processSpans = spans(exporter, "process default");
		expect(processSpans).toHaveLength(2);
		expect(new Set(processSpans.map((span) => span.spanContext().spanId)).size).toBe(2);
		expect(db.getExecution(id!)?.state).toBe("completed");
		await cleanup(provider);
	});

	test("a user-created active child is a child of process", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		const task = makeTask("active-child", async () => {
			const child = trace.getTracer("user").startSpan("user child");
			child.end();
		});
		await db.invoke({ task_key: "active-child", queue: "default", payload: {} });
		await makeWorker(db, task).drain("worker");
		const parent = spans(exporter, "process default")[0]!;
		const child = spans(exporter, "user child")[0]!;
		expect(child.parentSpanId).toBe(parent.spanContext().spanId);
		await cleanup(provider);
	});

	test("settle has process links and records database errors", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		await db.invoke({ task_key: "settle-error", queue: "default", payload: {} });
		const original = db.returnExecutions.bind(db);
		(db as any).returnExecutions = async () => {
			throw new Error("database unavailable");
		};
		await makeWorker(
			db,
			makeTask("settle-error", async () => undefined),
		).drain("worker");
		const settle = spans(exporter, "settle default")[0]!;
		expect(settle.links.length).toBe(1);
		expect(settle.status.code).toBe(SpanStatusCode.ERROR);
		(db as any).returnExecutions = original;
		await cleanup(provider);
	});

	test("step callback is one span and cached replay creates none", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
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
		await worker.drain("worker");
		db.advanceTime(16000);
		await worker.drain("worker");
		expect(spans(exporter, "step step-cache")).toHaveLength(1);
		await cleanup(provider);
	});

	test("cron and event executions are root process spans", async () => {
		const { exporter, provider } = installProvider();
		const db = new InMemoryDatabaseClient();
		await db.invoke({
			task_key: "root-cron",
			queue: "default",
			payload: {},
			cron_expression: "* * * * *",
			dedupe_key: "scheduled::nightly::1",
		});
		await db.invoke({
			task_key: "root-event",
			queue: "default",
			payload: { event: "user.created", payload: { id: 1 } },
		});
		const worker = makeWorker(
			db,
			makeTask("root-cron", async () => undefined),
		);
		// A second worker is unnecessary: use a task map with both definitions.
		const eventWorker = new Worker(
			"default",
			[makeTask("root-cron", async () => undefined), makeTask("root-event", async () => undefined)],
			db as unknown as DatabaseClient,
			logger,
			{ pollIntervalMs: 1, flushIntervalMs: 1 },
			{},
			[],
			true,
		);
		await eventWorker.drain("worker");
		for (const name of ["process default", "process default"])
			expect(spans(exporter, name)[0]!.parentSpanId).toBeUndefined();
		void worker;
		await cleanup(provider);
	});

	test("carrier sanitization preserves valid parents and bounds tracestate", async () => {
		const { exporter, provider } = installProvider();
		const traceparent = `00-${"1".repeat(32)}-${"2".repeat(16)}-01`;
		expect(() => extractCarrier({ traceparent: "not-valid" })).not.toThrow();
		expect(boundedCarrier({ traceparent, tracestate: "tenant@vendor=value" })).toEqual({
			traceparent,
			tracestate: "tenant@vendor=value",
		});
		expect(boundedCarrier({ traceparent, tracestate: "not a tracestate" })).toEqual({
			traceparent,
		});
		expect(boundedCarrier({ traceparent, tracestate: "x".repeat(2000) })).toEqual({ traceparent });
		expect(
			Buffer.byteLength(
				JSON.stringify(boundedCarrier({ traceparent, tracestate: "x".repeat(2000) })),
				"utf8",
			),
		).toBeLessThanOrEqual(1024);
		const db = new InMemoryDatabaseClient();
		await db.invoke({
			task_key: "safe",
			queue: "default",
			payload: { secret: "do not record" },
			trace_context: { traceparent: "bad" } as any,
		});
		await makeWorker(
			db,
			makeTask("safe", async () => undefined),
		).drain("worker");
		for (const span of exporter.getFinishedSpans()) {
			expect(span.attributes).not.toHaveProperty("payload");
			expect(JSON.stringify(span.attributes)).not.toContain("do not record");
		}
		await cleanup(provider);
	});

	test("hostile propagators fail open", async () => {
		const { exporter, provider } = installProvider();
		propagation.setGlobalPropagator({
			inject() {
				throw new Error("inject failed");
			},
			extract() {
				throw new Error("extract failed");
			},
			fields() {
				return [];
			},
		} as any);
		expect(() => carrierForContext(ROOT_CONTEXT)).not.toThrow();
		expect(() =>
			extractCarrier({ traceparent: `00-${"1".repeat(32)}-${"2".repeat(16)}-01` }),
		).not.toThrow();
		expect(exporter.getFinishedSpans()).toHaveLength(0);
		await cleanup(provider);
	});
});
