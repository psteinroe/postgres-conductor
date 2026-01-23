import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from "bun:test";
import { SpanKind, trace, context } from "@opentelemetry/api";
import {
	setupOtel,
	cleanupOtel,
	resetSpans,
	getSpans,
	findSpanByName,
	type TestContext,
} from "./setup";
import { TestDatabasePool, type TestDatabase } from "./fixtures/test-database";
import { Conductor, Orchestrator, TaskSchemas } from "pgconductor-js";
import { z } from "zod";

const testTaskDefinitions = [
	{
		name: "simple-task",
		queue: "default",
		payload: z.object({ value: z.number() }),
		returns: z.number(),
	},
	{
		name: "parent-task",
		queue: "default",
		payload: z.object({ value: z.number() }),
		returns: z.number(),
	},
	{
		name: "child-task",
		queue: "default",
		payload: z.object({ value: z.number() }),
		returns: z.number(),
	},
] as const;

describe("Context Propagation", () => {
	let otelCtx: TestContext;
	let pool: TestDatabasePool;
	let db: TestDatabase;

	beforeAll(async () => {
		otelCtx = setupOtel({ propagateContext: true });
		pool = await TestDatabasePool.create();
	}, 60000);

	afterAll(async () => {
		await cleanupOtel(otelCtx);
		await pool?.destroy();
	});

	beforeEach(async () => {
		db = await pool.child();
	});

	afterEach(async () => {
		resetSpans(otelCtx);
		await db?.destroy();
	});

	test("invoke() creates producer span with trace context injection", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		conductor.createTask({ name: "simple-task" }, { invocable: true }, async (event) => {
			return event.payload.value * 2;
		});

		await conductor.ensureInstalled();

		// Create a parent span to test context propagation
		const tracer = trace.getTracer("test");
		const parentSpan = tracer.startSpan("test-parent");

		await context.with(trace.setSpan(context.active(), parentSpan), async () => {
			await conductor.invoke({ name: "simple-task" }, { value: 42 });
		});

		parentSpan.end();

		const spans = getSpans(otelCtx);
		const publishSpan = findSpanByName(spans, "default simple-task publish");

		expect(publishSpan).toBeDefined();
		expect(publishSpan?.kind).toBe(SpanKind.PRODUCER);

		// Verify the publish span has a valid trace ID
		expect(publishSpan?.spanContext().traceId).toBeDefined();
		expect(publishSpan?.spanContext().traceId.length).toBe(32);
	});

	test("execute() creates consumer span linked to producer", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const simpleTask = conductor.createTask(
			{ name: "simple-task" },
			{ invocable: true },
			async (event) => {
				return event.payload.value * 2;
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [simpleTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();

		// Invoke task
		await conductor.invoke({ name: "simple-task" }, { value: 42 });

		// Wait for processing
		await new Promise((resolve) => setTimeout(resolve, 500));

		await orchestrator.stop();

		const spans = getSpans(otelCtx);
		const publishSpan = findSpanByName(spans, "default simple-task publish");
		const processSpan = findSpanByName(spans, "default simple-task process");

		expect(publishSpan).toBeDefined();
		expect(processSpan).toBeDefined();

		// Consumer span should have links to producer span (not parent-child)
		const links = processSpan?.links || [];
		expect(links.length).toBeGreaterThan(0);

		// Verify the link points to the producer span's trace
		expect(links[0]?.context.traceId).toBe(publishSpan?.spanContext().traceId);
	});

	test("ctx.invoke() creates internal span for child invocation", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const childTask = conductor.createTask(
			{ name: "child-task" },
			{ invocable: true },
			async (event) => {
				return event.payload.value * 3;
			},
		);

		const parentTask = conductor.createTask(
			{ name: "parent-task" },
			{ invocable: true },
			async (event, ctx) => {
				const childResult = await ctx.invoke(
					"invoke-child",
					{ name: "child-task" },
					{ value: event.payload.value },
				);
				return childResult;
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [parentTask, childTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		// Set fake time to speed up child execution
		await db.client.setFakeTime({ date: new Date() });

		await orchestrator.start();
		await conductor.invoke({ name: "parent-task" }, { value: 10 });

		// Wait for parent to start and invoke child
		await new Promise((resolve) => setTimeout(resolve, 300));

		// Advance time to allow child to complete and parent to resume
		for (let i = 0; i < 5; i++) {
			await new Promise((resolve) => setTimeout(resolve, 100));
			const newTime = new Date(Date.now() + 1000 * (i + 1));
			await db.client.setFakeTime({ date: newTime });
		}

		await orchestrator.stop();
		await db.client.clearFakeTime();

		const spans = getSpans(otelCtx);

		// Find parent process span and the ctx.invoke internal span
		const parentProcessSpan = findSpanByName(spans, "default parent-task process");
		const invokeSpan = findSpanByName(spans, "parent-task invoke child-task");

		expect(parentProcessSpan).toBeDefined();
		expect(invokeSpan).toBeDefined();

		// The invoke span should be INTERNAL kind (internal operation within task)
		expect(invokeSpan!.kind).toBe(SpanKind.INTERNAL);

		// The invoke span should be part of the same trace as the parent process span
		expect(invokeSpan!.spanContext().traceId).toBe(parentProcessSpan!.spanContext().traceId);
	});

	test("no link created when propagateContext=false", async () => {
		// Create new context with propagation disabled
		const noPropCtx = setupOtel({ propagateContext: false });

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const simpleTask = conductor.createTask(
			{ name: "simple-task" },
			{ invocable: true },
			async (event) => {
				return event.payload.value * 2;
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [simpleTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "simple-task" }, { value: 42 });
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const spans = getSpans(noPropCtx);
		const processSpan = findSpanByName(spans, "default simple-task process");

		// When propagation is disabled, there should be no links
		const links = processSpan?.links || [];
		expect(links.length).toBe(0);

		await cleanupOtel(noPropCtx);
	});
});
