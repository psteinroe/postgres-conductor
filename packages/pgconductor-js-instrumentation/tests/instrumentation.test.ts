import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from "bun:test";
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import {
	setupOtel,
	cleanupOtel,
	resetSpans,
	getSpans,
	findSpanByName,
	getSpanAttribute,
	type TestContext,
} from "./setup";
import * as SemanticConventions from "../src/semantic-conventions";
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
] as const;

describe("PgConductorInstrumentation", () => {
	let otelCtx: TestContext;
	let pool: TestDatabasePool;
	let db: TestDatabase;

	beforeAll(async () => {
		otelCtx = setupOtel();
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

	test("registers instrumentation without error", () => {
		expect(otelCtx.instrumentation).toBeDefined();
		expect(otelCtx.instrumentation.instrumentationName).toBe(
			SemanticConventions.INSTRUMENTATION_NAME,
		);
	});

	test("can be enabled and disabled", () => {
		otelCtx.instrumentation.disable();
		otelCtx.instrumentation.enable();
		expect(otelCtx.instrumentation).toBeDefined();
	});

	test("creates producer span with correct attributes", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		conductor.createTask({ name: "simple-task" }, { invocable: true }, async (event) => {
			if (event.name === "pgconductor.invoke") {
				return event.payload.value * 2;
			}
			return 0;
		});

		await conductor.ensureInstalled();

		await conductor.invoke({ name: "simple-task" }, { value: 42 });

		const spans = getSpans(otelCtx);
		const publishSpan = findSpanByName(spans, "default simple-task publish");

		expect(publishSpan).toBeDefined();
		expect(publishSpan?.kind).toBe(SpanKind.PRODUCER);
		expect(getSpanAttribute(publishSpan!, SemanticConventions.MESSAGING_SYSTEM)).toBe(
			SemanticConventions.MESSAGING_SYSTEM_VALUE,
		);
		expect(getSpanAttribute(publishSpan!, SemanticConventions.MESSAGING_OPERATION)).toBe(
			SemanticConventions.MESSAGING_OPERATION_PUBLISH,
		);
		expect(getSpanAttribute(publishSpan!, SemanticConventions.PGCONDUCTOR_TASK_NAME)).toBe(
			"simple-task",
		);
		expect(getSpanAttribute(publishSpan!, SemanticConventions.PGCONDUCTOR_TASK_QUEUE)).toBe(
			"default",
		);
	});

	test("sets execution ID on span", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		conductor.createTask({ name: "simple-task" }, { invocable: true }, async (event) => {
			if (event.name === "pgconductor.invoke") {
				return event.payload.value * 2;
			}
			return 0;
		});

		await conductor.ensureInstalled();

		const executionId = await conductor.invoke({ name: "simple-task" }, { value: 1 });

		const spans = getSpans(otelCtx);
		const publishSpan = findSpanByName(spans, "default simple-task publish");

		expect(publishSpan).toBeDefined();
		expect(getSpanAttribute(publishSpan!, SemanticConventions.PGCONDUCTOR_EXECUTION_ID)).toBe(
			executionId,
		);
	});

	test("sets span status to OK on success", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		conductor.createTask({ name: "simple-task" }, { invocable: true }, async (event) => {
			if (event.name === "pgconductor.invoke") {
				return event.payload.value * 2;
			}
			return 0;
		});

		await conductor.ensureInstalled();

		await conductor.invoke({ name: "simple-task" }, { value: 1 });

		const spans = getSpans(otelCtx);
		const publishSpan = findSpanByName(spans, "default simple-task publish");

		expect(publishSpan).toBeDefined();
		expect(publishSpan?.status.code).toBe(SpanStatusCode.OK);
	});

	test("creates consumer span with correct attributes", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const simpleTask = conductor.createTask(
			{ name: "simple-task" },
			{ invocable: true },
			async (event) => {
				if (event.name === "pgconductor.invoke") {
					return event.payload.value * 2;
				}
				return 0;
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
		await conductor.invoke({ name: "simple-task" }, { value: 5 });
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const spans = getSpans(otelCtx);
		const processSpan = findSpanByName(spans, "default simple-task process");

		expect(processSpan).toBeDefined();
		expect(processSpan?.kind).toBe(SpanKind.CONSUMER);
		expect(getSpanAttribute(processSpan!, SemanticConventions.MESSAGING_SYSTEM)).toBe(
			SemanticConventions.MESSAGING_SYSTEM_VALUE,
		);
		expect(getSpanAttribute(processSpan!, SemanticConventions.MESSAGING_OPERATION)).toBe(
			SemanticConventions.MESSAGING_OPERATION_PROCESS,
		);
	});

	test("sets execution status attribute", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const simpleTask = conductor.createTask(
			{ name: "simple-task" },
			{ invocable: true },
			async (event) => {
				if (event.name === "pgconductor.invoke") {
					return event.payload.value * 2;
				}
				return 0;
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
		await conductor.invoke({ name: "simple-task" }, { value: 10 });
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const spans = getSpans(otelCtx);
		const processSpan = findSpanByName(spans, "default simple-task process");

		expect(processSpan).toBeDefined();
		expect(getSpanAttribute(processSpan!, SemanticConventions.PGCONDUCTOR_EXECUTION_STATUS)).toBe(
			"completed",
		);
	});

	test("respects requireParentSpan config", async () => {
		// Create conductor with requireParentSpan
		const strictCtx = setupOtel({ requireParentSpan: true });

		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		conductor.createTask({ name: "simple-task" }, { invocable: true }, async (event) => {
			if (event.name === "pgconductor.invoke") {
				return event.payload.value * 2;
			}
			return 0;
		});

		await conductor.ensureInstalled();

		// Invoke without parent span - should not create span
		await conductor.invoke({ name: "simple-task" }, { value: 1 });

		const spans = getSpans(strictCtx);
		// With requireParentSpan=true, no span should be created without a parent
		expect(spans.length).toBe(0);

		await cleanupOtel(strictCtx);
	});
});
