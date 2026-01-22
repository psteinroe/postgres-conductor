import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from "bun:test";
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import {
	setupOtel,
	cleanupOtel,
	resetSpans,
	getSpans,
	findSpanByName,
	findSpansByPattern,
	getSpanAttribute,
	type TestContext,
} from "./setup";
import * as SemanticConventions from "../src/semantic-conventions";
import { TestDatabasePool, type TestDatabase } from "pgconductor-js/tests/fixtures/test-database";
import { Conductor, Orchestrator, TaskSchemas } from "pgconductor-js";
import { z } from "zod";

const testTaskDefinitions = [
	{
		name: "step-task",
		queue: "default",
		payload: z.object({ input: z.string() }),
		returns: z.string(),
	},
	{
		name: "sleep-task",
		queue: "default",
		payload: z.object({}),
		returns: z.object({ completed: z.boolean() }),
	},
] as const;

describe("Step Spans", () => {
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

	test("step() creates span with step.name attribute", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const stepTask = conductor.createTask(
			{ name: "step-task" },
			{ invocable: true },
			async (event, ctx) => {
				const result = await ctx.step("process-input", async () => {
					return `processed: ${event.payload.input}`;
				});
				return result;
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [stepTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "step-task" }, { input: "hello" });
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const spans = getSpans(otelCtx);
		const stepSpan = findSpanByName(spans, "step-task step process-input");

		expect(stepSpan).toBeDefined();
		expect(stepSpan?.kind).toBe(SpanKind.INTERNAL);
		expect(getSpanAttribute(stepSpan!, SemanticConventions.PGCONDUCTOR_STEP_NAME)).toBe(
			"process-input",
		);
		expect(getSpanAttribute(stepSpan!, SemanticConventions.PGCONDUCTOR_TASK_NAME)).toBe(
			"step-task",
		);
	});

	test("uncached step has step.cached=false", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const stepTask = conductor.createTask(
			{ name: "step-task" },
			{ invocable: true },
			async (event, ctx) => {
				const result = await ctx.step("first-run", async () => {
					return `result: ${event.payload.input}`;
				});
				return result;
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [stepTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "step-task" }, { input: "test" });
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const spans = getSpans(otelCtx);
		const stepSpan = findSpanByName(spans, "step-task step first-run");

		expect(stepSpan).toBeDefined();
		// On first execution, step is not cached
		expect(getSpanAttribute(stepSpan!, SemanticConventions.PGCONDUCTOR_STEP_CACHED)).toBe(false);
	});

	test("sleep() creates span with step.name attribute", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const sleepTask = conductor.createTask(
			{ name: "sleep-task" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.sleep("wait-period", 100);
				return { completed: true };
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [sleepTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "sleep-task" }, {});
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const spans = getSpans(otelCtx);
		const sleepSpan = findSpanByName(spans, "sleep-task sleep");

		expect(sleepSpan).toBeDefined();
		expect(sleepSpan?.kind).toBe(SpanKind.INTERNAL);
		expect(getSpanAttribute(sleepSpan!, SemanticConventions.PGCONDUCTOR_STEP_NAME)).toBe(
			"wait-period",
		);
	});

	test("nested steps create nested spans", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const stepTask = conductor.createTask(
			{ name: "step-task" },
			{ invocable: true },
			async (event, ctx) => {
				const step1 = await ctx.step("step-1", async () => {
					return `first: ${event.payload.input}`;
				});
				const step2 = await ctx.step("step-2", async () => {
					return `second: ${step1}`;
				});
				return step2;
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [stepTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "step-task" }, { input: "nested" });
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const spans = getSpans(otelCtx);
		const stepSpans = findSpansByPattern(spans, /step-task step/);

		expect(stepSpans.length).toBe(2);

		const step1Span = findSpanByName(spans, "step-task step step-1");
		const step2Span = findSpanByName(spans, "step-task step step-2");

		expect(step1Span).toBeDefined();
		expect(step2Span).toBeDefined();
	});

	test("step error recorded on span", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const stepTask = conductor.createTask(
			{ name: "step-task" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.step("error-step", async () => {
					throw new Error("Step failed");
				});
				return "never reached";
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [stepTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "step-task" }, { input: "will-fail" });
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const spans = getSpans(otelCtx);
		const stepSpan = findSpanByName(spans, "step-task step error-step");

		expect(stepSpan).toBeDefined();
		expect(stepSpan?.status.code).toBe(SpanStatusCode.ERROR);
		expect(stepSpan?.status.message).toContain("Step failed");
	});
});
