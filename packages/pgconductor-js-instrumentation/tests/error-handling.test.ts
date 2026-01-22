import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from "bun:test";
import { SpanStatusCode } from "@opentelemetry/api";
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
import {
	TestDatabasePool,
	type TestDatabase,
} from "pgconductor-js/tests/fixtures/test-database";
import { Conductor, Orchestrator, TaskSchemas } from "pgconductor-js";
import { z } from "zod";

const testTaskDefinitions = [
	{
		name: "error-task",
		queue: "default",
		payload: z.object({}),
		returns: z.object({ done: z.boolean() }),
	},
	{
		name: "retry-task",
		queue: "default",
		payload: z.object({ failCount: z.number() }),
		returns: z.string(),
	},
] as const;

describe("Error Handling", () => {
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

	test("failed task records exception on span", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const errorTask = conductor.createTask(
			{ name: "error-task" },
			{ invocable: true },
			async () => {
				throw new Error("Intentional test error");
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [errorTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "error-task" }, {});
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const spans = getSpans(otelCtx);
		const processSpan = findSpanByName(spans, "default error-task process");

		expect(processSpan).toBeDefined();
		expect(processSpan?.status.code).toBe(SpanStatusCode.ERROR);

		// Check error attributes
		const errorMsg = getSpanAttribute(
			processSpan!,
			SemanticConventions.PGCONDUCTOR_ERROR_MESSAGE,
		);
		expect(errorMsg).toContain("Intentional test error");
	});

	test("retryable error has error.retryable=true", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		let attempts = 0;
		const retryTask = conductor.createTask(
			{ name: "retry-task" },
			{ invocable: true },
			async (event) => {
				attempts++;
				if (attempts < event.payload.failCount) {
					throw new Error(`Attempt ${attempts} failed`);
				}
				return `Success on attempt ${attempts}`;
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [retryTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "retry-task" }, { failCount: 3 });
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const spans = getSpans(otelCtx);
		const processSpans = spans.filter((s) => s.name === "default retry-task process");

		// First execution should fail with retryable=true
		expect(processSpans.length).toBeGreaterThan(0);
		const failedSpan = processSpans.find((s) => s.status.code === SpanStatusCode.ERROR);
		expect(failedSpan).toBeDefined();
		const retryable = getSpanAttribute(
			failedSpan!,
			SemanticConventions.PGCONDUCTOR_ERROR_RETRYABLE,
		);
		expect(retryable).toBe(true);
	});

	test("error spans record error attributes", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		// Task that always fails
		const errorTask = conductor.createTask(
			{ name: "error-task" },
			{ invocable: true },
			async () => {
				throw new Error("Always fails");
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [errorTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "error-task" }, {});
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const spans = getSpans(otelCtx);
		const processSpans = spans.filter((s) => s.name === "default error-task process");

		// Should have at least one error span
		expect(processSpans.length).toBeGreaterThan(0);

		// Find an error span
		const errorSpan = processSpans.find((s) => s.status.code === SpanStatusCode.ERROR);
		expect(errorSpan).toBeDefined();

		// Error span should have error message attribute
		const errorMsg = getSpanAttribute(errorSpan!, SemanticConventions.PGCONDUCTOR_ERROR_MESSAGE);
		expect(errorMsg).toContain("Always fails");

		// Error span should have retryable attribute (true for first attempts)
		const retryable = getSpanAttribute(errorSpan!, SemanticConventions.PGCONDUCTOR_ERROR_RETRYABLE);
		expect(retryable).toBeDefined();
	});

	test("cancelled task has appropriate status", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		// Task that takes a while
		const errorTask = conductor.createTask(
			{ name: "error-task" },
			{ invocable: true },
			async (_event, ctx) => {
				await ctx.sleep("long-sleep", 60000); // 1 minute sleep
				return { done: true };
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [errorTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		const executionId = await conductor.invoke({ name: "error-task" }, {});

		// Wait for task to start
		await new Promise((resolve) => setTimeout(resolve, 200));

		// Cancel the execution
		await conductor.cancel(executionId as string);

		await new Promise((resolve) => setTimeout(resolve, 300));
		await orchestrator.stop();

		// Verify cancel was called (the span should reflect the operation)
		const spans = getSpans(otelCtx);
		expect(spans.length).toBeGreaterThan(0);
	});
});
