import { describe, test, expect, beforeAll, afterAll, beforeEach, afterEach } from "bun:test";
import { setupOtel, cleanupOtel, resetSpans, getMetrics, type TestContext } from "./setup";
import * as SemanticConventions from "../src/semantic-conventions";
import { TestDatabasePool, type TestDatabase } from "./fixtures/test-database";
import { Conductor, Orchestrator, TaskSchemas } from "pgconductor-js";
import { z } from "zod";
import type { DataPoint, SumMetricData, HistogramMetricData } from "@opentelemetry/sdk-metrics";

const testTaskDefinitions = [
	{
		name: "metric-task",
		queue: "default",
		payload: z.object({ value: z.number() }),
		returns: z.number(),
	},
	{
		name: "failing-task",
		queue: "default",
		payload: z.object({}),
		returns: z.void(),
	},
] as const;

function findMetric(
	collectionResult: Awaited<ReturnType<typeof getMetrics>>,
	name: string,
): SumMetricData | HistogramMetricData | undefined {
	const metrics = collectionResult.resourceMetrics;
	for (const scopeMetrics of metrics.scopeMetrics) {
		for (const metric of scopeMetrics.metrics) {
			if (metric.descriptor.name === name) {
				return metric as SumMetricData | HistogramMetricData;
			}
		}
	}
	return undefined;
}

function getDataPointValue(
	metric: SumMetricData | HistogramMetricData | undefined,
	attributeFilter?: Record<string, unknown>,
): number {
	if (!metric) return 0;

	const dataPoints = metric.dataPoints as DataPoint<number>[];

	for (const dp of dataPoints) {
		if (!attributeFilter) {
			return typeof dp.value === "number" ? dp.value : 0;
		}

		const attrs = dp.attributes;
		let match = true;
		for (const [key, value] of Object.entries(attributeFilter)) {
			if (attrs[key] !== value) {
				match = false;
				break;
			}
		}
		if (match) {
			return typeof dp.value === "number" ? dp.value : 0;
		}
	}

	return 0;
}

describe("Metrics", () => {
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

	test("task.invocations counter incremented on invoke", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		conductor.createTask({ name: "metric-task" }, { invocable: true }, async (event) => {
			return event.payload.value * 2;
		});

		await conductor.ensureInstalled();

		// Invoke multiple times
		await conductor.invoke({ name: "metric-task" }, { value: 1 });
		await conductor.invoke({ name: "metric-task" }, { value: 2 });
		await conductor.invoke({ name: "metric-task" }, { value: 3 });

		const metrics = await getMetrics(otelCtx);
		const invocationsMetric = findMetric(metrics, SemanticConventions.METRIC_TASK_INVOCATIONS);

		expect(invocationsMetric).toBeDefined();

		const count = getDataPointValue(invocationsMetric, {
			[SemanticConventions.PGCONDUCTOR_TASK_NAME]: "metric-task",
			[SemanticConventions.PGCONDUCTOR_TASK_QUEUE]: "default",
		});

		expect(count).toBe(3);
	});

	test("task.executions counter incremented with status", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const metricTask = conductor.createTask(
			{ name: "metric-task" },
			{ invocable: true },
			async (event) => {
				return event.payload.value * 2;
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [metricTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "metric-task" }, { value: 42 });
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const metrics = await getMetrics(otelCtx);
		const executionsMetric = findMetric(metrics, SemanticConventions.METRIC_TASK_EXECUTIONS);

		expect(executionsMetric).toBeDefined();

		const completedCount = getDataPointValue(executionsMetric, {
			[SemanticConventions.PGCONDUCTOR_TASK_NAME]: "metric-task",
			[SemanticConventions.PGCONDUCTOR_TASK_QUEUE]: "default",
			[SemanticConventions.PGCONDUCTOR_EXECUTION_STATUS]: "completed",
		});

		expect(completedCount).toBe(1);
	});

	test("task.duration histogram records execution time", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const metricTask = conductor.createTask(
			{ name: "metric-task" },
			{ invocable: true },
			async (event) => {
				// Small delay to ensure measurable duration
				await new Promise((resolve) => setTimeout(resolve, 10));
				return event.payload.value * 2;
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [metricTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "metric-task" }, { value: 100 });
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const metrics = await getMetrics(otelCtx);
		const durationMetric = findMetric(metrics, SemanticConventions.METRIC_TASK_DURATION);

		expect(durationMetric).toBeDefined();
		expect(durationMetric?.descriptor.unit).toBe("ms");

		// Histogram should have recorded at least one value
		const dataPoints = (durationMetric as HistogramMetricData)?.dataPoints || [];
		expect(dataPoints.length).toBeGreaterThan(0);
	});

	test("task.retries counter tracks failed attempts", async () => {
		const conductor = Conductor.create({
			sql: db.sql,
			tasks: TaskSchemas.fromSchema(testTaskDefinitions),
			context: {},
		});

		const failingTask = conductor.createTask(
			{ name: "failing-task" },
			{ invocable: true },
			async () => {
				throw new Error("Task failed");
			},
		);

		await conductor.ensureInstalled();

		const worker = conductor.createWorker({
			queue: "default",
			tasks: [failingTask],
			config: { pollIntervalMs: 50 },
		});

		const orchestrator = Orchestrator.create({
			conductor,
			workers: [worker],
		});

		await orchestrator.start();
		await conductor.invoke({ name: "failing-task" }, {});
		await new Promise((resolve) => setTimeout(resolve, 500));
		await orchestrator.stop();

		const metrics = await getMetrics(otelCtx);
		const retriesMetric = findMetric(metrics, SemanticConventions.METRIC_TASK_RETRIES);

		expect(retriesMetric).toBeDefined();

		// Should have at least one retry recorded
		const retryCount = getDataPointValue(retriesMetric, {
			[SemanticConventions.PGCONDUCTOR_TASK_NAME]: "failing-task",
			[SemanticConventions.PGCONDUCTOR_TASK_QUEUE]: "default",
		});

		expect(retryCount).toBeGreaterThanOrEqual(1);
	});
});
