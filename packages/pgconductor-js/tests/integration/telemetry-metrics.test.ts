import { afterEach, describe, expect, test } from "bun:test";
import { metrics } from "@opentelemetry/api";
import { MeterProvider, MetricReader } from "@opentelemetry/sdk-metrics";
import { Conductor } from "../../src/conductor";
import { Task } from "../../src/task";
import { Worker } from "../../src/worker";
import { DefaultLogger } from "../../src/lib/logger";
import { InMemoryDatabaseClient } from "../mocks/in-memory-database-client";
import { resetMetricsForTests } from "../../src/telemetry";

const logger = new DefaultLogger();

// sdk-metrics 1.x calls this MetricReader; keep the test's reader manual and
// synchronous while remaining compatible with the version supported here.
class ManualMetricReader extends MetricReader {
	protected async onShutdown(): Promise<void> {}
	protected async onForceFlush(): Promise<void> {}
}

const fakeSql = Object.assign((async () => []) as any, {
	json: (value: unknown) => JSON.stringify(value),
});

let provider: MeterProvider | undefined;

function installMetrics() {
	metrics.disable();
	resetMetricsForTests();
	const reader = new ManualMetricReader();
	provider = new MeterProvider({ readers: [reader] });
	expect(metrics.setGlobalMeterProvider(provider)).toBe(true);
	return reader;
}

async function collect(reader: ManualMetricReader) {
	await provider?.forceFlush();
	return (await reader.collect()).resourceMetrics;
}

async function cleanupMetrics() {
	await provider?.shutdown();
	provider = undefined;
	metrics.disable();
	resetMetricsForTests();
}

afterEach(async () => {
	await cleanupMetrics();
});

function makeTask(
	name: string,
	execute: (event: any, context: any) => Promise<any>,
	config: Record<string, unknown> = {},
) {
	return Task.create({ name, ...config } as any, { invocable: true } as any, execute as any);
}

function makeWorker(db: InMemoryDatabaseClient, tasks: any[], telemetry = true) {
	return new Worker(
		"default",
		tasks,
		db,
		logger,
		{
			pollIntervalMs: 1,
			flushIntervalMs: 1,
			fetchBatchSize: 10,
			flushBatchSize: 10,
		},
		{},
		[],
		telemetry,
	);
}

function allMetrics(resourceMetrics: any) {
	return resourceMetrics.scopeMetrics.flatMap((scope: any) => scope.metrics);
}

function metric(resourceMetrics: any, name: string): any {
	return allMetrics(resourceMetrics).find((candidate: any) => candidate.descriptor.name === name);
}

function metricValue(resourceMetrics: any, name: string, attributes: Record<string, unknown> = {}) {
	const candidate = metric(resourceMetrics, name);
	if (!candidate) return 0;
	return candidate.dataPoints
		.filter((point: any) =>
			Object.entries(attributes).every(([key, value]) => point.attributes[key] === value),
		)
		.reduce(
			(total: number, point: any) =>
				total +
				Number(
					typeof point.value === "number" ? point.value : (point.value?.sum ?? point.sum ?? 0),
				),
			0,
		);
}

function lifecycleTotal(resourceMetrics: any) {
	return [
		"pgconductor.execution.retries",
		"pgconductor.execution.permanent_failures",
		"pgconductor.execution.cancellations",
		"pgconductor.execution.dead_letters",
	].reduce((total, name) => total + metricValue(resourceMetrics, name), 0);
}

describe.serial("OpenTelemetry metrics instrumentation", () => {
	test("counts accepted invokes but not throttled no-ops", async () => {
		const reader = installMetrics();
		const db = new InMemoryDatabaseClient();
		const conductor = Conductor.create({ sql: fakeSql, context: {} });
		(conductor as any).db = db;

		const task = { name: "invoke-metrics" };
		expect(await (conductor as any).invoke(task, {}, { throttle: { seconds: 60 } })).toBeTruthy();
		expect(await (conductor as any).invoke(task, {}, { throttle: { seconds: 60 } })).toBeNull();

		const resourceMetrics = await collect(reader);
		expect(metricValue(resourceMetrics, "messaging.client.sent.messages")).toBe(1);
	});

	test("records worker consumption, handler/process duration, and settle duration", async () => {
		const reader = installMetrics();
		const db = new InMemoryDatabaseClient();
		const task = makeTask("worker-metrics", async () => undefined);
		await db.invoke({ task_key: task.name, queue: "default", payload: {} });
		await makeWorker(db, [task]).drain("metrics-worker");

		const resourceMetrics = await collect(reader);
		expect(metricValue(resourceMetrics, "messaging.client.consumed.messages")).toBe(1);
		expect(metricValue(resourceMetrics, "messaging.process.duration")).toBeGreaterThan(0);
		expect(
			metricValue(resourceMetrics, "messaging.client.operation.duration", {
				"messaging.operation.name": "settle",
			}),
		).toBeGreaterThan(0);
	});

	test("records retry and permanent failure only after returnExecutions commits", async () => {
		const reader = installMetrics();
		const db = new InMemoryDatabaseClient();
		const task = makeTask(
			"lifecycle-metrics",
			async () => {
				throw new Error("always fails");
			},
			{ maxAttempts: 2 },
		);
		await db.invoke({ task_key: task.name, queue: "default", payload: {} });

		let entered!: () => void;
		const returnEntered = new Promise<void>((resolve) => (entered = resolve));
		let release!: () => void;
		const commit = new Promise<void>((resolve) => (release = resolve));
		const originalReturn = db.returnExecutions.bind(db);
		(db as any).returnExecutions = async (results: any, options: any) => {
			entered();
			await commit;
			return originalReturn(results, options);
		};

		const firstDrain = makeWorker(db, [task]).drain("metrics-worker");
		await returnEntered;
		let resourceMetrics = await collect(reader);
		expect(lifecycleTotal(resourceMetrics)).toBe(0);

		release();
		await firstDrain;
		resourceMetrics = await collect(reader);
		expect(metricValue(resourceMetrics, "pgconductor.execution.retries")).toBe(1);

		db.advanceTime(16_000);
		await makeWorker(db, [task]).drain("metrics-worker-2");
		resourceMetrics = await collect(reader);
		expect(metricValue(resourceMetrics, "pgconductor.execution.retries")).toBe(1);
		expect(metricValue(resourceMetrics, "pgconductor.execution.permanent_failures")).toBe(1);
	});

	test("failed returnExecutions records no lifecycle metrics", async () => {
		const reader = installMetrics();
		const db = new InMemoryDatabaseClient();
		const task = makeTask("settlement-failure", async () => undefined);
		await db.invoke({ task_key: task.name, queue: "default", payload: {} });
		(db as any).returnExecutions = async () => {
			throw new Error("settlement failed");
		};

		await makeWorker(db, [task]).drain("metrics-worker");
		const resourceMetrics = await collect(reader);
		expect(lifecycleTotal(resourceMetrics)).toBe(0);
	});

	test("records cancellation and final failure plus DLQ lifecycle counts", async () => {
		const reader = installMetrics();
		const db = new InMemoryDatabaseClient();
		const dlqTask = makeTask("metrics-dlq", async () => undefined);
		const cancelledTask = makeTask("metrics-cancelled", async () => undefined, {
			maxAttempts: 1,
		});
		const failedTask = makeTask(
			"metrics-dead-letter",
			async () => {
				throw new Error("terminal failure");
			},
			{ maxAttempts: 1, deadLetter: { queue: "default", task: dlqTask } },
		);
		const cancelledId = await db.invoke({
			task_key: cancelledTask.name,
			queue: "default",
			payload: {},
		});
		await db.invoke({
			task_key: failedTask.name,
			queue: "default",
			payload: {},
		});
		await db.cancelExecution(cancelledId!, { reason: "cancelled for metrics" });

		await makeWorker(db, [cancelledTask, failedTask, dlqTask]).drain("metrics-worker");
		const resourceMetrics = await collect(reader);
		expect(metricValue(resourceMetrics, "pgconductor.execution.cancellations")).toBe(1);
		expect(metricValue(resourceMetrics, "pgconductor.execution.permanent_failures")).toBe(1);
		expect(metricValue(resourceMetrics, "pgconductor.execution.dead_letters")).toBe(1);
		expect(
			metric(resourceMetrics, "pgconductor.execution.permanent_failures")?.dataPoints.some(
				(point: any) =>
					point.attributes["pgconductor.task.name"] === failedTask.name &&
					point.attributes["pgconductor.outcome"] === "permanent_failure",
			),
		).toBe(true);
	});

	test("telemetry false records no metrics for conductor or worker", async () => {
		const reader = installMetrics();
		const db = new InMemoryDatabaseClient();
		const conductor = Conductor.create({
			sql: fakeSql,
			context: {},
			telemetry: false,
		});
		(conductor as any).db = db;
		const task = makeTask("telemetry-disabled", async () => undefined);
		const id = await conductor.invoke(task, {} as any);
		expect(id).toBeTruthy();
		await makeWorker(db, [task], false).drain("metrics-worker");

		expect(allMetrics(await collect(reader))).toHaveLength(0);
	});

	test("uses seconds for plausible duration values and safe metric attributes", async () => {
		const reader = installMetrics();
		const db = new InMemoryDatabaseClient();
		const task = makeTask("attribute-metrics", async () => undefined);
		await db.invoke({
			task_key: task.name,
			queue: "default",
			payload: { secret: "no metric" },
		});
		await makeWorker(db, [task]).drain("metrics-worker");
		const resourceMetrics = await collect(reader);
		const allowed = new Set([
			"messaging.system",
			"messaging.destination.name",
			"messaging.operation.name",
			"messaging.operation.type",
			"pgconductor.task.name",
			"pgconductor.outcome",
		]);

		for (const candidate of allMetrics(resourceMetrics)) {
			for (const point of candidate.dataPoints) {
				expect(Object.keys(point.attributes).every((key) => allowed.has(key))).toBe(true);
				expect(point.attributes["messaging.system"]).toBe("postgres_conductor");
				expect(point.attributes["messaging.destination.name"]).toBe("default");
				expect(point.attributes["messaging.operation.name"]).toBeTruthy();
				if (candidate.descriptor.name.includes("duration")) {
					expect(candidate.descriptor.unit).toBe("s");
					const duration = Number(
						typeof point.value === "number" ? point.value : (point.value?.sum ?? point.sum ?? 0),
					);
					expect(duration).toBeGreaterThan(0);
					expect(duration).toBeLessThan(10);
				}
			}
		}
		const processPoint = metric(resourceMetrics, "messaging.process.duration")?.dataPoints[0];
		expect(processPoint.attributes["pgconductor.task.name"]).toBe(task.name);
		expect(JSON.stringify(resourceMetrics)).not.toContain("secret");
	});
});
