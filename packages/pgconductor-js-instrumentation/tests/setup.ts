import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
import {
	InMemorySpanExporter,
	SimpleSpanProcessor,
	type ReadableSpan,
} from "@opentelemetry/sdk-trace-base";
import {
	MeterProvider,
	MetricReader,
	type ResourceMetrics,
	type CollectionResult,
	AggregationTemporality,
	InstrumentType,
} from "@opentelemetry/sdk-metrics";
import { PgConductorInstrumentation, type PgConductorInstrumentationConfig } from "../src";
import * as pgconductorJs from "pgconductor-js";

/**
 * Simple in-memory metric reader for testing.
 */
class TestMetricReader extends MetricReader {
	protected override onForceFlush(): Promise<void> {
		return Promise.resolve();
	}

	protected override onShutdown(): Promise<void> {
		return Promise.resolve();
	}

	override selectAggregationTemporality(_instrumentType: InstrumentType): AggregationTemporality {
		return AggregationTemporality.CUMULATIVE;
	}
}

export interface TestContext {
	spanExporter: InMemorySpanExporter;
	metricReader: TestMetricReader;
	instrumentation: PgConductorInstrumentation;
	tracerProvider: NodeTracerProvider;
	meterProvider: MeterProvider;
}

/**
 * Set up OpenTelemetry SDK for testing.
 * Manually patches pgconductor-js modules since Bun doesn't support Node.js module hooks.
 */
export function setupOtel(config?: PgConductorInstrumentationConfig): TestContext {
	const spanExporter = new InMemorySpanExporter();
	const metricReader = new TestMetricReader();

	const tracerProvider = new NodeTracerProvider();
	tracerProvider.addSpanProcessor(new SimpleSpanProcessor(spanExporter));
	tracerProvider.register();

	const meterProvider = new MeterProvider({
		readers: [metricReader],
	});

	const instrumentation = new PgConductorInstrumentation(config);
	instrumentation.setTracerProvider(tracerProvider);
	instrumentation.setMeterProvider(meterProvider);
	instrumentation.enable();
	instrumentation.initMetrics();

	// Manually patch modules for Bun compatibility
	// Node.js module hooking doesn't work in Bun
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	instrumentation.patchModule(pgconductorJs as any);

	return {
		spanExporter,
		metricReader,
		instrumentation,
		tracerProvider,
		meterProvider,
	};
}

/**
 * Clean up OpenTelemetry SDK.
 */
export async function cleanupOtel(ctx: TestContext): Promise<void> {
	// Manually unpatch modules
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	ctx.instrumentation.unpatchModule(pgconductorJs as any);
	ctx.instrumentation.disable();
	await ctx.tracerProvider.shutdown();
	await ctx.meterProvider.shutdown();
}

/**
 * Reset span exporter for next test.
 */
export function resetSpans(ctx: TestContext): void {
	ctx.spanExporter.reset();
}

/**
 * Get finished spans.
 */
export function getSpans(ctx: TestContext): ReadableSpan[] {
	return ctx.spanExporter.getFinishedSpans();
}

/**
 * Get metrics.
 */
export async function getMetrics(ctx: TestContext): Promise<CollectionResult> {
	return await ctx.metricReader.collect();
}

/**
 * Find span by name.
 */
export function findSpanByName(spans: ReadableSpan[], name: string): ReadableSpan | undefined {
	return spans.find((s) => s.name === name);
}

/**
 * Find spans by name pattern.
 */
export function findSpansByPattern(spans: ReadableSpan[], pattern: RegExp): ReadableSpan[] {
	return spans.filter((s) => pattern.test(s.name));
}

/**
 * Get attribute value from span.
 */
export function getSpanAttribute(
	span: ReadableSpan,
	key: string,
): string | number | boolean | undefined {
	const attr = span.attributes[key];
	if (attr === undefined) return undefined;
	if (Array.isArray(attr)) return attr[0] as string | number | boolean;
	return attr as string | number | boolean;
}
