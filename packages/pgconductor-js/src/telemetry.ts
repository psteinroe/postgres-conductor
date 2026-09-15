import {
	context,
	propagation,
	ROOT_CONTEXT,
	SpanKind,
	SpanStatusCode,
	trace,
	metrics,
	type Counter,
	type Histogram,
	type Context,
	type Link,
	type Span,
	type SpanContext,
} from "@opentelemetry/api";
import type { TraceContextCarrier } from "./internal-types";

const meter = () =>
	safe(() => metrics.getMeter(INSTRUMENTATION_NAME, INSTRUMENTATION_VERSION), null);

export type { TraceContextCarrier } from "./internal-types";

const TRACECONTEXT_SIZE_LIMIT = 1024;
const TRACEPARENT = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;
// W3C simple keys and multi-tenant vendor keys (for example tenant@vendor).
const TRACESTATE_KEY =
	/^(?:[a-z][a-z0-9_*/-]{0,255}|[a-z0-9][a-z0-9_*/-]{0,240}@[a-z][a-z0-9_*/-]{0,13})$/;
const TRACESTATE_VALUE = /^[\x20-\x2b\x2d-\x3c\x3e-\x7e]{1,256}$/;
const INSTRUMENTATION_NAME = "pgconductor-js";
const INSTRUMENTATION_VERSION = "0.1.0";
const ZERO_TRACE_ID = "00000000000000000000000000000000";
const ZERO_SPAN_ID = "0000000000000000";

type MessagingAttributes = Record<string, string | number | boolean>;

let instruments: {
	sent: Counter;
	consumed: Counter;
	processDuration: Histogram;
	operationDuration: Histogram;
	retry: Counter;
	permanentFailure: Counter;
	cancellation: Counter;
	deadLetter: Counter;
} | null = null;
let instrumentProvider: unknown = null;

function getInstruments() {
	const provider = safe(() => metrics.getMeterProvider(), null);
	// The API returns a no-op provider before an SDK is installed. Do not cache
	// instruments created from it: applications commonly install their provider
	// after constructing a Conductor.
	if (instruments && instrumentProvider === provider) return instruments;
	const m = meter();
	if (!m) return null;
	instrumentProvider = provider;
	return (instruments = {
		sent: m.createCounter("messaging.client.sent.messages", { unit: "{message}" }),
		consumed: m.createCounter("messaging.client.consumed.messages", { unit: "{message}" }),
		processDuration: m.createHistogram("messaging.process.duration", { unit: "s" }),
		operationDuration: m.createHistogram("messaging.client.operation.duration", { unit: "s" }),
		retry: m.createCounter("pgconductor.execution.retries", { unit: "{execution}" }),
		permanentFailure: m.createCounter("pgconductor.execution.permanent_failures", {
			unit: "{execution}",
		}),
		cancellation: m.createCounter("pgconductor.execution.cancellations", { unit: "{execution}" }),
		deadLetter: m.createCounter("pgconductor.execution.dead_letters", { unit: "{execution}" }),
	});
}

const OUTCOMES = new Set(["retry", "permanent_failure", "cancellation", "dead_letter"]);
const boundedDimension = (value: string | undefined): string | undefined =>
	value === undefined ? undefined : value.length <= 128 ? value : value.slice(0, 128);

function metricAttributes(queue: string, task?: string, operation?: string, outcome?: string) {
	return {
		"messaging.system": "postgres_conductor",
		"messaging.destination.name": boundedDimension(queue) || "unknown",
		"messaging.operation.name": boundedDimension(operation) || "unknown",
		"messaging.operation.type": boundedDimension(operation) || "unknown",
		...(task === undefined || boundedDimension(task) === undefined
			? {}
			: { "pgconductor.task.name": boundedDimension(task)! }),
		...(outcome !== undefined && OUTCOMES.has(outcome) ? { "pgconductor.outcome": outcome } : {}),
	};
}

export function recordSent(queue: string, count = 1, task?: string): void {
	safe(() => getInstruments()?.sent.add(count, metricAttributes(queue, task, "send")), undefined);
}
export function recordConsumed(queue: string, task?: string, count = 1): void {
	safe(
		() => getInstruments()?.consumed.add(count, metricAttributes(queue, task, "process")),
		undefined,
	);
}
export function recordProcessDuration(queue: string, durationMs: number, task?: string): void {
	safe(
		() =>
			getInstruments()?.processDuration.record(
				durationMs / 1000,
				metricAttributes(queue, task, "process"),
			),
		undefined,
	);
}
export function recordOperationDuration(
	queue: string,
	durationMs: number,
	operation: string,
	task?: string,
): void {
	safe(
		() =>
			getInstruments()?.operationDuration.record(
				durationMs / 1000,
				metricAttributes(queue, task, operation),
			),
		undefined,
	);
}
export function recordLifecycle(
	queue: string,
	outcome: "retry" | "permanent_failure" | "cancellation" | "dead_letter",
	task?: string,
	count = 1,
): void {
	safe(() => {
		const i = getInstruments();
		const counter =
			outcome === "retry"
				? i?.retry
				: outcome === "permanent_failure"
					? i?.permanentFailure
					: outcome === "cancellation"
						? i?.cancellation
						: i?.deadLetter;
		counter?.add(count, metricAttributes(queue, task, "settle", outcome));
	}, undefined);
}

export function resetMetricsForTests(): void {
	instruments = null;
	instrumentProvider = null;
}

type MessagingOperation = "send" | "process" | "settle";

export const messagingAttributes = (
	taskKey: string | undefined,
	queue: string,
	operation: MessagingOperation,
	messageId?: string,
	batchMessageCount?: number,
	eventKey?: string,
): MessagingAttributes => ({
	"messaging.system": "postgres_conductor",
	"messaging.destination.name": queue,
	"messaging.operation.name": operation,
	"messaging.operation.type": operation,
	...(taskKey === undefined ? {} : { "pgconductor.task.name": taskKey }),
	...(messageId === undefined ? {} : { "messaging.message.id": messageId }),
	...(batchMessageCount === undefined
		? {}
		: { "messaging.batch.message_count": batchMessageCount }),
	...(eventKey === undefined ? {} : { "pgconductor.event.name": boundedDimension(eventKey) }),
	"pgconductor.queue": queue,
});

export const stepAttributes = (
	taskKey: string,
	queue: string,
	stepKey: string,
): MessagingAttributes => ({
	"pgconductor.task.name": taskKey,
	"pgconductor.step.name": stepKey,
	"pgconductor.queue": queue,
});

function safe<T>(fn: () => T, fallback: T): T {
	try {
		return fn();
	} catch {
		return fallback;
	}
}

function property(value: object, key: string): unknown {
	return safe(() => Object.getOwnPropertyDescriptor(value, key)?.value, undefined);
}

function isValidTraceparent(value: unknown): value is string {
	return safe(() => {
		if (typeof value !== "string" || !TRACEPARENT.test(value)) return false;
		const match = TRACEPARENT.exec(value);
		const traceId = match?.[1];
		const spanId = match?.[2];
		const traceFlags = match?.[3];
		return (
			!!traceId &&
			!!spanId &&
			!!traceFlags &&
			trace.isSpanContextValid({
				traceId,
				spanId,
				traceFlags: Number.parseInt(traceFlags, 16),
			})
		);
	}, false);
}

function isValidTracestate(value: unknown): value is string {
	return safe(() => {
		if (typeof value !== "string" || value.length > 512) return false;
		const members = value.split(",").map((member) => member.trim());
		if (members.length > 32 || members.some((member) => member.length === 0)) return false;
		const keys = new Set<string>();
		for (const member of members) {
			const separator = member.indexOf("=");
			if (separator <= 0) return false;
			const key = member.slice(0, separator);
			const memberValue = member.slice(separator + 1);
			if (!TRACESTATE_KEY.test(key) || !TRACESTATE_VALUE.test(memberValue) || keys.has(key))
				return false;
			keys.add(key);
		}
		return true;
	}, false);
}

function carrierSize(traceparent: string, tracestate?: string): number {
	return Buffer.byteLength(
		JSON.stringify({ traceparent, ...(tracestate === undefined ? {} : { tracestate }) }),
		"utf8",
	);
}

/** Strictly validate a carrier without sanitizing malformed tracestate. */
export function isValidCarrier(value: unknown): value is TraceContextCarrier {
	return safe(() => {
		if (!value || typeof value !== "object") return false;
		const traceparent = property(value, "traceparent");
		const tracestate = property(value, "tracestate");
		return (
			isValidTraceparent(traceparent) &&
			(tracestate === undefined || isValidTracestate(tracestate)) &&
			carrierSize(traceparent, typeof tracestate === "string" ? tracestate : undefined) <=
				TRACECONTEXT_SIZE_LIMIT
		);
	}, false);
}

/** Sanitize a value read from the database before giving it to OTel or persisting it. */
export function boundedCarrier(value: unknown): TraceContextCarrier | null {
	if (!value || typeof value !== "object") return null;
	const traceparent = property(value, "traceparent");
	if (!isValidTraceparent(traceparent)) return null;

	const candidateTracestate = property(value, "tracestate");
	const tracestate =
		typeof candidateTracestate === "string" &&
		isValidTracestate(candidateTracestate) &&
		carrierSize(traceparent, candidateTracestate) <= TRACECONTEXT_SIZE_LIMIT
			? candidateTracestate
			: undefined;
	return { traceparent, ...(tracestate === undefined ? {} : { tracestate }) };
}

export function extractCarrier(value: unknown): Context | null {
	const carrier = boundedCarrier(value);
	if (!carrier) return null;
	return safe(() => propagation.extract(ROOT_CONTEXT, carrier), null);
}

export function startSpan(
	name: string,
	kind: SpanKind,
	attributes: MessagingAttributes,
	parent?: Context | null,
	links: Link[] = [],
): Span | null {
	return safe(
		() =>
			trace.getTracer(INSTRUMENTATION_NAME, INSTRUMENTATION_VERSION).startSpan(
				name,
				{
					kind,
					attributes,
					links,
				},
				// null is intentional: an absent carrier is a true root, rather than
				// accidentally inheriting a caller's active span.
				parent === undefined ? context.active() : parent || ROOT_CONTEXT,
			),
		null,
	);
}

export function linkContext(ctx: Context | null): Link | null {
	if (!ctx) return null;
	const value = safe(() => trace.getSpanContext(ctx), null);
	return value && value.traceId !== ZERO_TRACE_ID && value.spanId !== ZERO_SPAN_ID
		? { context: value }
		: null;
}

export function linkFromCarrier(value: unknown): Link | null {
	return linkContext(extractCarrier(value));
}

export function linksFromCarrier(value: unknown): Link[] {
	const link = linkFromCarrier(value);
	return link ? [link] : [];
}

export function spanContext(span: Span | null): SpanContext | null {
	if (!span) return null;
	return safe(() => {
		const value = span.spanContext();
		return value.traceId !== ZERO_TRACE_ID && value.spanId !== ZERO_SPAN_ID ? value : null;
	}, null);
}

export function contextForSpan(span: Span | null, parent: Context = context.active()): Context {
	return span ? safe(() => trace.setSpan(parent, span), parent) : parent;
}

export function contextForSpanContext(value: SpanContext | null): Context {
	return value ? safe(() => trace.setSpanContext(ROOT_CONTEXT, value), ROOT_CONTEXT) : ROOT_CONTEXT;
}

export function runWithSpan<T>(span: Span | null, fn: () => T): T {
	if (!span) return fn();
	const active = safe(() => context.active(), ROOT_CONTEXT);
	const scoped = safe(() => trace.setSpan(active, span), active);
	let callbackStarted = false;
	try {
		return context.with(scoped, () => {
			callbackStarted = true;
			return fn();
		});
	} catch (error) {
		// A broken context manager may throw before invoking the callback. Only then
		// retry without a scope; retrying after start would run user work twice.
		if (callbackStarted) throw error;
		return fn();
	}
}

export function endSpan(span: Span | null, error?: unknown): void {
	if (!span) return;
	if (error) setSpanError(span, error);
	safe(() => span.end(), undefined);
}

export function setSpanError(span: Span | null, error: unknown): void {
	if (!span) return;
	safe(() => {
		span.recordException(error instanceof Error ? error : new Error(String(error)));
		span.setStatus({ code: SpanStatusCode.ERROR });
	}, undefined);
}

export function setSpanAttribute(
	span: Span | null,
	key: string,
	value: string | number | boolean,
): void {
	if (!span) return;
	safe(() => span.setAttribute(key, value), undefined);
}

export function carrierForContext(ctx: Context = context.active()): TraceContextCarrier | null {
	return safe(() => {
		const carrier: Record<string, string> = {};
		propagation.inject(ctx, carrier);
		return boundedCarrier(carrier);
	}, null);
}

export const telemetryConstants = {
	instrumentationName: INSTRUMENTATION_NAME,
	instrumentationVersion: INSTRUMENTATION_VERSION,
};
