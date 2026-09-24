import {
	context,
	propagation,
	ROOT_CONTEXT,
	SpanStatusCode,
	trace,
	SpanKind,
	type Attributes,
	type Context,
	type Link,
	type Span,
	type SpanContext,
} from "@opentelemetry/api";

export type TraceContextCarrier = {
	readonly traceparent: string;
	readonly tracestate?: string;
};

const INSTRUMENTATION_NAME = "pgconductor-js";
const MAX_TRACE_HEADER_LENGTH = 512;

type TraceOptions<T> = {
	name: string;
	kind: SpanKind;
	attributes?: Attributes;
	parent?: Context;
	links?: Link[];
	run: (span: TelemetrySpan) => Promise<T> | T;
};

type MessageTraceOptions<T> = Omit<TraceOptions<T>, "attributes"> & {
	taskKey?: string;
	queue: string;
	operation: "send" | "process" | "settle";
	messageId?: string;
	batchMessageCount?: number;
};

type ProcessTraceOptions<T> = {
	taskKey: string;
	queue: string;
	messageId?: string;
	batchMessageCount?: number;
	traceContexts: readonly (TraceContextCarrier | null | undefined)[];
	run: () => Promise<T> | T;
};

type SettleTraceOptions<T> = {
	queue: string;
	batchMessageCount: number;
	run: () => Promise<T> | T;
};

export class TelemetrySpan {
	constructor(private readonly span: Span | null) {}

	traceContext(): TraceContextCarrier | null {
		if (!this.span) return null;
		try {
			const carrier: Record<string, string> = {};
			propagation.inject(trace.setSpan(context.active(), this.span), carrier);
			const { traceparent, tracestate } = carrier;
			if (!traceparent || traceparent.length > MAX_TRACE_HEADER_LENGTH) return null;
			return {
				traceparent,
				...(tracestate && tracestate.length <= MAX_TRACE_HEADER_LENGTH ? { tracestate } : {}),
			};
		} catch {
			return null;
		}
	}

	setAttribute(key: string, value: string | number | boolean): void {
		this.span?.setAttribute(key, value);
	}

	recordError(error: unknown): void {
		if (!this.span) return;
		const exception = error instanceof Error ? error : new Error(String(error));
		const errorType =
			error instanceof Error && error.constructor.name ? error.constructor.name : "_OTHER";
		this.span.recordException(exception);
		this.span.setAttribute("error.type", errorType);
		this.span.setStatus({ code: SpanStatusCode.ERROR });
	}
}

const NOOP_SPAN = new TelemetrySpan(null);

export class Telemetry {
	constructor(private readonly enabled = true) {}

	trace<T>({ name, kind, attributes, parent, links, run }: TraceOptions<T>): Promise<T> {
		if (!this.enabled) return Promise.resolve().then(() => run(NOOP_SPAN));

		const parentContext = parent ?? context.active();
		const span = trace
			.getTracer(INSTRUMENTATION_NAME)
			.startSpan(name, { kind, attributes, links }, parentContext);
		const telemetrySpan = new TelemetrySpan(span);
		const spanContext = trace.setSpan(parentContext, span);

		return Promise.resolve()
			.then(() => context.with(spanContext, () => run(telemetrySpan)))
			.catch((error) => {
				telemetrySpan.recordError(error);
				throw error;
			})
			.finally(() => span.end());
	}

	message<T>({
		taskKey,
		queue,
		operation,
		messageId,
		batchMessageCount,
		...options
	}: MessageTraceOptions<T>): Promise<T> {
		return this.trace({
			...options,
			attributes: {
				"messaging.system": "postgres_conductor",
				"messaging.destination.name": queue,
				"messaging.operation.name": operation,
				"messaging.operation.type": operation,
				...(taskKey === undefined ? {} : { "pgconductor.task.name": taskKey }),
				...(messageId === undefined ? {} : { "messaging.message.id": messageId }),
				...(batchMessageCount === undefined
					? {}
					: { "messaging.batch.message_count": batchMessageCount }),
			},
		});
	}

	process<T>({
		taskKey,
		queue,
		messageId,
		batchMessageCount,
		traceContexts,
		run,
	}: ProcessTraceOptions<T>): Promise<T> {
		const parentContexts = traceContexts.map((carrier) => this.extractTraceContext(carrier));
		const links = this.links(
			parentContexts.map((parent) => {
				const spanContext = trace.getSpanContext(parent);
				return spanContext && trace.isSpanContextValid(spanContext) ? spanContext : null;
			}),
		);

		return this.message({
			name: `process ${queue}`,
			kind: SpanKind.CONSUMER,
			taskKey,
			queue,
			operation: "process",
			messageId,
			batchMessageCount,
			parent: batchMessageCount === undefined ? parentContexts[0] : ROOT_CONTEXT,
			links,
			run: () => run(),
		});
	}

	settle<T>({ queue, batchMessageCount, run }: SettleTraceOptions<T>): Promise<T> {
		return this.message({
			name: `settle ${queue}`,
			kind: SpanKind.CLIENT,
			queue,
			operation: "settle",
			batchMessageCount,
			parent: ROOT_CONTEXT,
			run: () => run(),
		});
	}

	extractTraceContext(carrier?: TraceContextCarrier | null): Context {
		if (!this.enabled || !carrier) return ROOT_CONTEXT;
		try {
			return propagation.extract(ROOT_CONTEXT, carrier);
		} catch {
			return ROOT_CONTEXT;
		}
	}

	private links(contexts: Iterable<SpanContext | null | undefined>): Link[] {
		if (!this.enabled) return [];
		const links = new Map<string, Link>();
		for (const spanContext of contexts) {
			if (!spanContext) continue;
			links.set(`${spanContext.traceId}:${spanContext.spanId}`, { context: spanContext });
		}
		return [...links.values()];
	}
}
