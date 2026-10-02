import {
	context,
	propagation,
	ROOT_CONTEXT,
	SpanKind,
	SpanStatusCode,
	trace,
	type Attributes,
	type Context,
	type Link,
	type Span,
} from "@opentelemetry/api";

export type TraceContextCarrier = {
	readonly traceparent: string;
	readonly tracestate?: string;
};

type TraceOptions<T> = {
	name: string;
	kind: SpanKind;
	attributes?: Attributes;
	parent?: Context;
	links?: Link[];
	run: (span?: Span) => Promise<T> | T;
};

type SendOptions<T> = {
	taskKey: string;
	queue: string;
	batchMessageCount?: number;
	run: (span?: Span) => Promise<T>;
};

type ProcessOptions<T> = {
	taskKey: string;
	queue: string;
	messageId?: string;
	batchMessageCount?: number;
	traceContexts: (TraceContextCarrier | null | undefined)[];
	run: () => Promise<T>;
};

type SettleOptions<T> = {
	queue: string;
	batchMessageCount: number;
	run: () => Promise<T>;
};

const messaging = (queue: string, operation: string): Attributes => ({
	"messaging.system": "postgres_conductor",
	"messaging.destination.name": queue,
	"messaging.operation.name": operation,
	"messaging.operation.type": operation,
});

export class Telemetry {
	constructor(private readonly enabled = true) {}

	async trace<T>({
		name,
		parent = context.active(),
		run,
		...options
	}: TraceOptions<T>): Promise<T> {
		if (!this.enabled) return run();

		return trace
			.getTracer("pgconductor-js")
			.startActiveSpan(name, options, parent, async (span) => {
				try {
					return await run(span);
				} catch (error) {
					span.recordException(error instanceof Error ? error : String(error));
					span.setAttribute(
						"error.type",
						error instanceof Error ? error.constructor.name : "_OTHER",
					);
					span.setStatus({ code: SpanStatusCode.ERROR });
					throw error;
				} finally {
					span.end();
				}
			});
	}

	traceContext(): TraceContextCarrier | null {
		if (!this.enabled) return null;
		const carrier: Record<string, string> = {};
		propagation.inject(context.active(), carrier);
		const { traceparent, tracestate } = carrier;
		return traceparent ? { traceparent, tracestate } : null;
	}

	send<T>({ taskKey, queue, batchMessageCount, run }: SendOptions<T>): Promise<T> {
		return this.trace({
			name: `send ${queue}`,
			kind: SpanKind.PRODUCER,
			attributes: {
				...messaging(queue, "send"),
				"pgconductor.task.name": taskKey,
				"messaging.batch.message_count": batchMessageCount,
			},
			run,
		});
	}

	process<T>({
		taskKey,
		queue,
		messageId,
		batchMessageCount,
		traceContexts,
		run,
	}: ProcessOptions<T>): Promise<T> {
		const carriers = new Map(traceContexts.flatMap((c) => (c ? [[c.traceparent, c]] : [])));
		const parents = [...carriers.values()].map((c) => propagation.extract(ROOT_CONTEXT, c));
		const links = parents.flatMap((parent) => {
			const spanContext = trace.getSpanContext(parent);
			return spanContext ? [{ context: spanContext }] : [];
		});

		return this.trace({
			name: `process ${queue}`,
			kind: SpanKind.CONSUMER,
			attributes: {
				...messaging(queue, "process"),
				"pgconductor.task.name": taskKey,
				"messaging.message.id": messageId,
				"messaging.batch.message_count": batchMessageCount,
			},
			parent: batchMessageCount === undefined ? parents[0] || ROOT_CONTEXT : ROOT_CONTEXT,
			links,
			run,
		});
	}

	settle<T>({ queue, batchMessageCount, run }: SettleOptions<T>): Promise<T> {
		return this.trace({
			name: `settle ${queue}`,
			kind: SpanKind.CLIENT,
			attributes: {
				...messaging(queue, "settle"),
				"messaging.batch.message_count": batchMessageCount,
			},
			parent: ROOT_CONTEXT,
			run,
		});
	}
}
