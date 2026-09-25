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

export type PersistedTraceContext = {
	readonly parent?: TraceContextCarrier;
	readonly link?: TraceContextCarrier;
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

type SendTraceOptions<T> = Omit<MessageTraceOptions<T>, "name" | "kind" | "operation"> & {
	name?: string;
};

type ProcessTraceOptions<T> = {
	taskKey: string;
	queue: string;
	messageId?: string;
	batchMessageCount?: number;
	traceStates: readonly (PersistedTraceContext | null | undefined)[];
	run: (span: TelemetrySpan) => Promise<T> | T;
};

export type DurableMessage = {
	key: string;
	queue: string;
	taskKey: string;
	parent?: TraceContextCarrier | null;
};

type SettleTraceOptions = {
	queue: string;
	batchMessageCount: number;
	deliveries: readonly DurableMessage[];
	run: (
		traceStates: Readonly<Record<string, PersistedTraceContext | null>>,
	) => Promise<ReadonlySet<string>> | ReadonlySet<string>;
};

type ProduceOptions<T> = {
	messages: readonly DurableMessage[];
	run: (traceStates: Readonly<Record<string, PersistedTraceContext | null>>) => Promise<T> | T;
	isAccepted?: (result: T, message: DurableMessage) => boolean;
};

export class TelemetrySpan {
	constructor(
		private readonly span: Span | null,
		private readonly injectionContext?: Context,
	) {}

	traceContext(): TraceContextCarrier | null {
		if (!this.span && !this.injectionContext) return null;
		try {
			const carrier: Record<string, unknown> = {};
			const injectionContext =
				this.injectionContext ||
				(this.span ? trace.setSpan(context.active(), this.span) : ROOT_CONTEXT);
			propagation.inject(injectionContext, carrier);
			const { traceparent, tracestate } = carrier;
			if (
				typeof traceparent !== "string" ||
				traceparent.length === 0 ||
				Buffer.byteLength(traceparent) > MAX_TRACE_HEADER_LENGTH
			) {
				return null;
			}
			return {
				traceparent,
				...(typeof tracestate === "string" &&
				Buffer.byteLength(tracestate) <= MAX_TRACE_HEADER_LENGTH
					? { tracestate }
					: {}),
			};
		} catch {
			return null;
		}
	}

	persistedTraceContext(): PersistedTraceContext | null {
		const parent = this.traceContext();
		return parent ? { parent } : null;
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

	private message<T>({
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

	send<T>({ name, queue, ...options }: SendTraceOptions<T>): Promise<T> {
		return this.message({
			...options,
			name: name ?? `send ${queue}`,
			kind: SpanKind.PRODUCER,
			queue,
			operation: "send",
		});
	}

	process<T>({
		taskKey,
		queue,
		messageId,
		batchMessageCount,
		traceStates,
		run,
	}: ProcessTraceOptions<T>): Promise<T> {
		const isBatch = batchMessageCount !== undefined;
		const linkedContexts = traceStates.flatMap((state) =>
			(isBatch ? [state?.parent, state?.link] : [state?.link]).map((carrier) =>
				this.extractTraceContext(carrier),
			),
		);
		const links = this.links(
			linkedContexts.map((linkedContext) => {
				const spanContext = trace.getSpanContext(linkedContext);
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
			parent: isBatch ? ROOT_CONTEXT : this.extractTraceContext(traceStates[0]?.parent),
			links,
			run: (span) => run(span),
		});
	}

	settle({
		queue,
		batchMessageCount,
		deliveries,
		run,
	}: SettleTraceOptions): Promise<ReadonlySet<string>> {
		return this.produce({
			messages: deliveries,
			isAccepted: (accepted, message) => accepted.has(message.key),
			run: (traceStates) =>
				this.message({
					name: `settle ${queue}`,
					kind: SpanKind.CLIENT,
					queue,
					operation: "settle",
					batchMessageCount,
					parent: ROOT_CONTEXT,
					run: () => run(traceStates),
				}),
		});
	}

	async produce<T>({ messages, run, isAccepted }: ProduceOptions<T>): Promise<T> {
		const uniqueMessages = Array.from(
			new Map(messages.map((message) => [message.key, message])).values(),
		);
		if (!this.enabled) {
			return run(Object.fromEntries(uniqueMessages.map(({ key }) => [key, null] as const)));
		}

		const pending = uniqueMessages.map((message) => {
			const parent = this.extractTraceContext(message.parent);
			const span = trace.getTracer(INSTRUMENTATION_NAME).startSpan(
				`send ${message.queue}`,
				{
					kind: SpanKind.PRODUCER,
					attributes: {
						"messaging.system": "postgres_conductor",
						"messaging.destination.name": message.queue,
						"messaging.operation.name": "send",
						"messaging.operation.type": "send",
						"pgconductor.task.name": message.taskKey,
					},
				},
				parent,
			);
			return { message, span, telemetrySpan: new TelemetrySpan(span) };
		});
		const traceStateByMessageKey = Object.fromEntries(
			pending.map(
				({ message, telemetrySpan }) =>
					[message.key, telemetrySpan.persistedTraceContext()] as const,
			),
		);

		try {
			const result = await run(traceStateByMessageKey);
			if (isAccepted) {
				for (const { message, span } of pending) {
					if (!isAccepted(result, message)) {
						span.setStatus({
							code: SpanStatusCode.ERROR,
							message: "Delivery was not persisted",
						});
					}
				}
			}
			return result;
		} catch (error) {
			for (const { telemetrySpan } of pending) telemetrySpan.recordError(error);
			throw error;
		} finally {
			for (const { span } of pending) span.end();
		}
	}

	traceContext(): TraceContextCarrier | null {
		if (!this.enabled) return null;
		try {
			const activeContext = context.active();
			return new TelemetrySpan(trace.getSpan(activeContext) || null, activeContext).traceContext();
		} catch {
			return null;
		}
	}

	extractTraceContext(carrier: unknown): Context {
		if (!this.enabled || !carrier || typeof carrier !== "object") return ROOT_CONTEXT;
		const traceparent = "traceparent" in carrier ? carrier.traceparent : undefined;
		if (
			typeof traceparent !== "string" ||
			traceparent.length === 0 ||
			Buffer.byteLength(traceparent) > MAX_TRACE_HEADER_LENGTH
		) {
			return ROOT_CONTEXT;
		}
		const tracestate = "tracestate" in carrier ? carrier.tracestate : undefined;
		const boundedCarrier = {
			traceparent,
			...(typeof tracestate === "string" && Buffer.byteLength(tracestate) <= MAX_TRACE_HEADER_LENGTH
				? { tracestate }
				: {}),
		};
		try {
			return propagation.extract(ROOT_CONTEXT, boundedCarrier);
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
