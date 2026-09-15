/** The serializable W3C carrier persisted with an execution. */
export type TraceContextCarrier = {
	readonly [key: string]: string | undefined;
	readonly traceparent: string;
	readonly tracestate?: string;
};
