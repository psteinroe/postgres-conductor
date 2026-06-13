import type { Span } from "@opentelemetry/api";
import type { InstrumentationConfig } from "@opentelemetry/instrumentation";

/**
 * Configuration options for PgConductorInstrumentation.
 */
export interface PgConductorInstrumentationConfig extends InstrumentationConfig {
	/**
	 * If true, spans will only be created if there is an active parent span.
	 * @default false
	 */
	requireParentSpan?: boolean;

	/**
	 * If true, trace context will be propagated through executions.
	 * This enables linking producer spans (invoke) to consumer spans (process).
	 * @default true
	 */
	propagateContext?: boolean;

	/**
	 * Hook called before a producer span is created (invoke/emit).
	 * Return false to skip span creation.
	 * @param span - The span being created
	 * @param taskName - The task name being invoked
	 * @param payload - The task payload
	 */
	produceHook?: (span: Span, taskName: string, payload: unknown) => void | false;

	/**
	 * Hook called before a consumer span is created (process).
	 * Return false to skip span creation.
	 * @param span - The span being created
	 * @param execution - The execution being processed
	 */
	consumeHook?: (
		span: Span,
		execution: {
			id: string;
			task_key: string;
			queue: string;
			payload: unknown;
		},
	) => void | false;

	/**
	 * Hook called when a step span is created.
	 * @param span - The span being created
	 * @param name - The step name
	 * @param cached - Whether the step result was cached
	 */
	stepHook?: (span: Span, name: string, cached: boolean) => void;
}

/**
 * Trace context stored in executions table.
 */
export interface TraceContext {
	traceparent?: string;
	tracestate?: string;
}

/**
 * Task invocation info for producer spans.
 */
export interface TaskInvocationInfo {
	taskName: string;
	queue: string;
	payload?: unknown;
	executionId?: string;
}

/**
 * Execution info for consumer spans.
 */
export interface ExecutionInfo {
	id: string;
	task_key: string;
	queue: string;
	payload: unknown;
	attempt?: number;
	trace_context?: TraceContext | null;
}

/**
 * Step execution info for step spans.
 */
export interface StepInfo {
	name: string;
	executionId: string;
	taskKey: string;
	cached: boolean;
}
