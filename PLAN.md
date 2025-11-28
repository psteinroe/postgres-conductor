# OTEL Integration Plan

## Overview
Goal: add OpenTelemetry-ready hooks to `pgconductor-js` so users can enable tracing/metrics/log correlation without pulling OTEL into the core runtime. The core surfaces a lightweight `telemetry` option; an optional `@pgconductor/opentelemetry` package populates that option with real tracers/meters/propagators.

Key outcomes:
- Distributed traces span enqueue → PostgreSQL persistence → worker fetch → task execution → flush → child invocations.
- Metrics cover queue depth, fetch/flush durations, execution attempts, and retry delays.
- Logs can be correlated via trace/span ids if users opt in to the provided logger.

## Schema + Type Updates
To persist trace context between enqueue and execution we store it alongside each execution row.

```sql
alter table pgconductor._private_executions
    add column trace_context jsonb;

create index on pgconductor._private_executions ((trace_context ->> 'traceparent'));
```

```ts
// pgconductor-js/src/database-client.ts
export interface ExecutionSpec {
    task_key: string;
    queue: string;
    payload?: Payload | null;
    trace_context?: Payload | null; // serialized carrier
    // ... existing fields
}

export interface Execution {
    id: string;
    task_key: string;
    queue: string;
    payload: Payload;
    trace_context: Payload | null;
    // ... existing fields
}
```

`DatabaseClient.invoke()` writes `trace_context`, worker fetch queries read it back, and `TaskContext` propagates it when invoking child tasks.

## Telemetry Hook Surface
Expose an optional `telemetry?: TelemetryHooks` in `ConductorOptions`. Hooks stay intentionally small so different telemetry packages can plug in while the core remains dependency-free.

```ts
// pgconductor-js/src/telemetry.ts
import type { Context, Span } from "@opentelemetry/api";

export type TelemetryEvent =
	| { type: "worker.fetch"; queue: string; count: number; durationMs: number }
	| { type: "worker.flush"; queue: string; count: number; durationMs: number }
	| {
			type: "worker.execution";
			queue: string;
			task: string;
			executionId: string;
			durationMs: number;
			attempt: number;
			status: "completed" | "failed" | "released" | "permanently_failed" | "invoke_child" | "wait";
			error?: Error;
	  }
	| { type: "db.query"; label?: string; durationMs: number; retries: number };

export interface TelemetryHooks {
	trace?<T>(name: string, attrs: Record<string, string>, fn: (span?: Span) => Promise<T>): Promise<T>;
	propagation?: {
		inject(carrier: Record<string, string>): void;
		extract(carrier?: Record<string, string> | null): Context | undefined;
	};
	emitEvent?: (event: TelemetryEvent) => void;
}
```

No additional worker-event callbacks are needed—the runtime creates spans and records metrics inline whenever hooks are present. This keeps the API compact and makes behavior obvious: **if a `tracer`/`metrics` instrument exists, we use it; otherwise we skip instrumentation**.

## Instrumentation Points

### Conductor invoke/emit/cancel

```ts
async invoke(task, payload, opts) {
    const carrier: Record<string, string> = {};
    this.telemetry?.propagation?.inject?.(carrier);

return this.telemetry?.trace
	? this.telemetry.trace(
		"conductor.invoke",
		{
			"pgconductor.task": task.name,
			"pgconductor.queue": queue,
		},
		async (span) => {
			const id = await this.db.invoke({ ...spec, trace_context: carrier });
			span?.setAttribute("pgconductor.execution_id", id);
			return id;
		},
	)
	: this.db.invoke({ ...spec, trace_context: carrier });
}
```

### DatabaseClient.query

```ts
private async query<T>(queryOrCallback: PendingQuery<T> | ((sql: Sql) => Promise<T>), options?: QueryOptions) {
    const start = performance.now();
	let retries = 0;
	const result = await (typeof queryOrCallback === "function"
		? queryOrCallback(this.sql)
		: queryOrCallback);
	this.telemetry?.emitEvent?.({
		type: "db.query",
		label: options?.label,
		durationMs: performance.now() - start,
		retries,
	});
	return result;
}
```

For richer DB spans, the OTEL package can wrap `TelemetryHooks.tracer.startActiveSpan` around the `DatabaseClient` itself; the core only needs to expose duration metrics.

### Worker pipeline

```ts
// fetch
const fetchStart = performance.now();
const executions = await this.db.getExecutions(...);
this.telemetry?.emitEvent?.({
	type: "worker.fetch",
	queue: this.queueName,
	count: executions.length,
	durationMs: performance.now() - fetchStart,
});

// execute
await (this.telemetry?.trace
	? this.telemetry.trace(
		`task:${execution.task_key}`,
		{
			"pgconductor.execution_id": execution.id,
			"pgconductor.queue": execution.queue,
		},
		async () => {
			const ctx = this.telemetry?.propagation?.extract?.(execution.trace_context || undefined);
			return context.with(ctx ?? context.active(), async () => {
				const attemptStart = performance.now();
				const result = await runTask();
				this.telemetry?.emitEvent?.({
					type: "worker.execution",
					queue: execution.queue,
					task: execution.task_key,
					executionId: execution.id,
					durationMs: performance.now() - attemptStart,
					attempt: execution.attempts + 1,
					status: result.status,
					error: result.status === "failed" ? new Error(result.error) : undefined,
				});
				return result;
			});
		},
	)
	: runTask());

// flush
const flushStart = performance.now();
await this.db.flush(buffer);
this.telemetry?.emitEvent?.({
	type: "worker.flush",
	queue: this.queueName,
	count: buffer.count,
	durationMs: performance.now() - flushStart,
});
```

### TaskContext helpers

```ts
async step(name, fn) {
    if (!this.telemetry?.tracer) return baseStep(name, fn);
    return this.telemetry.tracer.startActiveSpan(`task.step:${name}`, async (span) => {
        const result = await baseStep(name, fn);
        span.end();
        return result;
    });
}

async invoke(key, task, payload) {
    const carrier: Record<string, string> = {};
    this.telemetry?.propagation?.inject?.(carrier);
    // persists context in child spec
    return baseInvoke(key, task, payload, carrier);
}
```

## `@pgconductor/opentelemetry` Package
This package wires real OTEL primitives into the hook surface:

```ts
// packages/pgconductor-opentelemetry/src/index.ts
import { context, propagation, trace, metrics } from "@opentelemetry/api";

export function createPgConductorTelemetry(): TelemetryHooks {
    const tracer = trace.getTracer("pgconductor");
    const meter = metrics.getMeter("pgconductor");

    return {
        tracer,
        propagation: {
            inject: (carrier) => propagation.inject(context.active(), carrier),
            extract: (carrier) => (carrier ? propagation.extract(context.active(), carrier) : undefined),
        },
        metrics: {
            workerFetchDuration: meter.createHistogram("pgconductor.worker.fetch.duration", {
                unit: "ms",
            }),
            workerExecutionsFetched: meter.createCounter("pgconductor.worker.fetch.count"),
            workerFlushDuration: meter.createHistogram("pgconductor.worker.flush.duration", { unit: "ms" }),
            workerFlushCounts: meter.createCounter("pgconductor.worker.flush.count"),
            taskDuration: meter.createHistogram("pgconductor.task.duration", { unit: "ms" }),
            taskAttempts: meter.createCounter("pgconductor.task.attempts"),
            dbQueryDuration: meter.createHistogram("pgconductor.db.query.duration", { unit: "ms" }),
            dbRetryCount: meter.createCounter("pgconductor.db.query.retries"),
        },
    };
}
```

Users enable telemetry by passing the returned hooks to `Conductor.create({ ..., telemetry: createPgConductorTelemetry() })`. Because the runtime already wraps every critical path in spans/metrics when the hooks exist, no extra worker-event bus is necessary—spans and metrics already cover the observability story.

## Implementation Steps
1. Apply schema change + regenerate TypeScript types (`just build-migrations`).
2. Introduce `TelemetryHooks` type, extend `ConductorOptions`, and plumb the optional hooks into `Conductor`, `Worker`, `TaskContext`, and `DatabaseClient`.
3. Update enqueue, database, worker, and task code paths to consult `telemetry` when present (as shown above).
4. Build the `@pgconductor/opentelemetry` package that instantiates real OTEL tracers/meters/propagators and exports `createPgConductorTelemetry`.
5. Document setup (register OTEL SDK, pass hooks, verify traces/metrics) and add integration tests to ensure trace context persists across retries and child tasks.
