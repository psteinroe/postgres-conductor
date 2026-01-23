import {
	type Span,
	type Link,
	SpanKind,
	SpanStatusCode,
	context,
	propagation,
	trace,
	ROOT_CONTEXT,
} from "@opentelemetry/api";
import {
	InstrumentationBase,
	InstrumentationNodeModuleDefinition,
	isWrapped,
} from "@opentelemetry/instrumentation";

import type { PgConductorInstrumentationConfig, TraceContext } from "./types";
import { VERSION } from "./version";
import * as SemanticConventions from "./semantic-conventions";
import { PgConductorMetrics } from "./metrics";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyFunction = (...args: any[]) => any;

/**
 * OpenTelemetry instrumentation for Postgres Conductor.
 *
 * Provides tracing and metrics for:
 * - Task invocations (producer spans)
 * - Task executions (consumer spans)
 * - Step operations (internal spans)
 * - Sleep/invoke child operations
 */
export class PgConductorInstrumentation extends InstrumentationBase<PgConductorInstrumentationConfig> {
	private metrics: PgConductorMetrics | null = null;

	constructor(config: PgConductorInstrumentationConfig = {}) {
		super(SemanticConventions.INSTRUMENTATION_NAME, VERSION, config);
	}

	protected init(): InstrumentationNodeModuleDefinition[] {
		return [
			new InstrumentationNodeModuleDefinition(
				"pgconductor-js",
				[">=0.1.0"],
				(moduleExports) => {
					this.patchConductor(moduleExports);
					this.patchWorker(moduleExports);
					this.patchTaskContext(moduleExports);
					return moduleExports;
				},
				(moduleExports) => {
					this.unpatchConductor(moduleExports);
					this.unpatchWorker(moduleExports);
					this.unpatchTaskContext(moduleExports);
					return moduleExports;
				},
			),
		];
	}

	override setConfig(config: PgConductorInstrumentationConfig): void {
		super.setConfig(config);
	}

	override getConfig(): PgConductorInstrumentationConfig {
		return this._config;
	}

	/**
	 * Initialize metrics. Call this after registering with a MeterProvider.
	 */
	initMetrics(): void {
		this.metrics = new PgConductorMetrics(this.meter);
	}

	private get propagateContext(): boolean {
		return this.getConfig().propagateContext !== false;
	}

	private get requireParentSpan(): boolean {
		return this.getConfig().requireParentSpan === true;
	}

	/**
	 * Manually patch pgconductor-js module exports.
	 * Use this in test environments where automatic module hooking doesn't work (e.g., Bun).
	 */
	patchModule(moduleExports: {
		Conductor?: { prototype: Record<string, AnyFunction> };
		Worker?: { prototype: Record<string, AnyFunction> };
		TaskContext?: { prototype: Record<string, AnyFunction> };
	}): void {
		if (moduleExports.Conductor) {
			this.patchConductor(
				moduleExports as { Conductor: { prototype: Record<string, AnyFunction> } },
			);
		}
		if (moduleExports.Worker) {
			this.patchWorker(moduleExports as { Worker: { prototype: Record<string, AnyFunction> } });
		}
		if (moduleExports.TaskContext) {
			this.patchTaskContext(
				moduleExports as { TaskContext: { prototype: Record<string, AnyFunction> } },
			);
		}
	}

	/**
	 * Manually unpatch pgconductor-js module exports.
	 */
	unpatchModule(moduleExports: {
		Conductor?: { prototype: Record<string, AnyFunction> };
		Worker?: { prototype: Record<string, AnyFunction> };
		TaskContext?: { prototype: Record<string, AnyFunction> };
	}): void {
		if (moduleExports.Conductor) {
			this.unpatchConductor(
				moduleExports as { Conductor: { prototype: Record<string, AnyFunction> } },
			);
		}
		if (moduleExports.Worker) {
			this.unpatchWorker(moduleExports as { Worker: { prototype: Record<string, AnyFunction> } });
		}
		if (moduleExports.TaskContext) {
			this.unpatchTaskContext(
				moduleExports as { TaskContext: { prototype: Record<string, AnyFunction> } },
			);
		}
	}

	// ==================== Conductor Patching ====================

	private patchConductor(moduleExports: {
		Conductor: { prototype: Record<string, AnyFunction> };
	}): void {
		const Conductor = moduleExports.Conductor;
		if (!Conductor?.prototype) return;

		if (isWrapped(Conductor.prototype.invoke)) {
			this._unwrap(Conductor.prototype, "invoke");
		}
		this._wrap(Conductor.prototype, "invoke", this.patchConductorInvoke.bind(this));

		if (isWrapped(Conductor.prototype.emit)) {
			this._unwrap(Conductor.prototype, "emit");
		}
		this._wrap(Conductor.prototype, "emit", this.patchConductorEmit.bind(this));
	}

	private unpatchConductor(moduleExports: {
		Conductor: { prototype: Record<string, AnyFunction> };
	}): void {
		const Conductor = moduleExports.Conductor;
		if (!Conductor?.prototype) return;

		if (isWrapped(Conductor.prototype.invoke)) {
			this._unwrap(Conductor.prototype, "invoke");
		}
		if (isWrapped(Conductor.prototype.emit)) {
			this._unwrap(Conductor.prototype, "emit");
		}
	}

	private patchConductorInvoke(original: AnyFunction): AnyFunction {
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const instrumentation = this;
		return async function (
			this: unknown,
			task: { name: string; queue?: string },
			payloadOrItems: unknown,
			opts?: Record<string, unknown>,
		) {
			const queue = (task.queue as string) || "default";
			const taskName = task.name;
			const spanName = `${queue} ${taskName} publish`;

			// Check if we should create a span
			if (instrumentation.requireParentSpan && !trace.getActiveSpan()) {
				return original.call(this, task, payloadOrItems, opts);
			}

			const tracer = instrumentation.tracer;
			const span = tracer.startSpan(spanName, {
				kind: SpanKind.PRODUCER,
				attributes: {
					[SemanticConventions.MESSAGING_SYSTEM]: SemanticConventions.MESSAGING_SYSTEM_VALUE,
					[SemanticConventions.MESSAGING_OPERATION]:
						SemanticConventions.MESSAGING_OPERATION_PUBLISH,
					[SemanticConventions.MESSAGING_DESTINATION_NAME]: queue,
					[SemanticConventions.PGCONDUCTOR_TASK_NAME]: taskName,
					[SemanticConventions.PGCONDUCTOR_TASK_QUEUE]: queue,
				},
			});

			// Call produce hook
			const config = instrumentation.getConfig();
			if (config.produceHook) {
				const result = config.produceHook(span, taskName, payloadOrItems);
				if (result === false) {
					span.end();
					return original.call(this, task, payloadOrItems, opts);
				}
			}

			// Inject trace context if propagation is enabled
			// Important: inject from the span's context, not from active context
			// because the span hasn't been set in active context yet
			let traceContext: TraceContext | undefined;
			if (instrumentation.propagateContext) {
				traceContext = {};
				propagation.inject(trace.setSpan(context.active(), span), traceContext);
			}

			// Merge trace_context into opts or items
			let modifiedOpts = opts;
			let modifiedItems = payloadOrItems;

			if (traceContext) {
				if (Array.isArray(payloadOrItems)) {
					// Batch invoke - add trace_context to each item
					modifiedItems = (payloadOrItems as Record<string, unknown>[]).map((item) => ({
						...item,
						trace_context: traceContext,
					}));
				} else {
					// Single invoke - add to opts
					modifiedOpts = { ...opts, trace_context: traceContext };
				}
			}

			return context.with(trace.setSpan(context.active(), span), async () => {
				try {
					const result = await original.call(this, task, modifiedItems, modifiedOpts);

					// Record metric
					instrumentation.metrics?.recordTaskInvocation({ taskName, queue });

					// Set execution ID if returned
					if (typeof result === "string") {
						span.setAttribute(SemanticConventions.PGCONDUCTOR_EXECUTION_ID, result);
					} else if (Array.isArray(result)) {
						span.setAttribute(SemanticConventions.PGCONDUCTOR_BATCH_SIZE, result.length);
					}

					span.setStatus({ code: SpanStatusCode.OK });
					return result;
				} catch (error) {
					instrumentation.recordError(span, error);
					throw error;
				} finally {
					span.end();
				}
			});
		};
	}

	private patchConductorEmit(original: AnyFunction): AnyFunction {
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const instrumentation = this;
		return async function (this: unknown, event: string, payload: unknown) {
			const spanName = `default emit ${event}`;

			if (instrumentation.requireParentSpan && !trace.getActiveSpan()) {
				return original.call(this, event, payload);
			}

			const tracer = instrumentation.tracer;
			const span = tracer.startSpan(spanName, {
				kind: SpanKind.PRODUCER,
				attributes: {
					[SemanticConventions.MESSAGING_SYSTEM]: SemanticConventions.MESSAGING_SYSTEM_VALUE,
					[SemanticConventions.MESSAGING_OPERATION]:
						SemanticConventions.MESSAGING_OPERATION_PUBLISH,
					[SemanticConventions.PGCONDUCTOR_TASK_NAME]: event,
				},
			});

			return context.with(trace.setSpan(context.active(), span), async () => {
				try {
					const result = await original.call(this, event, payload);
					span.setStatus({ code: SpanStatusCode.OK });
					return result;
				} catch (error) {
					instrumentation.recordError(span, error);
					throw error;
				} finally {
					span.end();
				}
			});
		};
	}

	// ==================== Worker Patching ====================

	private patchWorker(moduleExports: { Worker: { prototype: Record<string, AnyFunction> } }): void {
		const Worker = moduleExports.Worker;
		if (!Worker?.prototype) return;

		if (isWrapped(Worker.prototype.executeSingleTask)) {
			this._unwrap(Worker.prototype, "executeSingleTask");
		}
		this._wrap(Worker.prototype, "executeSingleTask", this.patchExecuteSingleTask.bind(this));

		if (isWrapped(Worker.prototype.executeBatchTask)) {
			this._unwrap(Worker.prototype, "executeBatchTask");
		}
		this._wrap(Worker.prototype, "executeBatchTask", this.patchExecuteBatchTask.bind(this));
	}

	private unpatchWorker(moduleExports: {
		Worker: { prototype: Record<string, AnyFunction> };
	}): void {
		const Worker = moduleExports.Worker;
		if (!Worker?.prototype) return;

		if (isWrapped(Worker.prototype.executeSingleTask)) {
			this._unwrap(Worker.prototype, "executeSingleTask");
		}
		if (isWrapped(Worker.prototype.executeBatchTask)) {
			this._unwrap(Worker.prototype, "executeBatchTask");
		}
	}

	private patchExecuteSingleTask(original: AnyFunction): AnyFunction {
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const instrumentation = this;
		return async function (
			this: { queueName: string },
			task: { name: string },
			exec: {
				id: string;
				task_key: string;
				queue: string;
				payload: unknown;
				trace_context?: TraceContext | null;
			},
		) {
			const queue = this.queueName || exec.queue;
			const taskKey = exec.task_key;
			const spanName = `${queue} ${taskKey} process`;

			const tracer = instrumentation.tracer;

			// Extract parent context from trace_context if available
			let parentContext = context.active();
			const links: Link[] = [];

			if (instrumentation.propagateContext && exec.trace_context) {
				const extractedContext = propagation.extract(ROOT_CONTEXT, exec.trace_context);
				const spanContext = trace.getSpanContext(extractedContext);
				if (spanContext) {
					// Link to the producer span instead of making it a parent
					links.push({ context: spanContext });
				}
			}

			const span = tracer.startSpan(
				spanName,
				{
					kind: SpanKind.CONSUMER,
					attributes: {
						[SemanticConventions.MESSAGING_SYSTEM]: SemanticConventions.MESSAGING_SYSTEM_VALUE,
						[SemanticConventions.MESSAGING_OPERATION]:
							SemanticConventions.MESSAGING_OPERATION_PROCESS,
						[SemanticConventions.MESSAGING_DESTINATION_NAME]: queue,
						[SemanticConventions.PGCONDUCTOR_TASK_NAME]: taskKey,
						[SemanticConventions.PGCONDUCTOR_TASK_QUEUE]: queue,
						[SemanticConventions.PGCONDUCTOR_EXECUTION_ID]: exec.id,
					},
					links,
				},
				parentContext,
			);

			// Call consume hook
			const config = instrumentation.getConfig();
			if (config.consumeHook) {
				const result = config.consumeHook(span, exec);
				if (result === false) {
					span.end();
					return original.call(this, task, exec);
				}
			}

			const startTime = Date.now();

			return context.with(trace.setSpan(context.active(), span), async () => {
				try {
					const result = (await original.call(this, task, exec)) as {
						status: string;
						error?: string;
					};

					const duration = Date.now() - startTime;

					// Set status based on result
					const status = result?.status;
					span.setAttribute(SemanticConventions.PGCONDUCTOR_EXECUTION_STATUS, status || "unknown");

					if (status === "completed") {
						span.setStatus({ code: SpanStatusCode.OK });
						instrumentation.metrics?.recordTaskExecution({
							taskName: taskKey,
							queue,
							status: "completed",
							durationMs: duration,
						});
					} else if (status === "failed" || status === "permanently_failed") {
						span.setStatus({ code: SpanStatusCode.ERROR, message: result?.error });
						if (result?.error) {
							span.setAttribute(SemanticConventions.PGCONDUCTOR_ERROR_MESSAGE, result.error);
						}
						span.setAttribute(
							SemanticConventions.PGCONDUCTOR_ERROR_RETRYABLE,
							status !== "permanently_failed",
						);
						instrumentation.metrics?.recordTaskExecution({
							taskName: taskKey,
							queue,
							status: "failed",
							durationMs: duration,
						});
						if (status === "failed") {
							instrumentation.metrics?.recordTaskRetry({ taskName: taskKey, queue });
						}
					} else if (status === "released") {
						span.setStatus({ code: SpanStatusCode.OK });
						instrumentation.metrics?.recordTaskExecution({
							taskName: taskKey,
							queue,
							status: "released",
							durationMs: duration,
						});
					} else if (status === "invoke_child") {
						span.setStatus({ code: SpanStatusCode.OK });
						instrumentation.metrics?.recordTaskExecution({
							taskName: taskKey,
							queue,
							status: "invoke_child",
							durationMs: duration,
						});
					}

					return result;
				} catch (error) {
					instrumentation.recordError(span, error);
					const duration = Date.now() - startTime;
					instrumentation.metrics?.recordTaskExecution({
						taskName: taskKey,
						queue,
						status: "failed",
						durationMs: duration,
					});
					throw error;
				} finally {
					span.end();
				}
			});
		};
	}

	private patchExecuteBatchTask(original: AnyFunction): AnyFunction {
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const instrumentation = this;
		return async function (
			this: { queueName: string },
			task: { name: string },
			taskKey: string,
			executions: { id: string; queue: string; trace_context?: TraceContext | null }[],
		) {
			const queue = this.queueName || executions[0]?.queue || "default";
			const spanName = `${queue} ${taskKey} process batch`;

			const tracer = instrumentation.tracer;

			// Build links from all execution trace contexts
			const links: Link[] = [];
			if (instrumentation.propagateContext) {
				for (const exec of executions) {
					if (exec.trace_context) {
						const extractedContext = propagation.extract(ROOT_CONTEXT, exec.trace_context);
						const spanContext = trace.getSpanContext(extractedContext);
						if (spanContext) {
							links.push({ context: spanContext });
						}
					}
				}
			}

			const span = tracer.startSpan(spanName, {
				kind: SpanKind.CONSUMER,
				attributes: {
					[SemanticConventions.MESSAGING_SYSTEM]: SemanticConventions.MESSAGING_SYSTEM_VALUE,
					[SemanticConventions.MESSAGING_OPERATION]:
						SemanticConventions.MESSAGING_OPERATION_PROCESS,
					[SemanticConventions.MESSAGING_DESTINATION_NAME]: queue,
					[SemanticConventions.PGCONDUCTOR_TASK_NAME]: taskKey,
					[SemanticConventions.PGCONDUCTOR_TASK_QUEUE]: queue,
					[SemanticConventions.PGCONDUCTOR_BATCH_SIZE]: executions.length,
				},
				links,
			});

			const startTime = Date.now();

			return context.with(trace.setSpan(context.active(), span), async () => {
				try {
					const results = (await original.call(this, task, taskKey, executions)) as {
						status: string;
						error?: string;
					}[];

					const duration = Date.now() - startTime;

					// Count statuses
					let completed = 0;
					let failed = 0;
					for (const result of results) {
						if (result.status === "completed") completed++;
						else if (result.status === "failed" || result.status === "permanently_failed") failed++;
					}

					if (failed === 0) {
						span.setStatus({ code: SpanStatusCode.OK });
					} else if (completed === 0) {
						span.setStatus({ code: SpanStatusCode.ERROR, message: "All executions failed" });
					} else {
						span.setStatus({ code: SpanStatusCode.OK });
					}

					// Record metrics for each execution
					for (const result of results) {
						instrumentation.metrics?.recordTaskExecution({
							taskName: taskKey,
							queue,
							status: result.status,
							durationMs: duration / results.length,
						});
					}

					return results;
				} catch (error) {
					instrumentation.recordError(span, error);
					const duration = Date.now() - startTime;
					for (const _exec of executions) {
						instrumentation.metrics?.recordTaskExecution({
							taskName: taskKey,
							queue,
							status: "failed",
							durationMs: duration / executions.length,
						});
					}
					throw error;
				} finally {
					span.end();
				}
			});
		};
	}

	// ==================== TaskContext Patching ====================

	private patchTaskContext(moduleExports: {
		TaskContext: { prototype: Record<string, AnyFunction> };
	}): void {
		const TaskContext = moduleExports.TaskContext;
		if (!TaskContext?.prototype) return;

		if (isWrapped(TaskContext.prototype.step)) {
			this._unwrap(TaskContext.prototype, "step");
		}
		this._wrap(TaskContext.prototype, "step", this.patchStep.bind(this));

		if (isWrapped(TaskContext.prototype.invoke)) {
			this._unwrap(TaskContext.prototype, "invoke");
		}
		this._wrap(TaskContext.prototype, "invoke", this.patchContextInvoke.bind(this));

		if (isWrapped(TaskContext.prototype.sleep)) {
			this._unwrap(TaskContext.prototype, "sleep");
		}
		this._wrap(TaskContext.prototype, "sleep", this.patchSleep.bind(this));
	}

	private unpatchTaskContext(moduleExports: {
		TaskContext: { prototype: Record<string, AnyFunction> };
	}): void {
		const TaskContext = moduleExports.TaskContext;
		if (!TaskContext?.prototype) return;

		if (isWrapped(TaskContext.prototype.step)) {
			this._unwrap(TaskContext.prototype, "step");
		}
		if (isWrapped(TaskContext.prototype.invoke)) {
			this._unwrap(TaskContext.prototype, "invoke");
		}
		if (isWrapped(TaskContext.prototype.sleep)) {
			this._unwrap(TaskContext.prototype, "sleep");
		}
	}

	private patchStep(original: AnyFunction): AnyFunction {
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const instrumentation = this;
		return async function (
			this: { opts: { execution: { task_key: string; id: string } } },
			name: string,
			fn: () => unknown,
		) {
			const taskKey = this.opts?.execution?.task_key || "unknown";
			const executionId = this.opts?.execution?.id || "unknown";
			const spanName = `${taskKey} step ${name}`;

			const tracer = instrumentation.tracer;
			const span = tracer.startSpan(spanName, {
				kind: SpanKind.INTERNAL,
				attributes: {
					[SemanticConventions.PGCONDUCTOR_TASK_NAME]: taskKey,
					[SemanticConventions.PGCONDUCTOR_STEP_NAME]: name,
					[SemanticConventions.PGCONDUCTOR_EXECUTION_ID]: executionId,
				},
			});

			const startTime = Date.now();

			return context.with(trace.setSpan(context.active(), span), async () => {
				try {
					// The original step() will check if cached and return early
					// We detect caching by checking if fn was called
					let fnCalled = false;
					const wrappedFn = async () => {
						fnCalled = true;
						return fn();
					};

					const result = await original.call(this, name, wrappedFn);

					const cached = !fnCalled;
					span.setAttribute(SemanticConventions.PGCONDUCTOR_STEP_CACHED, cached);

					const config = instrumentation.getConfig();
					if (config.stepHook) {
						config.stepHook(span, name, cached);
					}

					const duration = Date.now() - startTime;
					instrumentation.metrics?.recordStepExecution({ cached, durationMs: duration });

					span.setStatus({ code: SpanStatusCode.OK });
					return result;
				} catch (error) {
					instrumentation.recordError(span, error);
					throw error;
				} finally {
					span.end();
				}
			});
		};
	}

	private patchContextInvoke(original: AnyFunction): AnyFunction {
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const instrumentation = this;
		return async function (
			this: {
				opts: {
					execution: { task_key: string; id: string };
					abortController?: { signal: AbortSignal };
				};
			},
			key: string,
			task: { name: string; queue?: string },
			payload: unknown,
			timeout?: number,
		) {
			const parentTaskKey = this.opts?.execution?.task_key || "unknown";
			const executionId = this.opts?.execution?.id || "unknown";
			const childTaskName = task.name;
			const spanName = `${parentTaskKey} invoke ${childTaskName}`;

			const tracer = instrumentation.tracer;
			const span = tracer.startSpan(spanName, {
				kind: SpanKind.INTERNAL,
				attributes: {
					[SemanticConventions.PGCONDUCTOR_TASK_NAME]: parentTaskKey,
					[SemanticConventions.PGCONDUCTOR_EXECUTION_ID]: executionId,
					[SemanticConventions.PGCONDUCTOR_STEP_NAME]: key,
				},
			});

			// Invoke can either:
			// 1. Return cached result if step exists in DB
			// 2. Trigger abort (child-invocation) and return a never-resolving promise
			// We need to end the span in both cases

			const abortController = this.opts?.abortController;
			let abortListener: (() => void) | undefined;

			return context.with(trace.setSpan(context.active(), span), async () => {
				try {
					// Set up abort listener to end span when invoke triggers hangup
					if (abortController?.signal) {
						abortListener = () => {
							span.setAttribute(SemanticConventions.PGCONDUCTOR_EXECUTION_STATUS, "invoke_child");
							span.setStatus({ code: SpanStatusCode.OK });
							span.end();
						};
						abortController.signal.addEventListener("abort", abortListener, { once: true });
					}

					const result = await original.call(this, key, task, payload, timeout);

					// If we get here, invoke returned (from cache)
					if (abortListener && abortController?.signal) {
						abortController.signal.removeEventListener("abort", abortListener);
					}

					span.setStatus({ code: SpanStatusCode.OK });
					span.end();
					return result;
				} catch (error) {
					// Remove abort listener on error
					if (abortListener && abortController?.signal) {
						abortController.signal.removeEventListener("abort", abortListener);
					}
					instrumentation.recordError(span, error);
					span.end();
					throw error;
				}
			});
		};
	}

	private patchSleep(original: AnyFunction): AnyFunction {
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const instrumentation = this;
		return async function (
			this: {
				opts: {
					execution: { task_key: string; id: string };
					abortController?: { signal: AbortSignal };
				};
			},
			id: string,
			ms: number,
		) {
			const taskKey = this.opts?.execution?.task_key || "unknown";
			const executionId = this.opts?.execution?.id || "unknown";
			const spanName = `${taskKey} sleep`;

			const tracer = instrumentation.tracer;
			const span = tracer.startSpan(spanName, {
				kind: SpanKind.INTERNAL,
				attributes: {
					[SemanticConventions.PGCONDUCTOR_TASK_NAME]: taskKey,
					[SemanticConventions.PGCONDUCTOR_EXECUTION_ID]: executionId,
					[SemanticConventions.PGCONDUCTOR_STEP_NAME]: id,
				},
			});

			// Sleep can either:
			// 1. Return immediately if already cached (step exists in DB)
			// 2. Trigger abort and return a never-resolving promise (not cached)
			// We need to end the span in both cases

			const abortController = this.opts?.abortController;
			let abortListener: (() => void) | undefined;

			return context.with(trace.setSpan(context.active(), span), async () => {
				try {
					// Set up abort listener to end span when sleep triggers hangup
					if (abortController?.signal) {
						abortListener = () => {
							span.setAttribute(SemanticConventions.PGCONDUCTOR_EXECUTION_STATUS, "released");
							span.setStatus({ code: SpanStatusCode.OK });
							span.end();
						};
						abortController.signal.addEventListener("abort", abortListener, { once: true });
					}

					const result = await original.call(this, id, ms);

					// If we get here, sleep returned (from cache)
					// Remove the abort listener since we'll end the span normally
					if (abortListener && abortController?.signal) {
						abortController.signal.removeEventListener("abort", abortListener);
					}

					span.setStatus({ code: SpanStatusCode.OK });
					span.end();
					return result;
				} catch (error) {
					// Remove abort listener on error
					if (abortListener && abortController?.signal) {
						abortController.signal.removeEventListener("abort", abortListener);
					}
					instrumentation.recordError(span, error);
					span.end();
					throw error;
				}
			});
		};
	}

	// ==================== Helpers ====================

	private recordError(span: Span, error: unknown): void {
		const err = error instanceof Error ? error : new Error(String(error));
		span.recordException(err);
		span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
		span.setAttribute(SemanticConventions.PGCONDUCTOR_ERROR_MESSAGE, err.message);
	}
}
