import type {
	CronRegistration,
	DatabaseClientLike,
	EventSubscriptionSpec,
	Execution,
	ExecutionResult,
	ExecutionSpec,
	ExecutionCompleted,
	ExecutionFailed,
	ExecutionPermamentlyFailed,
	ExecutionReleased,
	ExecutionInvokeChild,
} from "./database-client";
import type { AnyTask, BatchConfig } from "./task";
import type { TaskDefinition, CustomEventTrigger, DatabaseEventTrigger } from "./task-definition";
import { waitFor } from "./lib/wait-for";
import { mapConcurrent } from "./lib/map-concurrent";
import { Deferred } from "./lib/deferred";
import { type PollableAsyncIterable } from "./lib/async-queue";
import { BatchingAsyncQueue, type BatchGroup } from "./lib/batching-async-queue";
import CronExpressionParser from "cron-parser";
import {
	createTaskSignal,
	isTaskAbortReason,
	TaskContext,
	BatchTaskContext,
	type TaskAbortReasons,
} from "./task-context";
import * as assert from "./lib/assert";
import { createMaintenanceTask } from "./maintenance-task";
import { makeChildLogger, type Logger } from "./lib/logger";
import type { EventDefinition } from "./event-definition";
import { coerceError } from "./lib/coerce-error";
import type { TypedAbortController } from "./lib/typed-abort-controller";
import { ROOT_CONTEXT, SpanKind, type Span, type SpanContext } from "@opentelemetry/api";
import {
	endSpan,
	extractCarrier,
	linksFromCarrier,
	carrierForContext,
	contextForSpan,
	contextForSpanContext,
	messagingAttributes,
	runWithSpan,
	setSpanAttribute,
	setSpanError,
	spanContext,
	startSpan,
	recordConsumed,
	recordProcessDuration,
	recordOperationDuration,
	recordLifecycle,
	recordSent,
} from "./telemetry";

/**
 * The configuration options for the Worker.
 */
export type WorkerConfig = {
	concurrency: number;
	flushBatchSize: number;
	fetchBatchSize: number;
	pollIntervalMs: number;
	flushIntervalMs: number;
};

/**
 * The default configuration for the Worker.
 */
export const EVENT_BATCH_SIZE = 100;

const RETRYABLE_EVENT_ERROR_CODES = new Set([
	"40001",
	"40P01",
	"55P03",
	"57P01",
	"57P02",
	"57P03",
	"53300",
	"08000",
	"08003",
	"08006",
	"08001",
	"ECONNRESET",
	"ECONNREFUSED",
	"ETIMEDOUT",
]);

function isRetryableEventError(error: unknown): boolean {
	const code = (error as { code?: string })?.code;
	return code !== undefined && RETRYABLE_EVENT_ERROR_CODES.has(code);
}

const MAINTENANCE_TASK_NAME = "pgconductor.maintenance";

const DEFAULT_WORKER_CONFIG: WorkerConfig = {
	concurrency: 1,
	flushBatchSize: 2,
	fetchBatchSize: 2,
	pollIntervalMs: 1000,
	flushIntervalMs: 2000,
};

/**
 * Encapsulates buffered execution results with internal counting and task key tracking.
 */
class BufferState {
	orchestratorId = "";
	completed: ExecutionCompleted[] = [];
	failed: (ExecutionFailed | ExecutionPermamentlyFailed)[] = [];
	released: ExecutionReleased[] = [];
	invokeChild: ExecutionInvokeChild[] = [];
	taskKeys = new Set<string>();
	count = 0;

	add(result: ExecutionResult): void {
		this.orchestratorId = result.orchestrator_id || this.orchestratorId;
		this.taskKeys.add(result.task_key);
		this.count++;

		switch (result.status) {
			case "completed":
				this.completed.push(result);
				break;
			case "failed":
			case "permanently_failed":
				this.failed.push(result);
				break;
			case "released":
				this.released.push(result);
				break;
			case "invoke_child":
				this.invokeChild.push(result);
				break;
		}
	}

	clear(): void {
		this.completed = [];
		this.failed = [];
		this.released = [];
		this.invokeChild = [];
		this.taskKeys.clear();
		this.count = 0;
	}

	restore(other: BufferState): void {
		this.completed.push(...other.completed);
		this.failed.push(...other.failed);
		this.released.push(...other.released);
		this.invokeChild.push(...other.invokeChild);
		this.orchestratorId = this.orchestratorId || other.orchestratorId;
		this.count += other.count;
		for (const key of other.taskKeys) {
			this.taskKeys.add(key);
		}
	}
}

/**
 * Worker implemented as async pipeline: fetch → execute → flush.
 * Uses async iterators for clean composition and natural backpressure.
 *
 * Queue-aware: Processes multiple tasks from a single queue.
 */
export class Worker<
	Tasks extends readonly TaskDefinition<string, any, any, string>[] = readonly TaskDefinition<
		string,
		any,
		any,
		string
	>[],
	Events extends readonly EventDefinition<string, any, any>[] = readonly EventDefinition<
		string,
		any,
		any
	>[],
> {
	private orchestratorId: string | null = null;

	private readonly tasks: Map<string, AnyTask>;

	private readonly fetchBatchSize: number;
	private readonly concurrency: number;
	private readonly flushBatchSize: number;
	private readonly flushIntervalMs: number;
	private readonly pollIntervalMs: number;

	private _startDeferred: Deferred<void> | null = null;
	private _stopDeferred: Deferred<void> | null = null;
	private _abortController: AbortController | null = null;
	private _drainDidWork = false;
	private _runningTasks = new Map<string, TypedAbortController<TaskAbortReasons>>();
	private eventProcessingGate: Promise<void> = Promise.resolve();
	private readonly processSpanContexts = new Map<string, SpanContext>();
	private readonly pendingDurableProducers = new Map<string, Span>();
	private readonly parentDeadLetterTargets = new Map<
		string,
		{ sourceExecutionId: string; queue: string; task: string }
	>();
	private flushInFlight: Promise<void> = Promise.resolve();
	private hasRegisteredCronSchedules = false;

	/** Used by Orchestrator to prevent local event fan-out before every worker registers. */
	setEventProcessingGate(gate: Promise<void>): void {
		this.eventProcessingGate = gate;
	}

	constructor(
		public readonly queueName: string,
		tasks: readonly AnyTask[],
		private readonly db: DatabaseClientLike,
		private readonly logger: Logger,
		config: Partial<WorkerConfig> = {},
		private readonly extraContext: object = {},
		private readonly eventDefinitions: readonly EventDefinition<string, any, any>[] = [],
		private readonly telemetry = true,
	) {
		const maintenanceTask = createMaintenanceTask(this.queueName);
		this.tasks = tasks.reduce(
			(m, task) => {
				m.set(task.name, task);
				return m;
			},
			new Map<string, AnyTask>([[maintenanceTask.name, maintenanceTask]]),
		);

		const fullConfig = { ...DEFAULT_WORKER_CONFIG, ...config };

		this.concurrency = fullConfig.concurrency;
		this.pollIntervalMs = fullConfig.pollIntervalMs;
		this.flushIntervalMs = fullConfig.flushIntervalMs;
		this.fetchBatchSize = fullConfig.fetchBatchSize;
		this.flushBatchSize = fullConfig.flushBatchSize;
	}

	/**
	 * Promise that resolves when worker has started.
	 * (Registration complete, pipeline initialized)
	 */
	get started(): Promise<void> {
		return this._startDeferred?.promise || Promise.resolve();
	}

	/**
	 * Promise that resolves when worker has stopped.
	 * (Pipeline complete, cleanup done)
	 */
	get stopped(): Promise<void> {
		return this._stopDeferred?.promise || Promise.resolve();
	}

	/** @internal Whether the last run-once pass observed any work. */
	get drainDidWork(): boolean {
		return this._drainDidWork;
	}

	/**
	 * Start the worker.
	 * Returns when startup is complete (registration done).
	 * Worker continues running in background until stop() is called.
	 */
	async start(orchestratorId: string): Promise<void> {
		return this._internalStart(orchestratorId, { runOnce: false });
	}

	/**
	 * Start the worker and wait until it stops.
	 * Equivalent to: await start(id); return stopped;
	 * Returns when worker has fully stopped.
	 */
	async run(orchestratorId: string): Promise<void> {
		await this._internalStart(orchestratorId, { runOnce: false });
		return this.stopped;
	}

	/**
	 * Process all queued tasks once and stop automatically.
	 * Returns when all work is complete.
	 * Useful for testing and batch processing.
	 */
	async drain(orchestratorId: string): Promise<void> {
		await this._internalStart(orchestratorId, { runOnce: true });
		return this.stopped;
	}

	private async _internalStart(
		orchestratorId: string,
		{ runOnce = false }: { runOnce?: boolean },
	): Promise<void> {
		if (this._stopDeferred) {
			throw new Error("Worker is already running");
		}

		this.orchestratorId = orchestratorId;
		this._drainDidWork = false;
		this._startDeferred = new Deferred<void>();
		this._stopDeferred = new Deferred<void>();
		this._abortController = new AbortController();

		// Synchronous registration. A failed registration rejects both lifecycle
		// promises; callers must never observe a worker that started partially.
		try {
			await this.register();
		} catch (error) {
			// Registration is part of startup, not a running pipeline. Resolve the
			// stop promise so callers that only await `start()` do not get an
			// unhandled rejection, then discard every piece of this failed attempt.
			const stopDeferred = this._stopDeferred;
			this._abortController.abort();
			// Reject `started` so an Orchestrator observes registration failure, but
			// attach a noop handler because callers that only use start() do not
			// necessarily observe this internal lifecycle promise.
			this._startDeferred.promise.catch(() => {});
			this._startDeferred.reject(error);
			if (!stopDeferred.isSettled) stopDeferred.resolve();
			this.resetLifecycle();
			throw error;
		}

		// Worker is now started
		this._startDeferred.resolve();

		// Run pipeline in background
		// Build batch configs map
		const batchConfigs = new Map<string, BatchConfig>();
		for (const [taskKey, task] of this.tasks.entries()) {
			if (task.batch) {
				batchConfigs.set(taskKey, task.batch);
			}
		}

		const queue = new BatchingAsyncQueue<Execution>(this.fetchBatchSize * 2, batchConfigs);
		if (runOnce) {
			void this.runDrainPipeline();
		} else {
			void this.fetchExecutions(queue, { runOnce });
			void (async () => {
				try {
					await Promise.all([
						this.flushResults(this.executeTasks(queue)),
						this.runEventProcessor(),
					]);
				} catch (error) {
					this.logger.error("Worker pipeline error:", error);
					this._stopDeferred?.reject(error);
				} finally {
					queue.close();
					await this.flushInFlight;
					if (this._stopDeferred && !this._stopDeferred.isSettled) this._stopDeferred.resolve();
					this.resetLifecycle();
				}
			})();
		}

		return this._startDeferred.promise;
	}

	private async runEventProcessor(): Promise<void> {
		await Promise.race([
			this.eventProcessingGate,
			new Promise<void>((resolve) =>
				this.signal.addEventListener("abort", () => resolve(), { once: true }),
			),
		]);
		await this.processEventBatches({ runOnce: false });
	}

	private async runDrainPipeline(): Promise<void> {
		try {
			while (!this.signal.aborted) {
				await Promise.race([
					this.eventProcessingGate,
					new Promise<void>((resolve) =>
						this.signal.addEventListener("abort", () => resolve(), {
							once: true,
						}),
					),
				]);
				const events = await this.processEventBatches({ runOnce: true });
				this._drainDidWork ||= events > 0;
				const queue = new BatchingAsyncQueue<Execution>(
					this.fetchBatchSize * 2,
					new Map(
						Array.from(this.tasks.entries()).flatMap(([key, task]) =>
							task.batch ? [[key, task.batch] as const] : [],
						),
					),
				);
				const fetched = this.fetchExecutions(queue, { runOnce: true });
				await this.flushResults(this.executeTasks(queue));
				const executions = await fetched;
				this._drainDidWork ||= executions > 0;
				if (events === 0 && executions === 0) break;
			}
		} catch (error) {
			this.logger.error("Worker pipeline error:", error);
			this._stopDeferred?.reject(error);
		} finally {
			await this.flushInFlight;
			if (this._stopDeferred && !this._stopDeferred.isSettled) this._stopDeferred.resolve();
			this.resetLifecycle();
		}
	}

	private resetLifecycle(): void {
		this._startDeferred = null;
		this._stopDeferred = null;
		this._abortController = null;
		this.orchestratorId = null;
		this.eventProcessingGate = Promise.resolve();
		this.processSpanContexts.clear();
		this.pendingDurableProducers.clear();
		this.parentDeadLetterTargets.clear();
	}

	private async processEventBatches({ runOnce }: { runOnce: boolean }): Promise<number> {
		let total = 0;
		while (!this.signal.aborted) {
			try {
				const resolvedBefore = await this.db.resolveEventWaits(
					{ batchSize: EVENT_BATCH_SIZE },
					{ signal: this.signal },
				);
				const processed = await this.db.processEvents(
					{ batchSize: EVENT_BATCH_SIZE },
					{ signal: this.signal },
				);
				const resolvedAfter = await this.db.resolveEventWaits(
					{ batchSize: EVENT_BATCH_SIZE },
					{ signal: this.signal },
				);
				const didWork = resolvedBefore + processed + resolvedAfter;
				total += didWork;
				if (runOnce && didWork === 0) return total;
				if (!runOnce && didWork === 0) {
					await waitFor(this.pollIntervalMs, { signal: this.signal });
				}
			} catch (error) {
				// Event fan-out is transactional. Non-retryable errors indicate a
				// broken processor/schema and must fail the worker, not spin forever.
				if (!isRetryableEventError(error)) throw error;
				await waitFor(this.pollIntervalMs, { signal: this.signal });
			}
		}
		return total;
	}

	/**
	 * Stop the worker gracefully.
	 * Returns when shutdown is complete.
	 */
	async stop(): Promise<void> {
		const currentStopDeferred = this._stopDeferred;
		const currentAbortController = this._abortController;

		if (!currentStopDeferred || !currentAbortController) {
			return;
		}

		currentAbortController.abort();

		try {
			await currentStopDeferred.promise;
		} catch {}
	}

	/**
	 * Cancel running executions by aborting their task controllers.
	 * Called by orchestrator when cancellation signals are received.
	 */
	public cancelExecutions(ids: string[]): void {
		for (const id of ids) {
			const controller = this._runningTasks.get(id);
			if (controller) {
				controller.abort({
					reason: "cancelled",
					__pgconductorTaskAborted: true,
				});
				this._runningTasks.delete(id);
			}
		}
	}

	private async register(): Promise<void> {
		// Filters are an explicit event-definition allowlist. Validate again at
		// registration so tasks assembled outside Conductor cannot bypass it.
		for (const task of this.tasks.values()) {
			for (const trigger of task.triggers) {
				if (!("event" in trigger)) continue;
				if ("when" in trigger) {
					throw new Error(`Custom event "${trigger.event}" does not support a when clause`);
				}
				if (!("filter" in trigger) || !trigger.filter) continue;
				const definition = this.eventDefinitions.find((event) => event.name === trigger.event);
				if (!definition) {
					throw new Error(`Filtered event "${trigger.event}" has no runtime event definition`);
				}
				const allowed = new Set<string>(definition.filterable ?? []);
				for (const [field, values] of Object.entries(trigger.filter as Record<string, unknown>)) {
					if (!allowed.has(field))
						throw new Error(
							`Filter for event "${trigger.event}" contains undeclared field "${field}"`,
						);
					if (!Array.isArray(values))
						throw new Error(
							`Filter value for event "${trigger.event}" field "${field}" must be an array`,
						);
					if (values.length === 0)
						throw new Error(
							`Filter value for event "${trigger.event}" field "${field}" cannot be empty`,
						);
				}
			}
		}

		// Convert RetentionSettings to integer: null=keep, 0=delete now, N=delete after N days
		const retentionToDays = (setting: boolean | { days: number } | undefined): number | null => {
			if (setting === undefined || setting === false) return null;
			if (setting === true) return 0;
			return setting.days;
		};

		const taskSpecs = Array.from(this.tasks.values()).map((task) => ({
			key: task.name,
			queue: this.queueName,
			maxAttempts: task.maxAttempts,
			removeOnCompleteDays: retentionToDays(task.removeOnComplete),
			removeOnFailDays: retentionToDays(task.removeOnFail),
			window: task.window,
			concurrency: task.concurrency,
			groupConcurrency: task.groupConcurrency,
			deadLetterQueue: task.deadLetter?.queue,
			deadLetterTaskKey: task.deadLetter?.task?.name,
		}));

		const allTasks = Array.from(this.tasks.values());

		const currentTime = await this.db.getCurrentTime({ signal: this.signal });
		const cronSchedules: ExecutionSpec[] = allTasks.flatMap((task) =>
			task.triggers
				.filter((t): t is { cron: string; name: string; group?: string } => "cron" in t)
				.map((trigger) => {
					const interval = CronExpressionParser.parse(trigger.cron, { currentDate: currentTime });
					const nextTimestamp = interval.next().toDate();
					const timestampSeconds = Math.floor(nextTimestamp.getTime() / 1000);
					return {
						task_key: task.name,
						queue: this.queueName,
						run_at: nextTimestamp,
						dedupe_key: `scheduled::${trigger.name}::${timestampSeconds}`,
						cron_expression: trigger.cron,
						group: trigger.group || null,
					};
				}),
		);

		const eventSubscriptions: EventSubscriptionSpec[] = allTasks.flatMap((task) => {
			const customEvents = task.triggers.flatMap((trigger): EventSubscriptionSpec[] => {
				if (!("event" in trigger) || "schema" in trigger || typeof trigger.event !== "string")
					return [];
				const customTrigger = trigger as CustomEventTrigger<
					string,
					string | undefined,
					Record<string, import("./database-client").JsonValue[]> | undefined
				>;
				return [
					{
						task_key: task.name,
						queue: this.queueName,
						event_key: customTrigger.event,
						schema_name: null,
						table_name: null,
						operation: null,
						when_clause: null,
						payload_fields: customTrigger.fields?.split(",").map((field) => field.trim()) || null,
						column_names: null,
						filter: customTrigger.filter || null,
					},
				];
			});

			const dbEvents = task.triggers.flatMap((trigger): EventSubscriptionSpec[] => {
				if (!("schema" in trigger) || !("table" in trigger) || !("operation" in trigger)) return [];
				const databaseTrigger = trigger as DatabaseEventTrigger;
				return [
					{
						task_key: task.name,
						queue: this.queueName,
						event_key: null,
						schema_name: databaseTrigger.schema,
						table_name: databaseTrigger.table,
						operation: databaseTrigger.operation,
						when_clause: databaseTrigger.when || null,
						payload_fields: null,
						column_names:
							databaseTrigger.columns?.split(",").map((column) => column.trim()) || null,
						filter: null,
					},
				];
			});

			return [...customEvents, ...dbEvents];
		});

		const userCronSchedules = cronSchedules.filter(
			(schedule) => schedule.task_key !== MAINTENANCE_TASK_NAME,
		);
		const cronProducer =
			this.telemetry && userCronSchedules.length && !this.hasRegisteredCronSchedules
				? startSpan(
						`send ${this.queueName}`,
						SpanKind.PRODUCER,
						messagingAttributes(
							userCronSchedules.length === 1 ? userCronSchedules[0]?.task_key : undefined,
							this.queueName,
							"send",
							undefined,
							userCronSchedules.length,
						),
					)
				: null;
		const cronCarrier = cronProducer ? carrierForContext(contextForSpan(cronProducer)) : null;
		for (const schedule of userCronSchedules) schedule.trace_context = cronCarrier;
		const registrationStarted = performance.now();
		try {
			const registrations: CronRegistration[] = await runWithSpan(cronProducer, () =>
				this.db.registerWorker(
					{ queueName: this.queueName, taskSpecs, cronSchedules, eventSubscriptions },
					{ signal: this.signal },
				),
			);
			const authoritativeUserRows = registrations.filter(
				(row) => row.authoritative && !row.is_maintenance,
			);
			setSpanAttribute(cronProducer, "messaging.batch.message_count", authoritativeUserRows.length);
			if (cronProducer && authoritativeUserRows.length > 0 && this.telemetry) {
				recordSent(this.queueName, authoritativeUserRows.length);
				recordOperationDuration(this.queueName, performance.now() - registrationStarted, "send");
			}
			this.hasRegisteredCronSchedules = true;
		} catch (error) {
			setSpanError(cronProducer, error);
			throw error;
		} finally {
			endSpan(cronProducer);
		}
	}

	// --- Stage 1: Fetch executions from database ---
	private async fetchExecutions(
		queue: BatchingAsyncQueue<Execution>,
		{ runOnce = false }: { runOnce?: boolean },
	): Promise<number> {
		let fetched = 0;
		assert.ok(this.orchestratorId, "orchestratorId must be set when starting the pipeline");

		const allTasks = Array.from(this.tasks.values());
		// Check if any tasks have windows - only then do we need time-based filtering
		const tasksWithWindows = allTasks.filter((task) => task.window);

		while (!this.signal?.aborted) {
			try {
				// Filter tasks based on time windows (if any)
				let disallowedTaskKeys: string[] = [];

				if (tasksWithWindows.length > 0) {
					// db.getCurrentTime() handles test vs production: returns fake time in tests, system time in production
					const now = await this.db.getCurrentTime({ signal: this.signal });

					const isWithinWindow = (task: AnyTask, now: Date): boolean => {
						if (!task.window) return true;
						const [start, end] = task.window;
						const currentTime = now.toISOString().slice(11, 19);
						return currentTime >= start && currentTime < end;
					};

					disallowedTaskKeys = tasksWithWindows
						.filter((task) => !isWithinWindow(task, now))
						.map((t) => t.name);

					if (disallowedTaskKeys.length === tasksWithWindows.length) {
						// All windowed tasks are outside their windows, skip fetch for those
						// But non-windowed tasks can still run
						if (tasksWithWindows.length === this.tasks.size) {
							// All tasks have windows and all are outside - skip entirely
							await waitFor(this.pollIntervalMs, { signal: this.signal });
							continue;
						}
					}
				}

				const executions = await this.db.getExecutions(
					{
						orchestratorId: this.orchestratorId,
						queueName: this.queueName,
						batchSize: this.fetchBatchSize,
						filterTaskKeys: disallowedTaskKeys,
					},
					{ signal: this.signal },
				);

				if (executions.length === 0) {
					if (runOnce) {
						queue.close();
						break;
					}
					await waitFor(this.pollIntervalMs, { signal: this.signal });
					continue;
				}

				for (const exec of executions) {
					fetched++;
					await queue.push(exec); // waits if full
					if (this.signal.aborted) break;
				}
			} catch {
				await waitFor(2000, { signal: this.signal });
			}
		}

		queue.close();
		return fetched;
	}

	// --- Stage 2: Execute tasks concurrently ---
	private async *executeTasks(
		source: PollableAsyncIterable<BatchGroup<Execution>>,
	): AsyncGenerator<ExecutionResult> {
		for await (const result of mapConcurrent(
			source,
			this.concurrency,
			async ({
				taskKey,
				items: executions,
			}): Promise<ExecutionResult | ExecutionResult[] | null> => {
				// Dispatch to correct task based on task_key
				const task = this.tasks.get(taskKey);
				if (!task) {
					return executions.map((exec) => ({
						queue: exec.queue,
						execution_id: exec.id,
						orchestrator_id: exec.locked_by,
						task_key: taskKey,
						status: "failed",
						cancelled: false,
						error: `Task not found: ${taskKey}`,
					})) as ExecutionResult[];
				}

				// Safety check: don't execute already-cancelled tasks
				const cancelledExecs = executions.filter((e) => e.cancelled);
				const cancelledResults = cancelledExecs.map((exec) => ({
					execution_id: exec.id,
					orchestrator_id: exec.locked_by,
					queue: exec.queue,
					task_key: taskKey,
					status: "permanently_failed" as const,
					cancelled: true as const,
					error: exec.last_error || "Execution was cancelled",
				}));
				if (cancelledExecs.length === executions.length) return cancelledResults;

				// Note: We intentionally do NOT check this.signal.aborted here.
				// When shutdown occurs, fetchExecutions closes the queue which flushes
				// pending batches. We want to process those flushed items during shutdown.
				// The abort signal tells fetchExecutions to stop fetching NEW items,
				// but executeTasks should process what's already queued.

				// Filter out cancelled executions
				const activeExecs = executions.filter((e) => !e.cancelled);

				// If all cancelled, we already returned cancelled results above
				if (activeExecs.length === 0) {
					// This shouldn't happen due to check above, but be safe
					return [];
				}

				// If task has batch config, always use batch execution (even for single items)
				if (task.batch) {
					return [
						...cancelledResults,
						...(await this.executeBatchTask(task, taskKey, activeExecs)),
					];
				}

				// Execute single (non-batched tasks)
				const singleExec = activeExecs[0];
				assert.ok(singleExec, "activeExecs must have at least one item");
				const activeResult = await this.executeSingleTask(task, singleExec);
				return cancelledResults.length && activeResult
					? [...cancelledResults, activeResult]
					: activeResult;
			},
		)) {
			// Waiting executions are released atomically by registerEventWait and
			// therefore have no result to settle here.
			if (result === null) continue;
			if (Array.isArray(result)) {
				for (const r of result) yield r;
			} else {
				yield result;
			}
		}
	}

	/**
	 * Execute a single task execution.
	 *
	 * @param task - The task to execute
	 * @param exec - The execution details
	 */
	private async executeSingleTask(task: AnyTask, exec: Execution): Promise<ExecutionResult | null> {
		const taskAbortController = createTaskSignal(this.signal);
		this._runningTasks.set(exec.id, taskAbortController);

		const abortPromise = new Promise<TaskAbortReasons>((resolve) => {
			taskAbortController.signal.addEventListener("abort", () => {
				resolve(taskAbortController.signal.reason);
			});
		});
		const processTelemetry = this.telemetry && exec.task_key !== MAINTENANCE_TASK_NAME;
		if (exec.parent_execution_id && exec.parent_dead_letter_queue)
			this.parentDeadLetterTargets.set(exec.id, {
				sourceExecutionId: exec.parent_execution_id,
				queue: exec.parent_dead_letter_queue,
				task: exec.parent_dead_letter_task_key || exec.parent_task_key || exec.task_key,
			});
		const processStarted = performance.now();
		const consumer = processTelemetry
			? startSpan(
					`process ${exec.queue}`,
					SpanKind.CONSUMER,
					messagingAttributes(exec.task_key, exec.queue, "process", exec.id),
					extractCarrier(exec.trace_context) || ROOT_CONTEXT,
					linksFromCarrier(exec.trace_link_context),
				)
			: null;
		const consumerContext = spanContext(consumer);
		if (consumerContext) this.processSpanContexts.set(exec.id, consumerContext);

		try {
			await this.scheduleNextExecution(exec, consumer, processTelemetry);

			// Determine event type based on execution data
			let taskEvent: any;
			if (exec.cron_expression) {
				// Extract schedule name from dedupe_key (format: scheduled::{name}::{timestamp})
				const scheduleName = exec.dedupe_key?.split("::")[1] || "unknown";
				taskEvent = { name: scheduleName };
			} else if (
				exec.payload &&
				typeof exec.payload === "object" &&
				"event" in exec.payload &&
				exec.payload.event !== "pgconductor.invoke"
			) {
				// Event-triggered execution (custom event or db event)
				taskEvent = {
					name: exec.payload.event,
					payload: exec.payload.payload,
				};
			} else {
				// Direct invoke
				taskEvent = { name: "pgconductor.invoke", payload: exec.payload };
			}

			// Pass db and tasks as extra context to maintenance task
			const extraContext =
				task.name === "pgconductor.maintenance"
					? { ...this.extraContext, db: this.db, tasks: this.tasks }
					: this.extraContext;

			const output = await runWithSpan(consumer, () =>
				Promise.race([
					task.execute(
						taskEvent,
						TaskContext.create<Tasks, Events, typeof extraContext>(
							{
								db: this.db,
								abortController: taskAbortController,
								execution: exec,
								logger: makeChildLogger(this.logger, {
									execution_id: exec.id,
									orchestrator_id: exec.locked_by,
									task_key: exec.task_key,
									queue: exec.queue,
								}),
								window: task.window,
								telemetry: processTelemetry ? undefined : false,
							},
							extraContext,
						),
					),
					abortPromise,
				]),
			);

			if (isTaskAbortReason(output)) {
				switch (output.reason) {
					case "wait-for-event":
						this.processSpanContexts.delete(exec.id);
						return null;
					case "child-invocation":
						return {
							execution_id: exec.id,
							orchestrator_id: exec.locked_by,
							queue: exec.queue,
							task_key: exec.task_key,
							status: "invoke_child",
							timeout_ms: output.timeout_ms,
							step_key: output.step_key,
							child_task_name: output.task.name,
							child_task_queue: output.task.queue || "default",
							child_payload: output.payload,
							group: output.group,
						} as const;
					case "cancelled":
						return {
							execution_id: exec.id,
							orchestrator_id: exec.locked_by,
							queue: exec.queue,
							task_key: exec.task_key,
							status: "permanently_failed",
							cancelled: true,
							error: exec.last_error || "Task was cancelled",
						} as const;
					case "released":
					case "parent-aborted":
						return {
							execution_id: exec.id,
							orchestrator_id: exec.locked_by,
							queue: exec.queue,
							reschedule_in_ms: output.reason === "released" ? output.reschedule_in_ms : undefined,
							step_key: output.reason === "released" ? output.step_key : undefined,
							task_key: exec.task_key,
							status: "released",
						} as const;
					default:
						assert.never(output);
				}
			}

			return {
				execution_id: exec.id,
				orchestrator_id: exec.locked_by,
				queue: exec.queue,
				task_key: exec.task_key,
				status: "completed",
				result: output,
			} as const;
		} catch (err) {
			setSpanError(consumer, err);
			return {
				execution_id: exec.id,
				orchestrator_id: exec.locked_by,
				queue: exec.queue,
				task_key: exec.task_key,
				status:
					exec.attempts !== undefined && exec.attempts >= (task.maxAttempts ?? 3)
						? "permanently_failed"
						: "failed",
				cancelled: false,
				error: coerceError(err).message,
			} as const;
		} finally {
			if (processTelemetry) {
				recordConsumed(exec.queue, exec.task_key);
				recordProcessDuration(exec.queue, performance.now() - processStarted, exec.task_key);
			}
			endSpan(consumer);
			// Clean up running task tracking
			this._runningTasks.delete(exec.id);
		}
	}

	/**
	 * Execute a batch task with multiple executions.
	 *
	 * @param task - The task to execute
	 * @param taskKey - The task key
	 * @param executions - The list of executions in this batch
	 */
	private async executeBatchTask(
		task: AnyTask,
		taskKey: string,
		executions: Execution[],
	): Promise<ExecutionResult[]> {
		const processTelemetry = this.telemetry && taskKey !== MAINTENANCE_TASK_NAME;
		for (const exec of executions)
			if (exec.parent_execution_id && exec.parent_dead_letter_queue)
				this.parentDeadLetterTargets.set(exec.id, {
					sourceExecutionId: exec.parent_execution_id,
					queue: exec.parent_dead_letter_queue,
					task: exec.parent_dead_letter_task_key || exec.parent_task_key || exec.task_key,
				});
		const links = processTelemetry
			? executions.flatMap((exec) => [
					...linksFromCarrier(exec.trace_context),
					...linksFromCarrier(exec.trace_link_context),
				])
			: [];
		const consumer = processTelemetry
			? startSpan(
					`process ${this.queueName}`,
					SpanKind.CONSUMER,
					messagingAttributes(taskKey, this.queueName, "process", undefined, executions.length),
					ROOT_CONTEXT,
					links,
				)
			: null;
		const processContext = spanContext(consumer);
		if (processContext)
			for (const execution of executions)
				this.processSpanContexts.set(execution.id, processContext);
		// Build event array
		const events = executions.map((exec) => {
			if (exec.cron_expression) {
				const scheduleName = exec.dedupe_key?.split("::")[1] || "unknown";
				return { name: scheduleName };
			} else if (
				exec.payload &&
				typeof exec.payload === "object" &&
				"event" in exec.payload &&
				exec.payload.event !== "pgconductor.invoke"
			) {
				return {
					name: exec.payload.event,
					payload: exec.payload.payload,
				};
			} else {
				return { name: "pgconductor.invoke", payload: exec.payload };
			}
		});

		const taskAbortController = createTaskSignal(this.signal);

		// Create batch context
		const batchContext = new BatchTaskContext(
			taskAbortController,
			makeChildLogger(this.logger, {
				task_key: taskKey,
				queue: this.queueName,
				batch_size: executions.length,
			}),
		);

		const abortPromise = new Promise<TaskAbortReasons>((resolve) => {
			taskAbortController.signal.addEventListener("abort", () => {
				resolve(taskAbortController.signal.reason);
			});
		});

		const processStarted = performance.now();
		try {
			// Schedule next executions for cron tasks
			await Promise.all(
				executions.map((exec) => this.scheduleNextExecution(exec, consumer, processTelemetry)),
			);

			const result = await runWithSpan(consumer, () =>
				Promise.race([task.execute(events, batchContext), abortPromise]),
			);

			// Handle abort reasons
			if (isTaskAbortReason(result)) {
				if (result.reason === "released") {
					// Batch sleep - reschedule all
					return executions.map((exec) => ({
						execution_id: exec.id,
						orchestrator_id: exec.locked_by,
						queue: exec.queue,
						task_key: taskKey,
						status: "released" as const,
						reschedule_in_ms: result.reschedule_in_ms,
						step_key: result.step_key,
					}));
				}

				// Other abort reasons
				return executions.map((exec) => ({
					execution_id: exec.id,
					orchestrator_id: exec.locked_by,
					queue: exec.queue,
					task_key: taskKey,
					status: "failed" as const,
					cancelled: false,
					error: `Task aborted: ${result.reason}`,
				}));
			}

			// Void tasks: all succeed
			if (result === undefined) {
				return executions.map((exec) => ({
					execution_id: exec.id,
					orchestrator_id: exec.locked_by,
					queue: exec.queue,
					task_key: taskKey,
					status: "completed" as const,
					result: undefined,
				}));
			}

			// Tasks with returns: validate array length
			if (!Array.isArray(result)) {
				throw new Error("Batch handler must return array matching input length");
			}

			if (result.length !== executions.length) {
				throw new Error(
					`Batch handler returned ${result.length} results but received ${executions.length} executions`,
				);
			}

			// Individual results
			return executions.map((exec, i) => ({
				execution_id: exec.id,
				orchestrator_id: exec.locked_by,
				queue: exec.queue,
				task_key: taskKey,
				status: "completed" as const,
				result: result[i],
			}));
		} catch (err) {
			setSpanError(consumer, err);
			// Handler threw: all fail together
			const errorMsg = coerceError(err).message;
			return executions.map((exec) => ({
				execution_id: exec.id,
				orchestrator_id: exec.locked_by,
				queue: exec.queue,
				task_key: taskKey,
				status: "failed" as const,
				cancelled: false,
				error: errorMsg,
			}));
		} finally {
			if (processTelemetry) {
				recordConsumed(this.queueName, taskKey, executions.length);
				recordProcessDuration(this.queueName, performance.now() - processStarted, taskKey);
			}
			endSpan(consumer);
		}
	}

	private async scheduleNextExecution(
		execution: Execution,
		parent: Span | null = null,
		telemetry = this.telemetry,
	): Promise<void> {
		if (!execution.cron_expression) {
			return;
		}

		// Extract schedule name from dedupe_key (format: scheduled::{name}::{timestamp})
		if (!execution.dedupe_key || !execution.dedupe_key.startsWith("scheduled::")) {
			return;
		}

		const parts = execution.dedupe_key.split("::");
		if (parts.length < 2) {
			return;
		}

		const scheduleName = parts[1];
		const currentTime = await this.db.getCurrentTime({ signal: this.signal });
		const interval = CronExpressionParser.parse(execution.cron_expression, {
			currentDate: currentTime,
		});
		const nextTimestamp = interval.next().toDate();
		const timestampSeconds = Math.floor(nextTimestamp.getTime() / 1000);
		const nextDedupeKey = `scheduled::${scheduleName}::${timestampSeconds}`;

		const producer = telemetry
			? startSpan(
					`send ${execution.queue}`,
					SpanKind.PRODUCER,
					messagingAttributes(execution.task_key, execution.queue, "send"),
					parent ? contextForSpanContext(spanContext(parent)) : ROOT_CONTEXT,
				)
			: null;
		const started = performance.now();
		try {
			const id = await runWithSpan(producer, () =>
				this.db.invoke(
					{
						task_key: execution.task_key,
						queue: execution.queue,
						run_at: nextTimestamp,
						dedupe_key: nextDedupeKey,
						cron_expression: execution.cron_expression,
						group: execution.group || null,
						trace_context: producer
							? carrierForContext(contextForSpanContext(spanContext(producer)))
							: null,
					},
					{ signal: this.signal },
				),
			);
			if (id) {
				setSpanAttribute(producer, "messaging.message.id", id);
				if (telemetry) recordSent(execution.queue, 1, execution.task_key);
				if (telemetry)
					recordOperationDuration(
						execution.queue,
						performance.now() - started,
						"send",
						execution.task_key,
					);
			}
		} catch (error) {
			setSpanError(producer, error);
			throw error;
		} finally {
			endSpan(producer);
		}
	}

	// --- Stage 3: Flush results to database ---
	private async flushResults(source: AsyncIterable<ExecutionResult>): Promise<void> {
		let buffer = new BufferState();
		let flushTimer: ReturnType<typeof setTimeout> | null = null;

		const flushNow = async (isCleanup = false) => {
			if (buffer.count === 0) return;

			const batch = buffer;
			buffer = new BufferState();

			if (flushTimer) {
				clearTimeout(flushTimer);
				flushTimer = null;
			}

			if (this.telemetry) {
				for (const result of [
					...batch.invokeChild,
					...batch.failed.filter((item) => {
						if (item.status !== "permanently_failed") return false;
						const task = this.tasks.get(item.task_key);
						return Boolean(task?.deadLetter?.queue && !item.cancelled);
					}),
				]) {
					const existing = this.pendingDurableProducers.get(result.execution_id);
					const parent = this.processSpanContexts.get(result.execution_id);
					const deadLetter =
						result.status === "invoke_child"
							? null
							: this.tasks.get(result.task_key)?.deadLetter || null;
					const targets =
						result.status === "invoke_child"
							? [
									{
										sourceExecutionId: result.execution_id,
										queue: result.child_task_queue,
										task: result.child_task_name,
									},
								]
							: [
									...(deadLetter?.queue && !result.cancelled
										? [
												{
													sourceExecutionId: result.execution_id,
													queue: deadLetter.queue,
													task: deadLetter.task?.name || result.task_key,
												},
											]
										: []),
									...(result.status === "permanently_failed" && !result.cancelled
										? (() => {
												const target = this.parentDeadLetterTargets.get(result.execution_id);
												return target ? [target] : [];
											})()
										: []),
								];
					for (const target of targets) {
						const producer =
							(target.sourceExecutionId === result.execution_id ? existing : undefined) ||
							startSpan(
								`send ${target.queue}`,
								SpanKind.PRODUCER,
								messagingAttributes(target.task, target.queue, "send"),
								parent ? contextForSpanContext(parent) : ROOT_CONTEXT,
							);
						if (producer) {
							this.pendingDurableProducers.set(target.sourceExecutionId, producer);
							const carrier = carrierForContext(contextForSpanContext(spanContext(producer)));
							if (carrier) {
								if (result.status === "invoke_child") result.trace_context = carrier;
								else {
									result.dead_letter_trace_contexts ||= {};
									result.dead_letter_trace_contexts[target.sourceExecutionId] = carrier;
								}
							}
						}
					}
				}
			}
			const links = this.telemetry
				? Array.from(
						new Set(
							[...batch.completed, ...batch.failed, ...batch.released, ...batch.invokeChild]
								.map((result) => this.processSpanContexts.get(result.execution_id))
								.filter((value): value is SpanContext => value !== undefined),
						),
					).map((context) => ({ context }))
				: [];
			const settle =
				this.telemetry && links.length
					? startSpan(
							`settle ${this.queueName}`,
							SpanKind.CLIENT,
							messagingAttributes(undefined, this.queueName, "settle", undefined, batch.count),
							ROOT_CONTEXT,
							links,
						)
					: null;
			let settled = false;
			const settleStarted = performance.now();
			try {
				batch.orchestratorId = this.orchestratorId || batch.orchestratorId;
				const committed = await runWithSpan(settle, () =>
					this.db.returnExecutions(batch, { signal: this.signal }),
				);
				const hasUserResults = Array.from(batch.taskKeys).some(
					(key) => key !== MAINTENANCE_TASK_NAME,
				);
				if (this.telemetry) {
					for (const delivery of committed?.deliveries || []) {
						const producer = this.pendingDurableProducers.get(delivery.sourceExecutionId);
						setSpanAttribute(
							producer || null,
							"messaging.message.id",
							delivery.destinationExecutionId,
						);
						endSpan(producer || null);
						this.pendingDurableProducers.delete(delivery.sourceExecutionId);
					}
				}
				if (this.telemetry && hasUserResults) {
					recordOperationDuration(this.queueName, performance.now() - settleStarted, "settle");
					for (const outcome of committed?.outcomes || []) {
						if (outcome.task_key === MAINTENANCE_TASK_NAME) continue;
						recordLifecycle(
							outcome.queue,
							outcome.outcome,
							outcome.task_key,
							Number(outcome.count),
						);
					}
				}
				settled = true;
				setSpanAttribute(settle, "pgconductor.db.commit.status", "success");
			} catch (err) {
				setSpanError(settle, err);
				this.logger.error("Error flushing results:", err);
				if (!isCleanup) {
					buffer.restore(batch);
				}
			} finally {
				if (settled || isCleanup) {
					const pending = isCleanup
						? [...this.pendingDurableProducers.entries()]
						: [...batch.completed, ...batch.failed, ...batch.released, ...batch.invokeChild].map(
								(result) =>
									[
										result.execution_id,
										this.pendingDurableProducers.get(result.execution_id),
									] as const,
							);
					for (const [executionId, producer] of pending) {
						endSpan(producer || null);
						this.pendingDurableProducers.delete(executionId);
					}
				}
				endSpan(settle);
				if (settled || isCleanup) {
					for (const result of [
						...batch.completed,
						...batch.failed,
						...batch.released,
						...batch.invokeChild,
					]) {
						this.processSpanContexts.delete(result.execution_id);
						this.parentDeadLetterTargets.delete(result.execution_id);
					}
				}
			}
		};

		const runFlush = (isCleanup = false): Promise<void> => {
			const next = this.flushInFlight.then(() => flushNow(isCleanup));
			this.flushInFlight = next.catch(() => {});
			return next;
		};

		const scheduleFlush = () => {
			if (flushTimer) clearTimeout(flushTimer);
			flushTimer = setTimeout(() => {
				void runFlush();
			}, this.flushIntervalMs);
		};

		try {
			for await (const result of source) {
				buffer.add(result);

				// Start timer only after first result arrives
				if (!flushTimer) scheduleFlush();

				if (buffer.count >= this.flushBatchSize) {
					await runFlush();
					scheduleFlush();
				}
			}
		} finally {
			if (flushTimer) clearTimeout(flushTimer);
			await runFlush(true);
		}
	}

	private get abortController(): AbortController {
		if (!this._abortController) {
			throw new Error("Worker is not running");
		}
		return this._abortController;
	}

	private get signal(): AbortSignal {
		return this.abortController.signal;
	}
}
