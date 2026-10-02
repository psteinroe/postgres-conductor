import type {
	DatabaseClient,
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
import type { TaskDefinition } from "./task-definition";
import { waitFor } from "./lib/wait-for";
import { mapConcurrent } from "./lib/map-concurrent";
import { Deferred } from "./lib/deferred";
import { BatchingAsyncQueue } from "./lib/batching-async-queue";
import { nextCronOccurrence } from "./lib/cron";
import { Clock } from "./lib/clock";
import {
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
import { TypedAbortController } from "./lib/typed-abort-controller";
import { Telemetry } from "./telemetry";
import { noop } from "./lib/noop";

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
export const DEFAULT_WORKER_CONFIG: WorkerConfig = {
	concurrency: 1,
	flushBatchSize: 2,
	fetchBatchSize: 2,
	pollIntervalMs: 1000,
	flushIntervalMs: 2000,
};

/**
 * Encapsulates buffered execution results with internal counting.
 */
class BufferState {
	completed: ExecutionCompleted[] = [];
	failed: (ExecutionFailed | ExecutionPermamentlyFailed)[] = [];
	released: ExecutionReleased[] = [];
	invokeChild: ExecutionInvokeChild[] = [];
	count = 0;

	constructor(readonly orchestratorId: string) {}

	add(result: ExecutionResult): void {
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

	restore(other: BufferState): void {
		this.completed.push(...other.completed);
		this.failed.push(...other.failed);
		this.released.push(...other.released);
		this.invokeChild.push(...other.invokeChild);
		this.count += other.count;
	}
}

function taskEventFor(execution: Execution): { name: string; payload?: unknown } {
	if (execution.cron_expression) {
		return { name: execution.dedupe_key?.split("::")[1] || "unknown" };
	}
	if (
		execution.subscription_id != null &&
		execution.payload &&
		typeof execution.payload === "object" &&
		"event" in execution.payload
	) {
		return {
			name: execution.payload.event as string,
			payload: execution.payload.payload,
		};
	}
	return { name: "pgconductor.invoke", payload: execution.payload };
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
	private readonly clock: Clock;

	private _startDeferred: Deferred<void> | null = null;
	private _stopDeferred: Deferred<void> | null = null;
	private _abortController: AbortController | null = null;
	private _runningTasks = new Map<string, TypedAbortController<TaskAbortReasons>>();

	constructor(
		public readonly queueName: string,
		tasks: readonly AnyTask[],
		private readonly db: DatabaseClient,
		private readonly logger: Logger,
		config: Partial<WorkerConfig> = {},
		private readonly extraContext: object = {},
		private readonly telemetry = new Telemetry(),
	) {
		const maintenanceTask = createMaintenanceTask(this.queueName);
		this.tasks = tasks.reduce(
			(registered, task) => {
				registered.set(task.name, task);
				return registered;
			},
			new Map<string, AnyTask>([[maintenanceTask.name, maintenanceTask]]),
		);

		const fullConfig = { ...DEFAULT_WORKER_CONFIG, ...config };

		this.concurrency = fullConfig.concurrency;
		this.pollIntervalMs = fullConfig.pollIntervalMs;
		this.flushIntervalMs = fullConfig.flushIntervalMs;
		this.fetchBatchSize = fullConfig.fetchBatchSize;
		this.flushBatchSize = fullConfig.flushBatchSize;
		this.clock = new Clock({
			sampleDatabaseTime: (signal) => this.db.getDatabaseTime({ signal }),
			logger: this.logger,
		});
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
		this._startDeferred = new Deferred<void>();
		this._stopDeferred = new Deferred<void>();
		this._abortController = new AbortController();

		try {
			// Sample before calculating cron schedules
			await this.clock.start(this.signal);
			await this.register();
		} catch (error) {
			this._startDeferred.promise.catch(noop);
			this._startDeferred.reject(error);
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
		void this.fetchExecutions(queue, { runOnce });
		void (async () => {
			try {
				await this.flushResults(this.executeTasks(queue));
			} catch (error) {
				this.logger.error("Worker pipeline error:", error);
			} finally {
				queue.close();
				this._stopDeferred?.resolve();
				this.resetLifecycle();
			}
		})();

		return this._startDeferred.promise;
	}

	private resetLifecycle(): void {
		this.clock.stop();
		this._startDeferred = null;
		this._stopDeferred = null;
		this._abortController = null;
		this.orchestratorId = null;
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

		const cronSchedules: ExecutionSpec[] = allTasks.flatMap((task) =>
			task.triggers
				.filter((t): t is { cron: string; name: string; group?: string } => "cron" in t)
				.map((trigger) => {
					const nextTimestamp = nextCronOccurrence(trigger.cron, this.clock.now());
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

		const eventSubscriptions: EventSubscriptionSpec[] = allTasks.flatMap((task) =>
			(task.eventTriggers ?? []).map((spec) => ({
				task_key: task.name,
				event_key: spec.event_key,
				payload_fields: spec.payload_fields,
				required_field_count: spec.required_field_count,
				terms: spec.terms,
			})),
		);

		await this.db.registerWorker(
			{
				queueName: this.queueName,
				taskSpecs,
				cronSchedules,
				eventSubscriptions,
			},
			{ signal: this.signal },
		);
	}

	// --- Stage 1: Fetch executions from database ---
	private async fetchExecutions(
		queue: BatchingAsyncQueue<Execution>,
		{ runOnce = false }: { runOnce?: boolean },
	) {
		assert.ok(this.orchestratorId, "orchestratorId must be set when starting the pipeline");

		const allTasks = Array.from(this.tasks.values());
		// A slot runs one execution, or one batch of a batched task
		const executionsPerSlot = Math.max(...allTasks.map((task) => task.batch?.size || 1));

		while (!this.signal?.aborted) {
			try {
				// Claim only what free slots can run so other workers get the rest
				const freeSlots = this.concurrency - queue.pending;
				if (freeSlots <= 0) {
					await queue.waitForRelease();
					continue;
				}

				// Claim only registered tasks that are within their time window
				const currentTime = this.clock.now().toISOString().slice(11, 19);
				const taskKeys = allTasks
					.filter(
						(task) =>
							!task.window || (currentTime >= task.window[0] && currentTime < task.window[1]),
					)
					.map((task) => task.name);

				const executions = await this.db.getExecutions(
					{
						orchestratorId: this.orchestratorId,
						queueName: this.queueName,
						batchSize: Math.min(this.fetchBatchSize, freeSlots * executionsPerSlot),
						taskKeys,
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
					await queue.push(exec); // waits if full
					if (this.signal.aborted) break;
				}
			} catch {
				await waitFor(2000, { signal: this.signal });
			}
		}

		queue.close();
	}

	// --- Stage 2: Execute tasks concurrently ---
	private async *executeTasks(
		queue: BatchingAsyncQueue<Execution>,
	): AsyncGenerator<ExecutionResult> {
		for await (const result of mapConcurrent(
			queue,
			this.concurrency,
			async ({ taskKey, items: executions }): Promise<ExecutionResult | ExecutionResult[]> => {
				// Dispatch to correct task based on task_key
				const task = this.tasks.get(taskKey);
				assert.ok(task, `claimed an execution of unregistered task ${taskKey}`);

				// Don't execute already-cancelled tasks
				const cancelled: ExecutionResult[] = executions
					.filter((e) => e.cancelled)
					.map((exec) => ({
						execution_id: exec.id,
						queue: exec.queue,
						task_key: taskKey,
						status: "permanently_failed",
						error: exec.last_error || "Execution was cancelled",
					}));
				const activeExecs = executions.filter((e) => !e.cancelled);
				if (activeExecs.length === 0) {
					return cancelled;
				}

				// Don't start executions claimed before shutdown
				if (this.signal.aborted) {
					return [
						...cancelled,
						...activeExecs.map((exec) => ({
							execution_id: exec.id,
							queue: exec.queue,
							task_key: taskKey,
							status: "released" as const,
						})),
					];
				}

				// If task has batch config, always use batch execution (even for single items)
				if (task.batch) {
					return [...cancelled, ...(await this.executeBatchTask(task, taskKey, activeExecs))];
				}

				// Execute single (non-batched tasks)
				const singleExec = activeExecs[0];
				assert.ok(singleExec, "activeExecs must have at least one item");
				return this.executeSingleTask(task, singleExec);
			},
		)) {
			queue.release();

			// Yield results (may be single or array)
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
	private async executeSingleTask(
		task: AnyTask,
		exec: Execution,
	): Promise<ExecutionResult | ExecutionResult[]> {
		const taskAbortController = new TypedAbortController<TaskAbortReasons>();
		const signal = AbortSignal.any([taskAbortController.signal, this.signal]);
		this._runningTasks.set(exec.id, taskAbortController);

		const abortPromise = new Promise<TaskAbortReasons>((resolve) => {
			taskAbortController.signal.addEventListener("abort", () => {
				resolve(taskAbortController.signal.reason);
			});
		});
		const taskEvent = taskEventFor(exec);

		// Pass db and tasks as extra context to maintenance task
		const extraContext =
			task.name === "pgconductor.maintenance"
				? { ...this.extraContext, db: this.db, tasks: this.tasks }
				: this.extraContext;

		try {
			await this.scheduleNextExecution(exec);

			const output = await this.telemetry.process({
				taskKey: exec.task_key,
				queue: exec.queue,
				messageId: exec.id,
				traceContexts: [exec.trace_context],
				run: () =>
					Promise.race([
						task.execute(
							taskEvent,
							TaskContext.create<Tasks, Events, typeof extraContext>(
								{
									db: this.db,
									clock: this.clock,
									abortController: taskAbortController,
									signal,
									execution: exec,
									logger: makeChildLogger(this.logger, {
										execution_id: exec.id,
										orchestrator_id: exec.locked_by,
										task_key: exec.task_key,
										queue: exec.queue,
									}),
									eventDefinitions: task.eventDefinitions,
									window: task.window,
									telemetry: this.telemetry,
								},
								extraContext,
							),
						),
						abortPromise,
					]),
			});

			if (isTaskAbortReason(output)) {
				switch (output.reason) {
					case "child-invocation":
						return {
							execution_id: exec.id,
							queue: exec.queue,
							task_key: exec.task_key,
							status: "invoke_child",
							timeout_ms: output.timeout_ms,
							step_key: output.step_key,
							child_task_name: output.task.name,
							child_task_queue: output.task.queue || "default",
							child_payload: output.payload,
							group: output.group,
							trace_context: output.trace_context,
						} as const;
					case "cancelled":
						return {
							execution_id: exec.id,
							queue: exec.queue,
							task_key: exec.task_key,
							status: "permanently_failed",
							error: exec.last_error || "Task was cancelled",
						} as const;
					case "suspended":
						return [];
					case "released":
						return {
							execution_id: exec.id,
							queue: exec.queue,
							reschedule_in_ms: output.reschedule_in_ms,
							step_key: output.step_key,
							task_key: exec.task_key,
							status: "released",
						} as const;
					default:
						assert.never(output);
				}
			}

			return {
				execution_id: exec.id,
				queue: exec.queue,
				task_key: exec.task_key,
				status: "completed",
				result: output,
			} as const;
		} catch (err) {
			// A handler that stops on ctx.signal during shutdown is released, not failed
			if (this.signal.aborted) {
				return {
					execution_id: exec.id,
					queue: exec.queue,
					task_key: exec.task_key,
					status: "released",
				} as const;
			}
			return {
				execution_id: exec.id,
				queue: exec.queue,
				task_key: exec.task_key,
				status: "failed",
				error: coerceError(err).message,
			} as const;
		} finally {
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
		const events = executions.map((exec) => {
			return {
				...taskEventFor(exec),
				execution: {
					id: exec.id,
					queue: exec.queue,
					task_key: exec.task_key,
					locked_by: exec.locked_by,
				},
			};
		});

		const taskAbortController = new TypedAbortController<TaskAbortReasons>();

		// Create batch context
		const batchContext = new BatchTaskContext(
			taskAbortController,
			AbortSignal.any([taskAbortController.signal, this.signal]),
			makeChildLogger(this.logger, {
				task_key: taskKey,
				queue: this.queueName,
				batch_size: executions.length,
			}),
			this.db,
			executions,
		);

		const abortPromise = new Promise<TaskAbortReasons>((resolve) => {
			taskAbortController.signal.addEventListener("abort", () => {
				resolve(taskAbortController.signal.reason);
			});
		});

		try {
			// Schedule next executions for cron tasks
			await Promise.all(executions.map((exec) => this.scheduleNextExecution(exec)));

			const result = await this.telemetry.process({
				taskKey,
				queue: this.queueName,
				batchMessageCount: executions.length,
				traceContexts: executions.map((exec) => exec.trace_context),
				run: async () => {
					const result = await Promise.race([task.execute(events, batchContext), abortPromise]);
					if (!isTaskAbortReason(result) && result !== undefined) {
						if (!Array.isArray(result)) {
							throw new Error("Batch handler must return array matching input length");
						}
						if (result.length !== executions.length) {
							throw new Error(
								`Batch handler returned ${result.length} results but received ${executions.length} executions`,
							);
						}
					}
					return result;
				},
			});

			// Handle abort reasons
			if (isTaskAbortReason(result)) {
				if (result.reason === "released") {
					// Batch sleep - reschedule all
					return executions.map((exec) => ({
						execution_id: exec.id,
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
					queue: exec.queue,
					task_key: taskKey,
					status: "failed" as const,
					error: `Task aborted: ${result.reason}`,
				}));
			}

			// Void tasks: all succeed
			if (result === undefined) {
				return executions.map((exec) => ({
					execution_id: exec.id,
					queue: exec.queue,
					task_key: taskKey,
					status: "completed" as const,
					result: undefined,
				}));
			}

			// Individual results
			return executions.map((exec, i) => ({
				execution_id: exec.id,
				queue: exec.queue,
				task_key: taskKey,
				status: "completed" as const,
				result: result[i],
			}));
		} catch (err) {
			if (this.signal.aborted) {
				return executions.map((exec) => ({
					execution_id: exec.id,
					queue: exec.queue,
					task_key: taskKey,
					status: "released" as const,
				}));
			}

			// Handler threw: all fail together
			const error = coerceError(err).message;
			return executions.map((exec) => ({
				execution_id: exec.id,
				queue: exec.queue,
				task_key: taskKey,
				status: "failed" as const,
				error,
			}));
		}
	}

	private async scheduleNextExecution(execution: Execution): Promise<void> {
		if (!execution.cron_expression) {
			return;
		}

		// dedupe_key format: {scheduled|dynamic}::{name}::{timestamp}
		const [prefix, scheduleName] = execution.dedupe_key?.split("::") || [];
		if (prefix !== "scheduled" && prefix !== "dynamic") {
			return;
		}

		const nextTimestamp = nextCronOccurrence(execution.cron_expression, this.clock.now());
		const timestampSeconds = Math.floor(nextTimestamp.getTime() / 1000);
		const nextDedupeKey = `${prefix}::${scheduleName}::${timestampSeconds}`;

		await this.db.invoke(
			{
				task_key: execution.task_key,
				queue: execution.queue,
				run_at: nextTimestamp,
				dedupe_key: nextDedupeKey,
				cron_expression: execution.cron_expression,
				group: execution.group || null,
			},
			{ signal: this.signal },
		);
	}

	// --- Stage 3: Flush results to database ---
	private async flushResults(source: AsyncIterable<ExecutionResult>): Promise<void> {
		const orchestratorId = this.orchestratorId;
		assert.ok(orchestratorId, "orchestratorId must be set when starting the pipeline");

		let buffer = new BufferState(orchestratorId);
		let flushTimer: ReturnType<typeof setTimeout> | null = null;

		const flushNow = async (isCleanup = false) => {
			if (flushTimer) {
				clearTimeout(flushTimer);
				flushTimer = null;
			}

			if (buffer.count === 0) return;

			const batch = buffer;
			buffer = new BufferState(orchestratorId);

			try {
				await this.telemetry.settle({
					queue: this.queueName,
					batchMessageCount: batch.count,
					run: () => this.db.returnExecutions(batch, { signal: this.signal }),
				});
			} catch (err) {
				this.logger.error("Error flushing results:", err);
				if (!isCleanup) {
					buffer.restore(batch);
				}
			}
		};

		const scheduleFlush = () => {
			if (flushTimer) clearTimeout(flushTimer);
			flushTimer = setTimeout(async () => {
				await flushNow();
			}, this.flushIntervalMs);
		};

		try {
			for await (const result of source) {
				buffer.add(result);

				// Start timer only after first result arrives
				if (!flushTimer) scheduleFlush();

				if (buffer.count >= this.flushBatchSize) {
					await flushNow();
					scheduleFlush();
				}
			}
		} finally {
			if (flushTimer) clearTimeout(flushTimer);
			await flushNow(true);
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
