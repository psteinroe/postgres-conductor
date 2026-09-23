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
import { type PollableAsyncIterable } from "./lib/async-queue";
import { BatchingAsyncQueue, type BatchGroup } from "./lib/batching-async-queue";
import { nextCronOccurrence } from "./lib/cron";
import { Clock } from "./lib/clock";
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
	private readonly clock: Clock;

	private _startDeferred: Deferred<void> | null = null;
	private _stopDeferred: Deferred<void> | null = null;
	private _abortController: AbortController | null = null;
	private _drainDidWork = false;
	private _runningTasks = new Map<string, TypedAbortController<TaskAbortReasons>>();

	constructor(
		public readonly queueName: string,
		tasks: readonly AnyTask[],
		private readonly db: DatabaseClient,
		private readonly logger: Logger,
		config: Partial<WorkerConfig> = {},
		private readonly extraContext: object = {},
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

		// Startup failures reject both lifecycle promises; callers must never
		// observe a worker that started partially.
		try {
			// Sample before calculating or registering cron schedules.
			await this.clock.start(this.abortController.signal);
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
			void this.runDrainPipeline(queue);
		} else {
			void this.fetchExecutions(queue, { runOnce });
			void (async () => {
				try {
					await this.flushResults(this.executeTasks(queue));
				} catch (error) {
					this.logger.error("Worker pipeline error:", error);
					this._stopDeferred?.reject(error);
				} finally {
					queue.close();
					if (this._stopDeferred && !this._stopDeferred.isSettled) this._stopDeferred.resolve();
					this.resetLifecycle();
				}
			})();
		}

		return this._startDeferred.promise;
	}

	private async runDrainPipeline(queue: BatchingAsyncQueue<Execution>): Promise<void> {
		try {
			const fetched = this.fetchExecutions(queue, { runOnce: true });
			await this.flushResults(this.executeTasks(queue));
			this._drainDidWork = (await fetched) > 0;
		} catch (error) {
			this.logger.error("Worker pipeline error:", error);
			this._stopDeferred?.reject(error);
		} finally {
			if (this._stopDeferred && !this._stopDeferred.isSettled) this._stopDeferred.resolve();
			this.resetLifecycle();
		}
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
	): Promise<number> {
		let fetched = 0;
		assert.ok(this.orchestratorId, "orchestratorId must be set when starting the pipeline");

		// Pre-compute task metadata once
		const allTasks = Array.from(this.tasks.values());
		const taskMaxAttempts: Record<string, number> = {};
		for (const task of allTasks) {
			taskMaxAttempts[task.name] = task.maxAttempts || 3;
		}
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
			async ({ taskKey, items: executions }): Promise<ExecutionResult | ExecutionResult[]> => {
				// Dispatch to correct task based on task_key
				const task = this.tasks.get(taskKey);
				if (!task) {
					return executions.map((exec) => ({
						queue: exec.queue,
						execution_id: exec.id,
						orchestrator_id: exec.locked_by,
						task_key: taskKey,
						status: "failed",
						error: `Task not found: ${taskKey}`,
					})) as ExecutionResult[];
				}

				// Safety check: don't execute already-cancelled tasks
				const cancelledExecs = executions.filter((e) => e.cancelled);
				if (cancelledExecs.length === executions.length) {
					// All cancelled - return failures for all
					return executions.map((exec) => ({
						execution_id: exec.id,
						orchestrator_id: exec.locked_by,
						queue: exec.queue,
						task_key: taskKey,
						status: "permanently_failed",
						error: exec.last_error || "Execution was cancelled",
					})) as ExecutionResult[];
				}

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
					return this.executeBatchTask(task, taskKey, activeExecs);
				}

				// Execute single (non-batched tasks)
				const singleExec = activeExecs[0];
				assert.ok(singleExec, "activeExecs must have at least one item");
				return this.executeSingleTask(task, singleExec);
			},
		)) {
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
	private async executeSingleTask(task: AnyTask, exec: Execution): Promise<ExecutionResult> {
		const taskAbortController = createTaskSignal(this.signal);
		this._runningTasks.set(exec.id, taskAbortController);

		const abortPromise = new Promise<TaskAbortReasons>((resolve) => {
			taskAbortController.signal.addEventListener("abort", () => {
				resolve(taskAbortController.signal.reason);
			});
		});

		try {
			await this.scheduleNextExecution(exec);

			// Determine event type based on execution data
			let taskEvent: any;
			if (exec.cron_expression) {
				// Extract schedule name from dedupe_key (format: scheduled::{name}::{timestamp})
				const scheduleName = exec.dedupe_key?.split("::")[1] || "unknown";
				taskEvent = { name: scheduleName };
			} else if (
				exec.subscription_id != null &&
				exec.payload &&
				typeof exec.payload === "object" &&
				"event" in exec.payload
			) {
				// Event-triggered executions carry dedicated database identity.
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

			const output = await Promise.race([
				task.execute(
					taskEvent,
					TaskContext.create<Tasks, Events, typeof extraContext>(
						{
							db: this.db,
							clock: this.clock,
							abortController: taskAbortController,
							execution: exec,
							logger: makeChildLogger(this.logger, {
								execution_id: exec.id,
								orchestrator_id: exec.locked_by,
								task_key: exec.task_key,
								queue: exec.queue,
							}),
							window: task.window,
						},
						extraContext,
					),
				),
				abortPromise,
			]);

			if (isTaskAbortReason(output)) {
				switch (output.reason) {
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
			return {
				execution_id: exec.id,
				orchestrator_id: exec.locked_by,
				queue: exec.queue,
				task_key: exec.task_key,
				status: "failed",
				error: coerceError(err).message,
			} as const;
		} finally {
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
		// Build event array
		const events = executions.map((exec) => {
			let event;
			if (exec.cron_expression) {
				const scheduleName = exec.dedupe_key?.split("::")[1] || "unknown";
				event = { name: scheduleName };
			} else if (
				exec.subscription_id != null &&
				exec.payload &&
				typeof exec.payload === "object" &&
				"event" in exec.payload
			) {
				event = { name: exec.payload.event, payload: exec.payload.payload };
			} else {
				event = { name: "pgconductor.invoke", payload: exec.payload };
			}
			return {
				...event,
				execution: {
					id: exec.id,
					queue: exec.queue,
					task_key: exec.task_key,
					locked_by: exec.locked_by,
				},
			};
		});

		const taskAbortController = createTaskSignal(this.signal);

		// Create batch context
		const batchContext = BatchTaskContext.create(
			taskAbortController,
			makeChildLogger(this.logger, {
				task_key: taskKey,
				queue: this.queueName,
				batch_size: executions.length,
			}),
			this.extraContext,
		);

		const abortPromise = new Promise<TaskAbortReasons>((resolve) => {
			taskAbortController.signal.addEventListener("abort", () => {
				resolve(taskAbortController.signal.reason);
			});
		});

		try {
			// Schedule next executions for cron tasks
			await Promise.all(executions.map((exec) => this.scheduleNextExecution(exec)));

			const result = await Promise.race([task.execute(events, batchContext), abortPromise]);

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
			// Handler threw: all fail together
			const errorMsg = coerceError(err).message;
			return executions.map((exec) => ({
				execution_id: exec.id,
				orchestrator_id: exec.locked_by,
				queue: exec.queue,
				task_key: taskKey,
				status: "failed" as const,
				error: errorMsg,
			}));
		}
	}

	private async scheduleNextExecution(execution: Execution): Promise<void> {
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
		const nextTimestamp = nextCronOccurrence(execution.cron_expression, this.clock.now());
		const timestampSeconds = Math.floor(nextTimestamp.getTime() / 1000);
		const nextDedupeKey = `scheduled::${scheduleName}::${timestampSeconds}`;

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
		let buffer = new BufferState();
		let flushTimer: Timer | null = null;

		const flushNow = async (isCleanup = false) => {
			if (buffer.count === 0) return;

			const batch = buffer;
			buffer = new BufferState();

			if (flushTimer) {
				clearTimeout(flushTimer);
				flushTimer = null;
			}

			try {
				batch.orchestratorId = this.orchestratorId || batch.orchestratorId;
				await this.db.returnExecutions(batch, { signal: this.signal });
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
