import type {
	DatabaseClient,
	EventSubscriptionSpec,
	Execution,
	ExecutionResult,
	ExecutionSpec,
	Payload,
} from "./database-client.js";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import type { AnyTask, BatchConfig } from "./task.js";
import type { TaskDefinition } from "./task-definition.js";
import { waitFor } from "./lib/wait-for.js";
import { mapConcurrent } from "./lib/map-concurrent.js";
import { Deferred } from "./lib/deferred.js";
import { BatchingAsyncQueue } from "./lib/batching-async-queue.js";
import { nextCronOccurrence } from "./lib/cron.js";
import { Clock } from "./lib/clock.js";
import { WindowChecker } from "./lib/window-checker.js";
import { TaskContext, BatchTaskContext, type TaskAbortReasons } from "./task-context.js";
import * as assert from "./lib/assert.js";
import { createMaintenanceTask } from "./maintenance-task.js";
import { makeChildLogger, type Logger } from "./lib/logger.js";
import type { EventDefinition } from "./event-definition.js";
import { coerceError } from "./lib/coerce-error.js";
import { TypedAbortController } from "./lib/typed-abort-controller.js";
import { Telemetry } from "./telemetry.js";
import { noop } from "./lib/noop.js";
import type { Middleware, MiddlewareResult } from "./middleware.js";
import { SignalInbox } from "./signal-inbox.js";
import { parseSchema } from "./lib/validate-schema.js";

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

// How long the final flush retries settling results the worker still holds
const FINAL_FLUSH_TIMEOUT_MS = 10_000;

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

// A value the schema rejects fails the execution without retries; a validator that throws fails the attempt
async function parseOrFail(
	schema: StandardSchemaV1 | undefined,
	value: unknown,
	label: string,
): Promise<{ value: unknown } | { status: "failed" | "permanently_failed"; error: string }> {
	try {
		const parsed = await parseSchema(schema, value, label);
		return "error" in parsed ? { status: "permanently_failed", error: parsed.error } : parsed;
	} catch (err) {
		return { status: "failed", error: coerceError(err).message };
	}
}

// Invocations carry the payload of the task's schema, events and cron occurrences do not
async function parsePayload(task: AnyTask, event: { name: string; payload?: unknown }) {
	if (event.name !== "pgconductor.invoke") return { value: event.payload };
	return parseOrFail(task.schemas.payload, event.payload, `payload for task "${task.name}"`);
}

function parseResult(task: AnyTask, result: unknown) {
	return parseOrFail(task.schemas.returns, result, `return value of task "${task.name}"`);
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
	// Executions polled for cancellation; members of a batch share its controller and have no inbox
	private _running = new Map<
		string,
		{ controller: TypedAbortController<TaskAbortReasons>; claimToken: string; inbox?: SignalInbox }
	>();
	// Claim token of each execution claimed and not yet settled, including results buffered for flush
	private readonly held = new Map<string, string>();

	constructor(
		public readonly queueName: string,
		tasks: readonly AnyTask[],
		private readonly db: DatabaseClient,
		private readonly logger: Logger,
		config: Partial<WorkerConfig> = {},
		private readonly extraContext: object = {},
		private readonly telemetry = new Telemetry(),
		private readonly middleware: readonly Middleware<any, any>[] = [],
		clock?: { now(): Date },
	) {
		const maintenanceTask = createMaintenanceTask(this.queueName);
		this.tasks = tasks.reduce(
			(registered, task) => {
				if (task.queue !== this.queueName) {
					throw new Error(
						`Task "${task.name}" belongs to queue "${task.queue}", not "${this.queueName}"`,
					);
				}
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
			localClock: clock && (() => clock.now()),
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
		const stopDeferred = new Deferred<void>();
		// A failed final flush rejects it; callers that never await it must not see an unhandled rejection
		stopDeferred.promise.catch(noop);
		this._stopDeferred = stopDeferred;
		const abortController = new AbortController();
		this._abortController = abortController;

		try {
			// Sample before calculating cron schedules
			await this.clock.start(this.signal);
			await this.register();
		} catch (error) {
			this._startDeferred.promise.catch(noop);
			this._startDeferred.reject(error);
			// stop() waits for this while registration retries
			stopDeferred.resolve();
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
		const fetching = this.fetchExecutions(queue, { runOnce });
		void (async () => {
			let failure: unknown = null;
			try {
				await this.flushResults(this.executeTasks(queue));
			} catch (error) {
				this.logger.error("Worker pipeline error:", error);
				failure = error;
			} finally {
				queue.close();
				// Ends the polling of running executions that a drain keeps up after its last claim
				abortController.abort();
				await fetching;
				this.resetLifecycle();
				if (failure) stopDeferred.reject(failure);
				else stopDeferred.resolve();
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

		const orchestratorId = this.orchestratorId;
		const allTasks = Array.from(this.tasks.values());
		// A slot runs one execution, or one batch of a batched task
		const executionsPerSlot = Math.max(...allTasks.map((task) => task.batch?.size || 1));
		const windows = allTasks.map((task) => ({
			name: task.name,
			window: task.window && new WindowChecker(task.window),
		}));

		// Claim executions, abort cancelled running ones and hand new signals to their handlers
		const claim = async (batchSize: number, taskKeys: string[]) => {
			const running = [...this._running].map(([id, { claimToken, inbox }]) => ({
				execution_id: id,
				claim_token: claimToken,
				after_seq: inbox ? inbox.afterSeq : null,
			}));
			if (batchSize === 0 && running.length === 0) return [];
			const { executions, signals, cancelled } = await this.db.getExecutions({
				orchestratorId,
				queueName: this.queueName,
				batchSize,
				taskKeys,
				running,
				held: [...this.held.keys()],
			});
			for (const id of cancelled) this._running.get(id)?.controller.abort({ reason: "cancelled" });
			for (const stored of signals) this._running.get(stored.execution_id)?.inbox?.push(stored);
			for (const exec of executions) this.held.set(exec.id, exec.claim_token);
			return executions;
		};

		// A drain stops claiming once a claim finds nothing, but keeps polling running executions
		let intake = true;
		while (!this.signal.aborted) {
			try {
				if (!intake) {
					await claim(0, []);
					await waitFor(this.pollIntervalMs, { signal: this.signal });
					continue;
				}

				// Claim only what free slots can run so other workers get the rest
				const freeSlots = this.concurrency - queue.pending;
				if (freeSlots <= 0) {
					// Read cancellations and signals of running executions every poll interval
					await claim(0, []);
					await Promise.race([
						queue.waitForRelease(),
						waitFor(this.pollIntervalMs, { signal: this.signal }),
					]);
					continue;
				}

				// Claim only registered tasks that are within their time window
				const now = this.clock.now();
				const taskKeys = windows
					.filter(({ window }) => !window || window.isWithinWindow(now))
					.map(({ name }) => name);

				const executions = await claim(
					Math.min(this.fetchBatchSize, freeSlots * executionsPerSlot),
					taskKeys,
				);

				if (executions.length === 0) {
					if (runOnce) {
						queue.close();
						intake = false;
						continue;
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
						claim_token: exec.claim_token,
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
							claim_token: exec.claim_token,
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
		const timedOut = new Deferred<void>();
		const timeout = task.timeoutMs
			? setTimeout(() => {
					timedOut.resolve();
					taskAbortController.abort({ reason: "timed-out" });
				}, task.timeoutMs)
			: undefined;
		const steps = new Set<Promise<unknown>>();

		// Wrapped, so no handler output can be mistaken for an abort
		const aborted = new Promise<{ abort: TaskAbortReasons }>((resolve) => {
			taskAbortController.signal.addEventListener("abort", () => {
				resolve({ abort: taskAbortController.signal.reason });
			});
		});
		const taskEvent = taskEventFor(exec);
		const identity = {
			execution_id: exec.id,
			claim_token: exec.claim_token,
			task_key: exec.task_key,
		};
		const signals = new SignalInbox({
			ack: (id) =>
				this.db.ackSignals(
					{
						executionId: exec.id,
						queue: exec.queue,
						orchestratorId: exec.locked_by,
						claimToken: exec.claim_token,
						id,
					},
					{ signal: this.signal },
				),
			onError: (error) => {
				// Stopping ends the retries of an acknowledgement. The handler keeps its slot, and the
				// attempt is released with the signal undelivered.
				if (this.signal.aborted) return;
				taskAbortController.abort({ reason: "signal-failed", error: error.message });
			},
		});
		// Signals stored before the claim are ready when the handler registers its listener
		for (const stored of exec.signals) signals.push(stored);
		this._running.set(exec.id, {
			controller: taskAbortController,
			claimToken: exec.claim_token,
			inbox: signals,
		});

		// Pass db and tasks as extra context to maintenance task, which skips middleware
		const isMaintenance = task.name === "pgconductor.maintenance";
		const extraContext = isMaintenance
			? { ...this.extraContext, db: this.db, tasks: this.tasks }
			: this.extraContext;
		const context = TaskContext.create<Tasks, Events, typeof extraContext>(
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
				metadataSchema: task.metadataSchema,
				window: task.window,
				telemetry: this.telemetry,
				signals,
				steps,
			},
			extraContext,
		);
		const execution = {
			id: exec.id,
			task_key: exec.task_key,
			queue: exec.queue,
			metadata: exec.metadata || undefined,
		};
		// next() also settles when the handler suspends, so middleware finally blocks run.
		// The outer race below sees the abort first, so middleware cannot turn it into a result.
		const run = (isMaintenance ? [] : this.middleware).reduceRight<
			() => ReturnType<AnyTask["execute"]>
		>(
			(next, middleware) => () =>
				middleware({ execution, ctx: context }, <TNext extends object>(added: TNext) => {
					Object.assign(context, added);
					return next() as Promise<MiddlewareResult<TNext>>;
				}),
			() => Promise.race([task.execute(taskEvent, context), aborted]),
		);

		try {
			const payload = await parsePayload(task, taskEvent);
			if ("error" in payload) return { ...identity, ...payload };
			taskEvent.payload = payload.value;

			await this.scheduleNextExecution(exec);

			const outcome = await this.telemetry.process({
				taskKey: exec.task_key,
				queue: exec.queue,
				messageId: exec.id,
				traceContexts: [exec.trace_context],
				run: () =>
					Promise.race([
						run().then(async (output) => ({
							output,
							// Deliver the signals already read, still racing the abort. Ones stored later
							// re-queue the execution.
							signalFailure: taskAbortController.signal.aborted
								? null
								: await signals.close({ drain: true }),
						})),
						aborted,
					]),
			});

			if ("abort" in outcome) {
				const output = outcome.abort;
				switch (output.reason) {
					case "child-invocation":
						return {
							...identity,
							status: "invoke_child",
							timeout_ms: output.timeout_ms,
							step_key: output.step_key,
							child_task_name: output.task.name,
							child_task_queue: output.task.queue || "default",
							child_payload: output.payload,
							group: output.group,
							trace_context: output.trace_context,
							metadata: output.metadata,
						} as const;
					case "cancelled":
						return {
							...identity,
							status: "permanently_failed",
							error: exec.last_error || "Task was cancelled",
						} as const;
					case "timed-out":
						return {
							...identity,
							status: "failed",
							error: `Task timed out after ${task.timeoutMs}ms`,
						} as const;
					case "signal-failed":
						return {
							...identity,
							status: "failed",
							error: output.error,
						} as const;
					case "suspended":
						this.release(exec.id, exec.claim_token);
						return [];
					case "released":
						return {
							...identity,
							status: "released",
							reschedule_in_ms: output.reschedule_in_ms,
							step_key: output.step_key,
						} as const;
					default:
						assert.never(output);
				}
			}

			if (outcome.signalFailure) {
				if (this.signal.aborted) return { ...identity, status: "released" } as const;
				return {
					...identity,
					status: "failed",
					error: outcome.signalFailure.message,
				} as const;
			}

			const result = await parseResult(task, outcome.output);
			if ("error" in result) return { ...identity, ...result };

			return {
				...identity,
				status: "completed",
				result: result.value as Payload | undefined,
				signal_names: signals.names,
			} as const;
		} catch (err) {
			// A handler that stops on ctx.signal during shutdown is released, not failed
			if (this.signal.aborted) {
				return {
					...identity,
					status: "released",
				} as const;
			}
			return {
				...identity,
				status: "failed",
				error: coerceError(err).message,
			} as const;
		} finally {
			// Steps still running when a sibling hung up or threw end with the attempt, unless the
			// worker aborted it. They save their results before the attempt settles.
			const reason = taskAbortController.signal.reason?.reason;
			if (reason !== "timed-out" && reason !== "cancelled" && reason !== "signal-failed") {
				await Promise.race([Promise.allSettled(steps), timedOut.promise]);
			}
			clearTimeout(timeout);
			this._running.delete(exec.id);
			// A hung-up or failed attempt leaves undelivered signals for the next one
			void signals.close({ drain: false });
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
		claimed: Execution[],
	): Promise<ExecutionResult[]> {
		const parsed = await Promise.all(
			claimed.map(async (exec) => {
				const event = taskEventFor(exec);
				return { exec, event, payload: await parsePayload(task, event) };
			}),
		);

		// Members with an invalid payload settle on their own, the others run as the batch
		const rejected: ExecutionResult[] = [];
		const executions: Execution[] = [];
		const events: object[] = [];
		for (const { exec, event, payload } of parsed) {
			if ("error" in payload) {
				rejected.push({
					execution_id: exec.id,
					claim_token: exec.claim_token,
					task_key: taskKey,
					...payload,
				});
				continue;
			}
			executions.push(exec);
			events.push({
				...event,
				payload: payload.value,
				execution: {
					id: exec.id,
					queue: exec.queue,
					task_key: exec.task_key,
					locked_by: exec.locked_by,
				},
			});
		}
		if (executions.length === 0) return rejected;
		return [...rejected, ...(await this.executeBatch(task, taskKey, executions, events))];
	}

	private async executeBatch(
		task: AnyTask,
		taskKey: string,
		executions: Execution[],
		events: object[],
	): Promise<ExecutionResult[]> {
		const taskAbortController = new TypedAbortController<TaskAbortReasons>();
		const timeout = task.timeoutMs
			? setTimeout(() => taskAbortController.abort({ reason: "timed-out" }), task.timeoutMs)
			: undefined;
		for (const exec of executions) {
			this._running.set(exec.id, { controller: taskAbortController, claimToken: exec.claim_token });
		}

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

		const aborted = new Promise<{ abort: TaskAbortReasons }>((resolve) => {
			taskAbortController.signal.addEventListener("abort", () => {
				resolve({ abort: taskAbortController.signal.reason });
			});
		});

		try {
			// Schedule next executions for cron tasks
			await Promise.all(executions.map((exec) => this.scheduleNextExecution(exec)));

			const outcome = await this.telemetry.process({
				taskKey,
				queue: this.queueName,
				batchMessageCount: executions.length,
				traceContexts: executions.map((exec) => exec.trace_context),
				run: () =>
					Promise.race([
						task.execute(events, batchContext).then((output) => {
							if (output !== undefined) {
								if (!Array.isArray(output)) {
									throw new Error("Batch handler must return array matching input length");
								}
								if (output.length !== executions.length) {
									throw new Error(
										`Batch handler returned ${output.length} results but received ${executions.length} executions`,
									);
								}
							}
							return { output };
						}),
						aborted,
					]),
			});

			// Handle abort reasons
			if ("abort" in outcome) {
				const result = outcome.abort;
				if (result.reason === "released") {
					// Batch sleep - reschedule all
					return executions.map((exec) => ({
						execution_id: exec.id,
						claim_token: exec.claim_token,
						task_key: taskKey,
						status: "released" as const,
						reschedule_in_ms: result.reschedule_in_ms,
						step_key: result.step_key,
					}));
				}

				// Settlement fails the cancelled members; the others run again without spending an attempt
				if (result.reason === "cancelled") {
					return executions.map((exec) => ({
						execution_id: exec.id,
						claim_token: exec.claim_token,
						task_key: taskKey,
						status: "released" as const,
					}));
				}

				// Other abort reasons
				return executions.map((exec) => ({
					execution_id: exec.id,
					claim_token: exec.claim_token,
					task_key: taskKey,
					status: "failed" as const,
					error:
						result.reason === "timed-out"
							? `Task timed out after ${task.timeoutMs}ms`
							: `Task aborted: ${result.reason}`,
				}));
			}

			return Promise.all(
				executions.map(async (exec, i) => {
					const identity = {
						execution_id: exec.id,
						claim_token: exec.claim_token,
						task_key: taskKey,
					};
					const result = await parseResult(task, outcome.output?.[i]);
					if ("error" in result) return { ...identity, ...result };
					return {
						...identity,
						status: "completed" as const,
						result: result.value as Payload | undefined,
					};
				}),
			);
		} catch (err) {
			if (this.signal.aborted) {
				return executions.map((exec) => ({
					execution_id: exec.id,
					claim_token: exec.claim_token,
					task_key: taskKey,
					status: "released" as const,
				}));
			}

			// Handler threw: all fail together
			const error = coerceError(err).message;
			return executions.map((exec) => ({
				execution_id: exec.id,
				claim_token: exec.claim_token,
				task_key: taskKey,
				status: "failed" as const,
				error,
			}));
		} finally {
			clearTimeout(timeout);
			for (const exec of executions) this._running.delete(exec.id);
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

		await this.db.scheduleNextCronExecution(
			{
				executionId: execution.id,
				queue: execution.queue,
				orchestratorId: execution.locked_by,
				claimToken: execution.claim_token,
				runAt: nextTimestamp,
				dedupeKey: nextDedupeKey,
			},
			{ signal: this.signal },
		);
	}

	// --- Stage 3: Flush results to database ---
	private async flushResults(source: AsyncIterable<ExecutionResult>): Promise<void> {
		const orchestratorId = this.orchestratorId;
		assert.ok(orchestratorId, "orchestratorId must be set when starting the pipeline");

		let buffer: ExecutionResult[] = [];
		let flushTimer: ReturnType<typeof setTimeout> | null = null;
		// Flushes run one at a time, so the final flush follows every earlier one
		let flushing = Promise.resolve();
		let closed = false;

		const send = async (signal: AbortSignal) => {
			if (flushTimer) {
				clearTimeout(flushTimer);
				flushTimer = null;
			}

			if (buffer.length === 0) return;

			const results = buffer;
			buffer = [];

			try {
				await this.telemetry.settle({
					queue: this.queueName,
					batchMessageCount: results.length,
					run: () =>
						this.db.returnExecutions(
							{ orchestratorId, queueName: this.queueName, results },
							{ signal },
						),
				});
			} catch (err) {
				buffer = [...results, ...buffer];
				throw err;
			}
			for (const result of results) {
				this.release(result.execution_id, result.claim_token);
			}
		};

		const flush = () => {
			if (closed) return flushing;
			flushing = flushing
				.then(() => send(this.signal))
				.catch((err) => {
					this.logger.error("Error flushing results:", err);
					scheduleFlush();
				});
			return flushing;
		};

		const scheduleFlush = () => {
			if (closed) return;
			if (flushTimer) clearTimeout(flushTimer);
			flushTimer = setTimeout(flush, this.flushIntervalMs);
		};

		try {
			for await (const result of source) {
				buffer.push(result);

				// Start timer only after first result arrives
				if (!flushTimer) scheduleFlush();

				if (buffer.length >= this.flushBatchSize) await flush();
			}
		} finally {
			closed = true;
			if (flushTimer) clearTimeout(flushTimer);
			await flushing;
			// The worker signal is aborted on stop, so retries get their own deadline. Results that
			// still cannot be settled fail the worker's stop.
			await send(AbortSignal.timeout(FINAL_FLUSH_TIMEOUT_MS));
		}
	}

	// A newer claim of the same execution stays held
	private release(executionId: string, claimToken: string): void {
		if (this.held.get(executionId) === claimToken) this.held.delete(executionId);
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
