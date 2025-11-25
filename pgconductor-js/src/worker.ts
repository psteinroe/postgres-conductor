import type {
	DatabaseClient,
	EventSubscriptionSpec,
	Execution,
	ExecutionResult,
	ExecutionSpec,
} from "./database-client";
import type { AnyTask } from "./task";
import type { TaskDefinition } from "./task-definition";
import { waitFor } from "./lib/wait-for";
import { mapConcurrent } from "./lib/map-concurrent";
import { Deferred } from "./lib/deferred";
import { AsyncQueue } from "./lib/async-queue";
import CronExpressionParser from "cron-parser";
import { TaskContext } from "./task-context";
import * as assert from "./lib/assert";
import { createMaintenanceTask } from "./maintenance-task";
import { makeChildLogger, type Logger } from "./lib/logger";
import type { EventDefinition } from "./event-definition";

const HANGUP = Symbol("hangup");

export type WorkerConfig = {
	concurrency: number;
	localBuffer: number;
	fetchBatchSize: number;
	pollIntervalMs: number;
	flushIntervalMs: number;
};

export const DEFAULT_WORKER_CONFIG: WorkerConfig = {
	concurrency: 1,
	localBuffer: 2,
	fetchBatchSize: 2,
	pollIntervalMs: 1000,
	flushIntervalMs: 2000,
};

/**
 * Worker implemented as async pipeline: fetch → execute → flush.
 * Uses async iterators for clean composition and natural backpressure.
 *
 * Queue-aware: Processes multiple tasks from a single queue.
 */
export class Worker<
	Tasks extends readonly TaskDefinition<
		string,
		any,
		any,
		string
	>[] = readonly TaskDefinition<string, any, any, string>[],
	Events extends readonly EventDefinition<
		string,
		any
	>[] = readonly EventDefinition<string, any>[],
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
		this.flushBatchSize = fullConfig.localBuffer;
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

		// Synchronous registration
		await this.register();

		// Worker is now started
		this._startDeferred.resolve();

		// Run pipeline in background
		const queue = new AsyncQueue<Execution>(this.fetchBatchSize * 2);
		void this.fetchExecutions(queue, { runOnce });

		(async () => {
			try {
				// Consume from queue → execute → flush
				for await (const _ of this.flushResults(this.executeTasks(queue))) {
					if (this.signal.aborted) break;
				}
			} catch (err) {
				this.logger.error("Worker pipeline error:", err);
			} finally {
				queue.close();
				// Cleanup
				this._stopDeferred?.resolve();
				this._startDeferred = null;
				this._stopDeferred = null;
				this._abortController = null;
			}
		})();

		return this._startDeferred.promise;
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
		const retentionToDays = (
			setting: boolean | { days: number } | undefined,
		): number | null => {
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
		}));

		const allTasks = Array.from(this.tasks.values());

		const cronSchedules: ExecutionSpec[] = allTasks.flatMap((task) =>
			task.triggers
				.filter((t): t is { cron: string } => "cron" in t)
				.map((trigger) => {
					const interval = CronExpressionParser.parse(trigger.cron);
					const nextTimestamp = interval.next().toDate();
					const timestampSeconds = Math.floor(nextTimestamp.getTime() / 1000);
					return {
						task_key: task.name,
						queue: this.queueName,
						run_at: nextTimestamp,
						dedupe_key: `repeated::${trigger.cron}::${timestampSeconds}`,
						cron_expression: trigger.cron,
					};
				}),
		);

		const eventSubscriptions: EventSubscriptionSpec[] = allTasks.flatMap(
			(task) => {
				const customEvents = task.triggers
					.filter(
						(t): t is { event: string } =>
							"event" in t && typeof (t as any).event === "string",
					)
					.map(
						(trigger): EventSubscriptionSpec => ({
							task_key: task.name,
							queue: this.queueName,
							source: "event",
							event_key: trigger.event,
						}),
					);

				const dbEvents = task.triggers
					.filter((t) => "schema" in t && "table" in t && "operation" in t)
					.map((trigger): EventSubscriptionSpec => {
						const dbTrigger = trigger as {
							schema: string;
							table: string;
							operation: string;
							columns?: string;
						};
						return {
							task_key: task.name,
							queue: this.queueName,
							source: "db",
							schema_name: dbTrigger.schema,
							table_name: dbTrigger.table,
							operation: dbTrigger.operation,
							columns: dbTrigger.columns
								?.split(",")
								.map((c: string) => c.trim().toLowerCase()),
						};
					});

				return [...customEvents, ...dbEvents];
			},
		);

		await this.db.registerWorker(
			this.queueName,
			taskSpecs,
			cronSchedules,
			eventSubscriptions,
		);
	}

	// --- Stage 1: Fetch executions from database ---
	private async fetchExecutions(
		queue: AsyncQueue<Execution>,
		{ runOnce = false }: { runOnce?: boolean },
	) {
		assert.ok(
			this.orchestratorId,
			"orchestratorId must be set when starting the pipeline",
		);

		// Pre-compute task metadata once
		const allTasks = Array.from(this.tasks.values());
		const taskMaxAttempts: Record<string, number> = {};
		for (const task of allTasks) {
			taskMaxAttempts[task.name] = task.maxAttempts || 3;
		}

		while (!this.signal?.aborted) {
			try {
				// Filter tasks based on time windows (if any)
				const now = new Date();

				function isWithinWindow(task: AnyTask, now: Date): boolean {
					if (!task.window) return true;
					const [start, end] = task.window;
					const currentTime = now.toTimeString().slice(0, 8);
					return currentTime >= start && currentTime < end;
				}

				const disallowedTaskKeys = allTasks
					.filter((task) => !isWithinWindow(task, now))
					.map((t) => t.name);

				if (disallowedTaskKeys.length === this.tasks.size) {
					// skip fetch if no tasks are allowed to run now
					await waitFor(this.pollIntervalMs, { signal: this.signal });
					continue;
				}

				const executions = await this.db.getExecutions(
					this.orchestratorId,
					this.queueName,
					this.fetchBatchSize,
					disallowedTaskKeys,
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
			} catch (err) {
				await waitFor(2000, { signal: this.signal });
			}
		}

		queue.close();
	}

	// --- Stage 2: Execute tasks concurrently ---
	private async *executeTasks(
		source: AsyncIterable<Execution>,
	): AsyncGenerator<ExecutionResult> {
		for await (const result of mapConcurrent(
			source,
			this.concurrency,
			async (exec) => {
				// Dispatch to correct task based on task_key
				const task = this.tasks.get(exec.task_key);
				if (!task) {
					return {
						execution_id: exec.id,
						task_key: exec.task_key,
						status: "failed",
						error: `Task not found: ${exec.task_key}`,
					} as const;
				}

				const taskAbortController = new AbortController();

				const hangupPromise = new Promise<typeof HANGUP>((resolve) => {
					taskAbortController.signal.addEventListener("abort", () => {
						resolve(HANGUP);
					});
				});

				try {
					await this.scheduleNextExecution(exec);

					// Determine event type based on execution data
					let taskEvent: any;
					if (exec.cron_expression) {
						taskEvent = { event: "pgconductor.cron" };
					} else if (
						exec.payload &&
						typeof exec.payload === "object" &&
						"event" in exec.payload &&
						exec.payload.event !== "pgconductor.invoke"
					) {
						// Event-triggered execution (custom event or db event)
						taskEvent = {
							event: exec.payload.event,
							payload: exec.payload.payload,
						};
					} else {
						// Direct invoke
						taskEvent = { event: "pgconductor.invoke", payload: exec.payload };
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
									signal: this.signal,
									db: this.db,
									abortController: taskAbortController,
									execution: exec,
									logger: makeChildLogger(this.logger, {
										execution_id: exec.id,
										task_key: exec.task_key,
										queue: exec.queue,
									}),
								},
								extraContext,
							),
						),
						hangupPromise,
					]);

					if (output === HANGUP) {
						return {
							execution_id: exec.id,
							task_key: exec.task_key,
							status: "released",
						} as const;
					}

					return {
						execution_id: exec.id,
						task_key: exec.task_key,
						status: "completed",
						result: output,
					} as const;
				} catch (err) {
					return {
						execution_id: exec.id,
						task_key: exec.task_key,
						status: "failed",
						// todo: properly serialize errors
						error: (err as Error).message,
					} as const;
				}
			},
		)) {
			yield result;
		}
	}

	private async scheduleNextExecution(execution: Execution): Promise<void> {
		if (!execution.cron_expression) {
			return;
		}

		const interval = CronExpressionParser.parse(execution.cron_expression);
		const nextTimestamp = interval.next().toDate();
		const timestampSeconds = Math.floor(nextTimestamp.getTime() / 1000);
		let dedupePrefix = `repeated::${execution.cron_expression}`;
		if (execution.dedupe_key && execution.dedupe_key.startsWith("dynamic::")) {
			const parts = execution.dedupe_key.split("::");
			if (parts.length >= 2) {
				dedupePrefix = `${parts[0]}::${parts[1]}`;
			}
		}
		const nextDedupeKey = `${dedupePrefix}::${timestampSeconds}`;

		await this.db.invoke({
			task_key: execution.task_key,
			queue: execution.queue,
			run_at: nextTimestamp,
			dedupe_key: nextDedupeKey,
			cron_expression: execution.cron_expression,
		});
	}

	// --- Stage 3: Flush results to database ---
	private async *flushResults(
		source: AsyncIterable<ExecutionResult>,
	): AsyncGenerator<void> {
		let buffer: ExecutionResult[] = [];
		let flushTimer: Timer | null = null;

		const flushNow = async (isCleanup = false) => {
			if (buffer.length === 0) return;

			const batch = buffer;
			buffer = [];

			if (flushTimer) {
				clearTimeout(flushTimer);
				flushTimer = null;
			}

			try {
				await this.db.returnExecutions(batch);
			} catch (err) {
				this.logger.error("Error flushing results:", err);
				if (!isCleanup) {
					buffer.push(...batch);
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
				buffer.push(result);

				// Start timer only after first result arrives
				if (!flushTimer) scheduleFlush();

				if (buffer.length >= this.flushBatchSize) {
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
