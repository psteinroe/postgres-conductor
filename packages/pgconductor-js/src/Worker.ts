import type { DatabaseClient, Execution } from "./DatabaseClient";
import type { RepeatedTask } from "./RepeatedTask";
import type { Task } from "./Task";
import { TaskContext } from "./TaskContext";
import { delay, type AbortablePromise } from "./abortable";
import assert from "./assert";
import { sleep } from "./utils";

// heartbeat should NOT happen per worker, but globally per orchestrator
// process signal handlers should also be in orchestrator, not worker

export type WorkerOptions = Partial<WorkerConfiguration>;

type WorkerConfiguration = {
	concurrency: number;
	pollIntervalMs: number;
	flushIntervalMs: number;
	flushBatchSize: number;
	prefetchMultiplier: number;
	maxPendingResults: number;
};

export type WorkerTasks = {
	tasks: Record<string, Task>;
	repeated: RepeatedTask[];
};

type ExecutionResult = {
	executionId: string;
	success: boolean;
	result?: Record<string, unknown>;
	error?: string;
};

export class Worker {
	private readonly id: string = crypto.randomUUID();

	private state: "created" | "running" | "stopping" | "stopped" = "created";

	private readonly config: WorkerConfiguration;

	// Execution tracking
	private inFlightCount = 0;
	private inFlightExecutions = new Map<string, Promise<ExecutionResult>>();
	private executionControllers = new Map<string, AbortController>();

	private localQueue: Execution[] = [];

	private pendingResults: ExecutionResult[] = [];

	private flushing = false;
	private lastFlushTime: number = Date.now();

	private fetchBackoffMs: number = 0;

	// TODO: replace with Signals
	private flushDelay: AbortablePromise<void> | null = null;
	private fetchDelay: AbortablePromise<void> | null = null;

	constructor(
		public readonly queue: string,
		public readonly tasks: WorkerTasks,
		options: WorkerOptions,
		private readonly db: DatabaseClient,
	) {
		this.config = {
			concurrency: options.concurrency || 5,
			pollIntervalMs: options.pollIntervalMs || 2000,
			flushIntervalMs: options.flushIntervalMs || 1000,
			flushBatchSize: options.flushBatchSize || 100,
			prefetchMultiplier: options.prefetchMultiplier || 2,
			maxPendingResults: options.maxPendingResults || 1000,
		};
	}

	async start() {
		assert.ok(this.state === "created", "Worker already started");
		this.state = "running";

		await this.upsertQueue();

		try {
			const loops = [
				this._wrapLoop(this.fetchLoop.bind(this)),
				this._wrapLoop(this.flushLoop.bind(this)),
			];

			await Promise.all(loops);
		} catch (err) {
			console.error("Worker encountered a fatal error:", err);
			throw err;
		} finally {
			await this.shutdown();
		}
	}

	private async upsertQueue() {}

	private async _wrapLoop(loopFn: () => Promise<void>) {
		try {
			await loopFn();
		} catch (err) {
			// mark stopping so other loops can see it, and abort delays to wake them
			console.error("Loop threw, initiating shutdown", err);
			this.state = "stopping";
			this.abortDelays();
			throw err;
		}
	}

	// Flusher loop runs while running or stopping to ensure final flushes
	private async flushLoop() {
		while (this.state === "running" || this.state === "stopping") {
			try {
				if (this.pendingResults.length > 0) {
					const timeSinceLastFlush = Date.now() - this.lastFlushTime;
					const shouldFlush =
						this.pendingResults.length >= this.config.flushBatchSize ||
						timeSinceLastFlush >= this.config.flushIntervalMs ||
						(this.state === "stopping" && this.pendingResults.length > 0);

					if (shouldFlush) {
						await this.flush();
					}
				}

				this.flushDelay = delay(this.config.flushIntervalMs);
				await this.flushDelay;
				this.flushDelay = null;
			} catch (err) {
				// flush errors are handled in flush; here just wait a bit and continue
				console.error("Flusher loop error", err);
			}
		}
	}

	private async fetchLoop() {
		while (this.state === "running") {
			try {
				// Wait if at capacity
				if (this.inFlightCount >= this.config.concurrency) {
					// TODO: replace with Signal
					await this.waitForCapacity();
					continue;
				}

				// Backpressure: wait if results are piling up
				if (this.pendingResults.length >= this.config.maxPendingResults) {
					// TODO: replace with Signal
					await this.waitForFlush();
					continue;
				}

				// Fetch more work if queue is empty
				if (this.localQueue.length === 0) {
					const fetchLimit =
						(this.config.concurrency - this.inFlightCount) *
						this.config.prefetchMultiplier;
					const execs = await this.db.getExecutions(
						this.queue,
						this.id,
						fetchLimit,
					);

					this.fetchBackoffMs = 0; // reset on success

					if (execs.length > 0) {
						this.localQueue.push(...execs);
					} else {
						// No work available, poll later
						this.flushDelay = delay(this.config.pollIntervalMs);
						await this.flushDelay;
						this.fetchDelay = null;
						continue;
					}
				}

				// Start as many executions as possible
				while (
					this.inFlightCount < this.config.concurrency &&
					this.localQueue.length > 0
				) {
					const exec = this.localQueue.shift();
					if (!exec) break;
					this.startExecution(exec);
				}
			} catch (err) {
				console.log("Fetch error", err);

				this.fetchBackoffMs = this.fetchBackoffMs
					? Math.min(this.fetchBackoffMs * 2, 30_000)
					: 500;

				// TODO: consider adding some max retries or fatal error handling here
				const jitter = Math.random() * 1000;
				this.flushDelay = delay(this.fetchBackoffMs + jitter);
				await this.flushDelay;
				this.fetchDelay = null;
			}
		}
	}

	private startExecution(execution: Execution) {
		// important: must be sync
		this.inFlightCount += 1;

		const controller = new AbortController();
		this.executionControllers.set(execution.id, controller);

		const p = this.executeTask(execution, controller.signal)
			.then((result) => {
				this.pendingResults.push(result);

				if (this.pendingResults.length >= this.config.maxPendingResults) {
					this.signalFlushNeeded();
				}
				return result;
			})
			.catch((err) => {
				// If executeTask throws, convert to failed result
				const result: ExecutionResult = {
					id: execution.id,
					status: "failed",
					error: err?.message ?? String(err),
				};
				this.pendingResults.push(result);

				if (this.pendingResults.length >= this.config.maxPendingResults) {
					this.signalFlushNeeded();
				}
				return result;
			})
			.finally(() => {
				this.inFlightCount--;
				this.inFlightExecutions.delete(execution.id);
				this.executionControllers.delete(execution.id);
				this.signalExecutionComplete();
			});

		this.inFlightExecutions.set(execution.id, p);
	}

	private async executeTask(
		execution: Execution,
		signal: AbortSignal,
	): Promise<ExecutionResult> {
		// Resolve task key (support multiple possible fields)
		const taskKey =
			(execution as any).taskKey ??
			(execution as any).workflow_key ??
			(execution as any).task_type;

		const task = taskKey ? this.tasks.tasks[taskKey] : undefined;

		assert.ok(task, `Unknown task: ${taskKey ?? execution.id}`);

		const ctx = new TaskContext({ signal });

		// Task should return an object or throw on failure
		// We expect task.execute to be cancellation-aware (checks ctx.signal)
		const resultObj = await task!.execute(execution.payload, ctx);

		// If the task completed but returned an explicit failure shape, normalize here
		// For simplicity, any throw is treated as failure. Otherwise it's completed.
		return {
			id: execution.id,
			status: "completed",
			result: resultObj,
		};
	}

	// Stop fetching new work (initiates stopping)
	async stop() {
		assert.eq(this.state, "running", "Worker not running");

		this.state = "stopping";
		this.abortDelays();
	}

	private async shutdown(timeoutMs = 30_000) {
		if (this.state === "stopped" || this.state === "stopping") {
			return;
		}

		this.state = "stopping";

		this.abortDelays();

		// todo: send abort signals to in-flight tasks
		// this.abortExecutions();

		// Wait for in-flight executions (with timeout)
		await Promise.race([
			Promise.allSettled(Array.from(this.inFlightExecutions.values())),
			sleep(timeoutMs),
		]);

		// Final flush of any accumulated results
		await this.flush();

		this.state = "stopped";
	}

	private async flush() {
		if (this.flushing) return;
		this.flushing = true;

		// Snapshot current batch
		const batch = [...this.pendingResults];
		if (batch.length === 0) {
			this.flushing = false;
			return;
		}

		// Clear pending results immediately (new results can accumulate)
		this.pendingResults = [];

		try {
			await this.db.returnExecutions({
				workerId: this.id,
				queue: this.queue,
				results: batch,
			});
			this.lastFlushTime = Date.now();
		} catch (err) {
			console.error("Flush failed", err);

			this.pendingResults.push(...batch);
		} finally {
			this.flushing = false;
		}
	}

	private abortDelays() {
		this.fetchDelay?.abort();
		this.flushDelay?.abort();
	}
}
