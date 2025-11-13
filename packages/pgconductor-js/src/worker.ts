import type {
	DatabaseClient,
	Execution,
	ExecutionResult,
} from "./database-client";
import type { AnyTask } from "./task";
import { waitFor } from "./lib/wait-for";
import { mapConcurrent } from "./lib/map-concurrent";
import { Deferred } from "./lib/deferred";
import { AsyncQueue } from "./lib/async-queue";
import { TaskContext } from "./task-context";

const HANGUP = Symbol("hangup");

/**
 * Worker implemented as async pipeline: fetch → execute → flush.
 * Uses async iterators for clean composition and natural backpressure.
 */
export class Worker {
	private readonly fetchBatchSize: number;
	private readonly concurrency: number;
	private readonly flushBatchSize: number;
	private readonly flushIntervalMs: number;
	private pollIntervalMs: number;

	private _deferred: Deferred<void> | null = null;
	private _abortController: AbortController | null = null;

	constructor(
		private readonly orchestratorId: string,
		private readonly task: AnyTask,
		private readonly db: DatabaseClient,
		private readonly extraContext: object = {},
	) {
		// todo: get from Task
		this.concurrency = 1;
		this.pollIntervalMs = 1000;

		// TODO: Tune these parameters
		this.fetchBatchSize = this.concurrency * 2;
		this.flushBatchSize = this.concurrency * 4;
		this.flushIntervalMs = this.pollIntervalMs * 2;
	}

	/**
	 * Start the worker pipeline.
	 * Returns a promise that resolves when the worker stops.
	 */
	async start(): Promise<void> {
		if (this._deferred) return this._deferred.promise;

		this._abortController = new AbortController();
		this._deferred = new Deferred<void>();

		// Upsert task configuration before starting worker
		await this.db.upsertTask(
			{
				key: this.task.key,
				maxAttempts: this.task.config.maxAttempts,
				partition: this.task.config.partition,
				window: this.task.config.window,
			},
			this.signal,
		);

		const queue = new AsyncQueue<Execution>(this.fetchBatchSize * 2);

		void this.fetchExecutions(queue);

		(async () => {
			try {
				// Consume from queue → execute → flush
				for await (const _ of this.flushResults(this.executeTasks(queue))) {
					if (this.signal.aborted) break;
				}

				this.deferred.resolve();
			} catch (err) {
				console.error("Worker pipeline error:", err);
				this.deferred.reject(err);
			} finally {
				queue.close();
				this._deferred = null;
				this._abortController = null;
			}
		})();

		return this._deferred.promise;
	}

	/**
	 * Stop the worker gracefully.
	 * Returns a promise that resolves when the worker has fully stopped.
	 */
	async stop(): Promise<void> {
		const currentDeferred = this._deferred;

		if (!currentDeferred) {
			return;
		}

		this.abortController.abort();

		try {
			await currentDeferred.promise;
		} catch {}
	}

	// --- Stage 1: Fetch executions from database ---
	private async fetchExecutions(queue: AsyncQueue<Execution>) {
		while (!this.signal?.aborted) {
			try {
				const executions = await this.db.getExecutions(
					this.orchestratorId,
					this.task.key,
					this.fetchBatchSize,
					this.signal,
				);

				if (executions.length === 0) {
					await waitFor(this.pollIntervalMs, { signal: this.signal });
					continue;
				}

				for (const exec of executions) {
					await queue.push(exec); // waits if full
					if (this.signal.aborted) break;
				}
			} catch (err) {
				console.error("Fetch error:", err);
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
				const taskAbortController = new AbortController();

				const hangupPromise = new Promise<typeof HANGUP>((resolve) => {
					taskAbortController.signal.addEventListener("abort", () => {
						resolve(HANGUP);
					});
				});

				try {
					const output = await Promise.race([
						this.task.execute(
							exec.payload,
							TaskContext.create(
								{
									signal: this.signal,
									db: this.db,
									abortController: taskAbortController,
								},
								this.extraContext,
							),
						),
						hangupPromise,
					]);

					if (output === HANGUP) {
						return {
							execution_id: exec.id,
							status: "released",
						} as const;
					}

					return {
						execution_id: exec.id,
						status: "completed",
						result: output,
					} as const;
				} catch (err) {
					return {
						execution_id: exec.id,
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
				await this.db.returnExecutions(
					batch,
					isCleanup ? undefined : this.signal,
				);
			} catch (err) {
				console.error("Flush failed:", err);
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

	private get deferred(): Deferred<void> {
		if (!this._deferred) {
			throw new Error("Worker is not running");
		}
		return this._deferred;
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
