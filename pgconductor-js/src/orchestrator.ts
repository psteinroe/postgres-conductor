import { Worker, type WorkerConfig } from "./worker";
import { DatabaseClient } from "./database-client";
import { MigrationStore } from "./migration-store";
import { SchemaManager } from "./schema-manager";
import { Deferred } from "./lib/deferred";
import type { Conductor } from "./conductor";
import { type AnyTask, type ValidateTasksQueue, Task } from "./task";
import { PACKAGE_VERSION } from "./versions";
import { makeChildLogger, type Logger } from "./lib/logger";

export type OrchestratorOptions<
	TTasks extends readonly AnyTask[] = readonly AnyTask[],
> = {
	conductor: Conductor<any, any, any, any, any, any, any>;
	tasks?: ValidateTasksQueue<"default", TTasks>;
	defaultWorker?: Partial<WorkerConfig>;
	workers?: Worker[];
};

type InternalOrchestratorOptions = {
	conductor: Conductor<any, any, any, any, any, any, any>;
	tasks?: readonly AnyTask[];
	defaultWorker?: Partial<WorkerConfig>;
	workers?: Worker[];
};

const HEARTBEAT_INTERVAL_MS = 30000; // 30 seconds
const STALE_ORCHESTRATOR_MAX_AGE_MS = HEARTBEAT_INTERVAL_MS * 10;

/**
 * Orchestrator manages the worker pool and handles:
 * - Worker lifecycle (start/stop)
 * - Heartbeat tracking
 * - Live migrations with graceful shutdown
 * - Stale orchestrator recovery
 */
export class Orchestrator {
	private readonly db: DatabaseClient;
	private readonly workers: Worker[] = [];
	private readonly orchestratorId: string;
	private readonly migrationStore: MigrationStore;
	private readonly schemaManager: SchemaManager;
	private readonly logger: Logger;

	private heartbeatTimer: Timer | null = null;
	private _stopDeferred: Deferred<void> | null = null;
	private _startDeferred: Deferred<void> | null = null;
	private _abortController: AbortController | null = null;

	private constructor(options: InternalOrchestratorOptions) {
		this.orchestratorId = crypto.randomUUID();
		this.db = options.conductor.db;
		this.logger = makeChildLogger(options.conductor.logger, {
			component: "orchestrator",
			orchestratorId: this.orchestratorId,
		});
		this.migrationStore = new MigrationStore();
		this.schemaManager = new SchemaManager(this.db);

		if (options.tasks?.length) {
			const worker = new Worker(
				"default",
				options.tasks,
				this.db,
				this.logger,
				options.defaultWorker,
				options.conductor.options.context,
			);
			this.workers.push(worker);
		}

		for (const w of options.workers || []) {
			if (this.workers.find((existing) => existing.queueName === w.queueName)) {
				throw new Error(`Duplicate worker name: ${w.queueName}`);
			}

			this.workers.push(w);
		}
	}

	static create<
		const TTasks extends readonly Task<any, "default", any, any, any, any>[],
	>(options: OrchestratorOptions<TTasks>): Orchestrator {
		return new Orchestrator(options);
	}

	/**
	 * Promise that resolves when orchestrator has started.
	 * (Migrations complete, workers registered and started)
	 */
	get started(): Promise<void> {
		return this._startDeferred?.promise || Promise.resolve();
	}

	/**
	 * Promise that resolves when orchestrator has stopped.
	 * (All workers stopped, cleanup complete)
	 */
	get stopped(): Promise<void> {
		return this._stopDeferred?.promise || Promise.resolve();
	}

	/**
	 * Start the orchestrator.
	 * Returns when startup is complete (all workers started).
	 * Orchestrator continues running in background until stop() is called.
	 */
	async start(): Promise<void> {
		return this._internalStart({ runOnce: false });
	}

	/**
	 * Start the orchestrator and wait until it stops.
	 * Equivalent to: await start(); return stopped;
	 * Returns when orchestrator has fully stopped.
	 */
	async run(): Promise<void> {
		await this._internalStart({ runOnce: false });
		return this.stopped;
	}

	/**
	 * Process all queued tasks once and stop automatically.
	 * Returns when all work is complete.
	 * Useful for testing and batch processing.
	 */
	async drain(): Promise<void> {
		await this._internalStart({ runOnce: true });
		return this.stopped;
	}

	private async _internalStart({
		runOnce = false,
	}: { runOnce?: boolean } = {}): Promise<void> {
		if (this._stopDeferred) {
			throw new Error("Orchestrator is already running");
		}

		this._stopDeferred = new Deferred<void>();
		this._startDeferred = new Deferred<void>();
		this._abortController = new AbortController();

		(async () => {
			try {
				const ourVersion = this.migrationStore.getLatestMigrationNumber();
				const installedVersion = await this.db.getInstalledMigrationNumber();

				// Step 1: Check if we're too old (should never happen, but safety check)
				if (installedVersion > ourVersion) {
					this.logger.info(
						`Orchestrator version ${ourVersion} is older than installed version ${installedVersion}, shutting down`,
					);
					this.stopDeferred.resolve();
					this.startDeferred.resolve();
					return;
				}

				// Step 2: Ensure schema is at latest version
				// ensureLatest() will check if migrations needed and:
				// - Try to acquire lock (non-blocking)
				// - If can't get lock, wait and recheck or shutdown
				// - Signal others to shut down (if schema exists)
				// - Wait for them to exit
				// - Apply migrations
				const { shouldShutdown } = await this.schemaManager.ensureLatest(
					this.signal,
				);

				if (shouldShutdown) {
					this.logger.info(
						`Orchestrator ${this.orchestratorId} could not acquire migration lock, shutting down`,
					);
					this.stopDeferred.resolve();
					this.startDeferred.resolve();
					return;
				}

				const signals = await this.db.orchestratorHeartbeat({
					orchestratorId: this.orchestratorId,
					version: PACKAGE_VERSION,
					migrationNumber: this.migrationStore.getLatestMigrationNumber(),
				});

				// Check for shutdown signal on startup
				const hasShutdownSignal = signals.some(
					(s) => s.signal_type === "shutdown",
				);
				if (hasShutdownSignal) {
					this.logger.info(
						`Orchestrator ${this.orchestratorId} detected newer schema after migrations, shutting down`,
					);
					this.stopDeferred.resolve();
					this.startDeferred.resolve();
					return;
				}

				// Start heartbeat loop
				this.startHeartbeatLoop();

				// Kick off all workers (don't await yet!)
				if (runOnce) {
					// Drain mode: workers will process and stop
					this.workers.forEach((w) => void w.drain(this.orchestratorId));
				} else {
					// Normal mode: workers will run continuously
					this.workers.forEach((w) => void w.run(this.orchestratorId));
				}

				// Wait for ALL workers to finish starting (register() complete)
				await Promise.all(this.workers.map((w) => w.started));

				// NOW signal that orchestrator has started
				this.startDeferred.resolve();

				// Wait for shutdown signal or all workers to complete
				await Promise.race([
					Promise.all(this.workers.map((w) => w.stopped)),
					this.waitForShutdownSignal(),
				]);

				// Stop gracefully
				await this.stopWorkers();

				this.stopDeferred.resolve();
			} catch (err) {
				this.logger.error(err);
				this.stopDeferred.reject(err);
				this.startDeferred.reject(err);
			} finally {
				await this.cleanup();
			}
		})();

		// Wait for start to complete
		return this._startDeferred.promise;
	}

	/**
	 * Stop the orchestrator gracefully:
	 * 1. Stop heartbeat
	 * 2. Stop all workers
	 * 3. Clean up resources
	 *
	 * Waits until the orchestrator is fully stopped
	 */
	async stop(): Promise<void> {
		// Capture deferred before aborting (avoid race)
		const currentDeferred = this._stopDeferred;

		if (!currentDeferred) {
			return; // Not running
		}

		// Signal shutdown
		this.abortController.abort();

		// Wait for orchestrator to complete
		try {
			await currentDeferred.promise;
		} catch {
			// Ignore errors; already logged in start()
		}
	}

	/**
	 * Start the heartbeat loop that:
	 * - Updates last_heartbeat_at every heartbeatIntervalMs
	 * - Checks for version mismatch shutdowns from database
	 * - Recovers stale orchestrators periodically (every 8th heartbeat)
	 */
	private startHeartbeatLoop(): void {
		let heartbeatCount = 0;

		const beat = async () => {
			try {
				if (this.abortController.signal.aborted) {
					// if we are aborting, do not run heartbeat
					return;
				}

				heartbeatCount = (heartbeatCount % 4) + 1;

				// Every 8th heartbeat, recover stale orchestrators
				if (heartbeatCount === 8) {
					await this.db.recoverStaleOrchestrators({
						maxAge: `${STALE_ORCHESTRATOR_MAX_AGE_MS} milliseconds`,
					});
				}

				// Send heartbeat and process signals
				const signals = await this.db.orchestratorHeartbeat({
					orchestratorId: this.orchestratorId,
					version: PACKAGE_VERSION,
					migrationNumber: this.migrationStore.getLatestMigrationNumber(),
				});

				// Process signals in order
				for (const signal of signals) {
					if (!signal.signal_type) continue;

					switch (signal.signal_type) {
						case "shutdown":
							if (!this.signal.aborted) {
								this.logger.info(
									`Received shutdown signal: ${signal.signal_payload?.reason || "unknown"}`,
								);
								this.abortController.abort();
							}
							break;

						case "cancel_execution":
							if (
								signal.signal_execution_id &&
								signal.signal_payload &&
								signal.signal_payload.queue
							) {
								const worker = this.workers.find(
									(w) => w.queueName === signal.signal_payload?.queue,
								);
								if (worker) {
									worker.cancelExecutions([signal.signal_execution_id]);
								}
							}
							break;
					}
				}
			} catch (err) {
				this.logger.error("Heartbeat error:", err);
			} finally {
				// Schedule next heartbeat if not shutting down
				if (!this.abortController.signal.aborted) {
					this.heartbeatTimer = setTimeout(beat, HEARTBEAT_INTERVAL_MS);
				}
			}
		};

		// Start first heartbeat
		this.heartbeatTimer = setTimeout(beat, HEARTBEAT_INTERVAL_MS);
	}

	/**
	 * Wait for shutdown signal via abort controller
	 */
	private async waitForShutdownSignal(): Promise<void> {
		return new Promise((resolve) => {
			if (this.abortController.signal.aborted) {
				resolve();
				return;
			}

			this.abortController.signal.addEventListener("abort", () => {
				resolve();
			});
		});
	}

	/**
	 * Stop all workers gracefully
	 */
	private async stopWorkers(): Promise<void> {
		await Promise.all(this.workers.map((worker) => worker.stop()));
	}

	/**
	 * Clean up resources:
	 * - Stop heartbeat
	 * - Remove orchestrator from database and release locked executions
	 * - Close database connection
	 */
	private async cleanup(): Promise<void> {
		// Stop heartbeat
		if (this.heartbeatTimer) {
			clearTimeout(this.heartbeatTimer);
			this.heartbeatTimer = null;
		}

		// Remove ourselves from orchestrators table and release locked executions
		await this.db.orchestratorShutdown({
			orchestratorId: this.orchestratorId,
		});

		// Close database client (no-op if user supplied their own instance)
		await this.db.close();
		this._stopDeferred = null;
		this._startDeferred = null;
		this._abortController = null;
	}

	get info() {
		return {
			id: this.orchestratorId,
			version: PACKAGE_VERSION,
			migrationNumber: this.migrationStore.getLatestMigrationNumber(),
			workerCount: this.workers.length,
			isRunning: this._stopDeferred !== null,
			shutdownSignal: this._abortController?.signal.aborted || false,
		};
	}

	private get stopDeferred(): Deferred<void> {
		if (!this._stopDeferred) {
			throw new Error("Orchestrator is not running");
		}
		return this._stopDeferred;
	}

	private get startDeferred(): Deferred<void> {
		if (!this._startDeferred) {
			throw new Error("Orchestrator is not running");
		}
		return this._startDeferred;
	}

	private get abortController(): AbortController {
		if (!this._abortController) {
			throw new Error("Orchestrator is not running");
		}
		return this._abortController;
	}

	private get signal(): AbortSignal {
		return this.abortController.signal;
	}
}
