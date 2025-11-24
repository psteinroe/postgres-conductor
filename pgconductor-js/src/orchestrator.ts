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
	 * Promise that resolves when the orchestrator stops (either via stop() or error)
	 */
	get stopped(): Promise<void> {
		if (!this._stopDeferred) {
			return Promise.resolve();
		}
		return this._stopDeferred.promise;
	}

	/**
	 * Start the orchestrator:
	 * 1. Get installed version (-1 if not installed, 0+ if installed)
	 * 2. Check if we're too old (installed version > our version)
	 * 3. Run migrations if needed (migrate() handles locking/signaling/waiting)
	 * 4. Start heartbeat loop
	 * 5. Start all workers
	 *
	 * Returns when the orchestrator has started (not when it stops)
	 */
	async start(): Promise<void> {
		if (this._stopDeferred) {
			throw new Error("Orchestrator is already running");
		}

		this._stopDeferred = new Deferred<void>();
		this._startDeferred = new Deferred<void>();
		this._abortController = new AbortController();

		(async () => {
			try {
				const ourVersion = this.migrationStore.getLatestMigrationNumber();
				const installedVersion = await this.db.getInstalledMigrationNumber(
					this.signal,
				);

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

				const hbShutdown = await this.db.orchestratorHeartbeat(
					this.orchestratorId,
					PACKAGE_VERSION,
					this.migrationStore.getLatestMigrationNumber(),
					this.signal,
				);

				if (hbShutdown) {
					this.logger.info(
						`Orchestrator ${this.orchestratorId} detected newer schema after migrations, shutting down`,
					);
					this.stopDeferred.resolve();
					this.startDeferred.resolve();
					return;
				}

				// Start heartbeat loop
				this.startHeartbeatLoop();

				// Start all workers
				const workerPromises = this.workers.map((worker) =>
					worker.start(this.orchestratorId),
				);

				// Signal that we've started successfully
				this.startDeferred.resolve();

				// Wait for shutdown signal or all workers to complete
				await Promise.race([
					Promise.all(workerPromises),
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
					await this.db.recoverStaleOrchestrators(
						`${STALE_ORCHESTRATOR_MAX_AGE_MS} milliseconds`,
						this.signal,
					);
				}

				// Send heartbeat and check for shutdown signal
				const shouldShutdown = await this.db.orchestratorHeartbeat(
					this.orchestratorId,
					PACKAGE_VERSION,
					this.migrationStore.getLatestMigrationNumber(),
					this.signal,
				);

				if (shouldShutdown && !this.signal.aborted) {
					this.logger.info(`Received shutdown signal`);
					this.abortController.abort();
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

		const cleanupSignal = new AbortController().signal;

		// Remove ourselves from orchestrators table and release locked executions
		await this.db.orchestratorShutdown(this.orchestratorId, cleanupSignal);

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
