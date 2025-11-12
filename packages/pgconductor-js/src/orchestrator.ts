import postgres, { type Sql } from "postgres";
import type { Task } from "./task";
import { Worker } from "./worker";
import { DatabaseClient } from "./database-client";
import { MigrationStore } from "./migration-store";
import { SchemaManager } from "./schema-manager";
import { Deferred } from "./lib/deferred";

export type OrchestratorOptions = {
	connectionString: string;
	tasks: Task[];
	version: string;
	heartbeatIntervalMs?: number;
	staleOrchestratorMaxAge?: string; // interval string, e.g. "30 seconds"
};

/**
 * Orchestrator manages the worker pool and handles:
 * - Worker lifecycle (start/stop)
 * - Heartbeat tracking
 * - Live migrations with graceful shutdown
 * - Stale orchestrator recovery
 */
export class Orchestrator {
	private readonly sql: Sql;
	private readonly db: DatabaseClient;
	private readonly workers: Worker[] = [];
	private readonly orchestratorId: string;
	private readonly migrationStore: MigrationStore;
	private readonly schemaManager: SchemaManager;
	private readonly heartbeatIntervalMs: number;
	private readonly staleOrchestratorMaxAge: string;

	private heartbeatTimer: Timer | null = null;
	private _deferred: Deferred<void> | null = null;
	private _abortController: AbortController | null = null;

	constructor(private options: OrchestratorOptions) {
		this.orchestratorId = crypto.randomUUID();
		this.sql = postgres(options.connectionString);
		this.db = new DatabaseClient(this.orchestratorId, this.sql);
		this.migrationStore = new MigrationStore();
		this.schemaManager = new SchemaManager(this.db);
		this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? 30000; // 30s default
		this.staleOrchestratorMaxAge =
			options.staleOrchestratorMaxAge ?? "1 minute";

		// Create workers for each task
		// TODO: concurrency and pollIntervalMs will be defined on task level
		for (const task of options.tasks) {
			const worker = new Worker(task, {}, this.db);
			this.workers.push(worker);
		}
	}

	/**
	 * Start the orchestrator:
	 * 1. Get installed version (-1 if not installed, 0+ if installed)
	 * 2. Check if we're too old (installed version > our version)
	 * 3. Run migrations if needed (migrate() handles locking/signaling/waiting)
	 * 4. Start heartbeat loop
	 * 5. Start all workers
	 */
	async start(): Promise<void> {
		if (this._deferred) {
			return this._deferred.promise;
		}

		this._deferred = new Deferred<void>();
		this._abortController = new AbortController();

		(async () => {
			try {
				const ourVersion = this.migrationStore.getLatestVersion();
				const installedVersion = await this.schemaManager.getInstalledVersion();

				// Step 1: Check if we're too old (should never happen, but safety check)
				if (installedVersion > ourVersion) {
					console.log(
						`Orchestrator version ${ourVersion} is older than installed version ${installedVersion}, shutting down`,
					);
					this.deferred.resolve();
					return;
				}

				// Step 2: Ensure schema is at latest version
				// ensureLatest() will check if migrations needed and:
				// - Try to acquire lock (non-blocking)
				// - If can't get lock, wait and recheck or shutdown
				// - Signal others to shut down (if schema exists)
				// - Wait for them to exit
				// - Apply migrations
				const { shouldShutdown } = await this.schemaManager.ensureLatest();

				if (shouldShutdown) {
					console.log(
						`Orchestrator ${this.orchestratorId} could not acquire migration lock, shutting down`,
					);
					this.deferred.resolve();
					return;
				}

				const hbShutdown = await this.db.orchestratorHeartbeat(
					this.orchestratorId,
					this.options.version,
					this.migrationStore.getLatestVersion(),
				);

				if (hbShutdown) {
					console.log(
						`Orchestrator ${this.orchestratorId} detected newer schema after migrations, shutting down`,
					);
					this.deferred.resolve();
					return;
				}

				// Start heartbeat loop
				this.startHeartbeatLoop();

				// Start all workers
				const workerPromises = this.workers.map((worker) => worker.start());

				// Wait for shutdown signal or all workers to complete
				await Promise.race([
					Promise.all(workerPromises),
					this.waitForShutdownSignal(),
				]);

				// Stop gracefully
				await this.stopWorkers();

				this.deferred.resolve();
			} catch (err) {
				console.error("Orchestrator error:", err);
				this.deferred.reject(err);
			} finally {
				await this.cleanup();
			}
		})();

		return this._deferred.promise;
	}

	/**
	 * Stop the orchestrator gracefully:
	 * 1. Stop heartbeat
	 * 2. Stop all workers
	 * 3. Clean up resources
	 */
	async stop(): Promise<void> {
		// Capture deferred before aborting (avoid race)
		const currentDeferred = this._deferred;

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
	 * - Recovers stale orchestrators periodically (every 4th heartbeat)
	 */
	private startHeartbeatLoop(): void {
		let heartbeatCount = 0;

		const beat = async () => {
			try {
				heartbeatCount = (heartbeatCount % 4) + 1;

				// Every 4th heartbeat, recover stale orchestrators
				if (heartbeatCount === 4) {
					await this.db.recoverStaleOrchestrators(this.staleOrchestratorMaxAge);
				}

				// Send heartbeat and check for shutdown signal
				const shouldShutdown = await this.db.orchestratorHeartbeat(
					this.orchestratorId,
					this.options.version,
					this.migrationStore.getLatestVersion(),
				);

				if (shouldShutdown && !this.signal.aborted) {
					console.log(
						`Orchestrator ${this.orchestratorId} received shutdown signal`,
					);
					this.abortController.abort();
				}
			} catch (err) {
				console.error("Heartbeat error:", err);
			} finally {
				// Schedule next heartbeat if not shutting down
				if (!this.abortController.signal.aborted) {
					this.heartbeatTimer = setTimeout(beat, this.heartbeatIntervalMs);
				}
			}
		};

		// Start first heartbeat
		this.heartbeatTimer = setTimeout(beat, this.heartbeatIntervalMs);
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
		console.log(`Stopping ${this.workers.length} workers...`);

		await Promise.all(this.workers.map((worker) => worker.stop()));

		console.log("All workers stopped");
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
		await this.db.orchestratorShutdown(this.orchestratorId);

		// Close database connection
		await this.sql.end();

		this._deferred = null;
		this._abortController = null;
	}

	get info() {
		return {
			id: this.orchestratorId,
			version: this.options.version,
			migrationNumber: this.migrationStore.getLatestVersion(),
			workerCount: this.workers.length,
			isRunning: this._deferred !== null,
			shutdownSignal: this._abortController?.signal.aborted ?? false,
		};
	}

	private get deferred(): Deferred<void> {
		if (!this._deferred) {
			throw new Error("Orchestrator is not running");
		}
		return this._deferred;
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
