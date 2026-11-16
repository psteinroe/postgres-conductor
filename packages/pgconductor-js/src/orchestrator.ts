import { createEventDispatchTask, EVENT_DISPATCH_QUEUE } from "./event-dispatch-task.js";
import { DEFAULT_WORKER_CONFIG, Worker, type WorkerConfig } from "./worker.js";
import { DatabaseClient } from "./database-client.js";
import { MigrationStore } from "./migration-store.js";
import { SchemaManager } from "./schema-manager.js";
import { Deferred } from "./lib/deferred.js";
import type { Conductor } from "./conductor.js";
import { type AnyTask, type ValidateTasksQueue, Task } from "./task.js";
import { PACKAGE_VERSION } from "./versions.js";
import { makeChildLogger, type Logger } from "./lib/logger.js";
import * as assert from "./lib/assert.js";
import { SIGNALS, type Signal } from "./lib/signals.js";
import { noop } from "./lib/noop.js";
import { coerceError } from "./lib/coerce-error.js";
import { waitFor } from "./lib/wait-for.js";

export type OrchestratorOptions<TTasks extends readonly AnyTask[] = readonly AnyTask[]> = {
	conductor: Conductor<any, any, any, any, any, any, any>;
	tasks?: ValidateTasksQueue<"default", TTasks>;
	defaultWorker?: Partial<WorkerConfig>;
	workers?: Worker[];
	/** How often the orchestrator heartbeats (default 5000) */
	heartbeatIntervalMs?: number;
	/**
	 * How long an orchestrator may go without a heartbeat before others remove it and release its
	 * executions (default 30000). At least twice `heartbeatIntervalMs`; use the same value on every
	 * orchestrator of a database
	 */
	staleAfterMs?: number;
	/**
	 * Stop gracefully on SIGINT, SIGTERM, SIGHUP, SIGUSR2 and SIGABRT, and exit at once on a second
	 * SIGINT or SIGTERM (default true). Set to false when the application handles process signals
	 * itself and calls `stop()`
	 */
	handleSignals?: boolean;
};

type InternalOrchestratorOptions = {
	conductor: Conductor<any, any, any, any, any, any, any>;
	tasks?: readonly AnyTask[];
	defaultWorker?: Partial<WorkerConfig>;
	workers?: Worker[];
	heartbeatIntervalMs?: number;
	staleAfterMs?: number;
	handleSignals?: boolean;
};

const DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000;
// Six missed heartbeats, so a crashed orchestrator's executions are released within about 35 seconds
const DEFAULT_STALE_AFTER_MS = 30_000;
// How long shutdown retries removing the orchestrator; stale recovery releases what it leaves behind
const CLEANUP_TIMEOUT_MS = 10_000;

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
	private readonly heartbeatIntervalMs: number;
	private readonly staleAfterMs: number;
	private readonly handleSignals: boolean;

	private heartbeats: Promise<void> | null = null;
	private heartbeatController: AbortController | null = null;
	private _stopDeferred: Deferred<void> | null = null;
	private _startDeferred: Deferred<void> | null = null;
	private _abortController: AbortController | null = null;

	private releaseSignalHandlers: (() => void) | null = null;

	private constructor(options: InternalOrchestratorOptions) {
		this.orchestratorId = crypto.randomUUID();
		this.db = options.conductor.db;
		this.logger = makeChildLogger(options.conductor.logger, {
			component: "orchestrator",
			orchestratorId: this.orchestratorId,
		});
		this.migrationStore = new MigrationStore();
		this.schemaManager = new SchemaManager(this.db, { logger: this.logger });
		this.heartbeatIntervalMs = options.heartbeatIntervalMs || DEFAULT_HEARTBEAT_INTERVAL_MS;
		this.staleAfterMs = options.staleAfterMs || DEFAULT_STALE_AFTER_MS;
		this.handleSignals = options.handleSignals !== false;
		if (!(this.heartbeatIntervalMs > 0) || !(this.staleAfterMs >= this.heartbeatIntervalMs * 2)) {
			throw new Error(
				`staleAfterMs (${this.staleAfterMs}) must be at least twice heartbeatIntervalMs (${this.heartbeatIntervalMs}), or live orchestrators look stale`,
			);
		}

		if (options.tasks?.length) {
			this.workers.push(
				options.conductor.buildWorker("default", options.tasks, options.defaultWorker, this.logger),
			);
		}

		for (const w of options.workers || []) {
			this.workers.push(w);
		}

		const queues = new Set<string>();
		for (const worker of this.workers) {
			if (worker.queueName === EVENT_DISPATCH_QUEUE) {
				throw new Error(`Queue "${EVENT_DISPATCH_QUEUE}" is reserved for internal use`);
			}
			if (queues.has(worker.queueName)) {
				throw new Error(
					`Orchestrator cannot configure multiple workers for queue "${worker.queueName}"; configure one worker with all tasks for that queue`,
				);
			}
			queues.add(worker.queueName);
		}

		if (options.conductor.events.length) {
			const { pollIntervalMs, flushIntervalMs } = {
				...DEFAULT_WORKER_CONFIG,
				...options.defaultWorker,
			};
			this.workers.push(
				options.conductor.buildWorker(
					EVENT_DISPATCH_QUEUE,
					[createEventDispatchTask(this.db, options.conductor.options.eventRetention)],
					{
						concurrency: 1,
						fetchBatchSize: 10,
						flushBatchSize: 10,
						pollIntervalMs,
						flushIntervalMs,
					},
					this.logger,
				),
			);
		}
	}

	static create<const TTasks extends readonly Task<any, "default", any, any, any, any>[]>(
		options: OrchestratorOptions<TTasks>,
	): Orchestrator {
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

	private async _internalStart({ runOnce = false }: { runOnce?: boolean } = {}): Promise<void> {
		if (this._stopDeferred) {
			throw new Error("Orchestrator is already running");
		}

		this._stopDeferred = new Deferred<void>();
		// Startup can fail before callers have a reason to observe `stopped`.
		// Keep the rejection available to explicit awaiters while marking it handled.
		this._stopDeferred.promise.catch(noop);
		this._startDeferred = new Deferred<void>();
		this._abortController = new AbortController();
		if (this.handleSignals) this.registerSignalHandlers();
		// Heartbeats run on a connection of their own, so handlers holding every pool connection cannot
		// make the orchestrator look stale
		const heartbeatDb = this.db.withOwnConnection();

		(async () => {
			let error: Error | undefined;

			try {
				// Ensure schema is at latest version
				// ensureLatest() will check if migrations needed and:
				// - Try to acquire lock (non-blocking)
				// - If can't get lock, wait and recheck or shutdown
				// - Signal others to shut down (if schema exists)
				// - Wait for them to exit
				// - Apply migrations
				const { shouldShutdown } = await this.schemaManager.ensureLatest(this.signal);

				if (shouldShutdown) {
					throw new Error("The database schema is newer than this version of Postgres Conductor");
				}

				const { shutdown_reason } = await heartbeatDb.registerOrchestrator(
					{
						orchestratorId: this.orchestratorId,
						version: PACKAGE_VERSION,
						migrationNumber: this.migrationStore.getLatestMigrationNumber(),
					},
					{ signal: this.signal },
				);

				if (shutdown_reason) {
					throw new Error(`Received shutdown signal during startup: ${shutdown_reason}`);
				}

				this.heartbeatController = new AbortController();
				this.heartbeats = this.runHeartbeats(heartbeatDb, this.heartbeatController.signal);

				const shutdown = this.waitForShutdownSignal();
				const workerLifecycles: Promise<void>[] = [];
				for (const worker of this.workers) {
					const lifecycle = runOnce
						? worker.drain(this.orchestratorId)
						: worker.run(this.orchestratorId);
					// Mark each lifecycle as observed before awaiting registration.
					lifecycle.catch(noop);
					workerLifecycles.push(lifecycle);
					// Registration retries until the worker stops, which happens in the catch below
					await Promise.race([worker.started, shutdown]);
					if (this.signal.aborted) throw new Error("Orchestrator was stopped during startup");
				}
				const allWorkers = Promise.all(workerLifecycles);

				// NOW signal that orchestrator has started
				this.startDeferred.resolve();

				// Wait for shutdown signal or all workers to complete
				await Promise.race([allWorkers, shutdown]);

				// Stop gracefully, and fail if a worker could not settle its results
				await this.stopWorkers();
				await allWorkers;
			} catch (err) {
				error = coerceError(err);
				this.logger.error(err);
				await this.stopWorkers();

				// Only reject startDeferred if startup hasn't completed yet
				if (!this.startDeferred.isSettled) {
					this.startDeferred.reject(error);
				}
			} finally {
				// capture before cleanup nulls it
				const stopDeferred = this._stopDeferred;

				try {
					await this.cleanup(heartbeatDb);
				} catch (cleanupErr) {
					this.logger.error("Cleanup failed:", cleanupErr);
					error = coerceError(cleanupErr);
				}

				// resolve/reject AFTER cleanup, based on ANY error
				if (error) {
					stopDeferred?.reject(error);
				} else {
					stopDeferred?.resolve();
				}
			}
		})();

		// Wait for start to complete
		return this._startDeferred.promise;
	}

	/**
	 * Stop the orchestrator gracefully:
	 * 1. Stop all workers
	 * 2. Stop heartbeat
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
	 * Run the heartbeat loop until cleanup stops it, so the orchestrator stays live while workers drain:
	 * - Updates last_heartbeat_at every heartbeat
	 * - Stops when the database asks for a shutdown, such as for a newer migration
	 * - Stops when another orchestrator recovered us as stale
	 * - Recovers stale orchestrators after every heartbeat once its heartbeats have succeeded for the
	 *   stale age. This holds from registration on, so an orchestrator that starts right after a
	 *   database outage does not remove others that are still reconnecting
	 */
	private async runHeartbeats(db: DatabaseClient, signal: AbortSignal): Promise<void> {
		// Registration heartbeated. Null after a failed heartbeat
		let liveSince: number | null = Date.now();
		while (true) {
			await waitFor(this.heartbeatIntervalMs, { signal });
			if (signal.aborted) return;

			const sentAt = Date.now();
			try {
				// A heartbeat that takes the stale age is cancelled, so the next one can find out that the
				// orchestrator was recovered; stopping cancels it at once
				const heartbeat = await db.orchestratorHeartbeat(
					{ orchestratorId: this.orchestratorId },
					{ deadline: AbortSignal.any([signal, AbortSignal.timeout(this.staleAfterMs)]) },
				);

				// Our row is gone, so another orchestrator recovered our executions
				if (!heartbeat) {
					this.logger.warn("Orchestrator was recovered as stale, shutting down");
					this.abortController.abort();
					return;
				}

				if (heartbeat.shutdown_reason && !this.isShuttingDown) {
					this.logger.info(`Received shutdown signal: ${heartbeat.shutdown_reason}`);
					this.abortController.abort();
				}

				// The database may have been unreachable for the others as well, so after a failed or slow
				// heartbeat they get the stale age to heartbeat again
				const now = Date.now();
				liveSince = now - sentAt > this.heartbeatIntervalMs ? now : liveSince || now;
				if (now - liveSince >= this.staleAfterMs) {
					await db.recoverStaleOrchestrators(
						{ maxAge: `${this.staleAfterMs} milliseconds` },
						{ signal, deadline: signal },
					);
				}
			} catch (err) {
				if (signal.aborted) return;
				liveSince = null;
				this.logger.error("Heartbeat error:", err);
			}
		}
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
	 * This will register the signal handlers to make sure the worker shuts down
	 * gracefully if it can. It will only register signal handlers once.
	 */
	private registerSignalHandlers() {
		assert.ok(!this.isStopped, "Why are we registering signal handlers when stopped?");
		assert.ok(!this.isShuttingDown, "Why are we registering signal handlers when shutting down?");

		const shutdownHandler = (signal: Signal) => {
			if (this.isShuttingDown) {
				// A second interrupt or termination means the caller will not wait: exit as without us
				if (signal === "SIGINT" || signal === "SIGTERM") {
					this.logger.warn(`Received ${signal} again while shutting down, exiting at once`);
					this.releaseSignalHandlers?.();
					process.kill(process.pid, signal);
					return;
				}
				this.logger.debug(`Ignoring duplicate signal: ${signal}, already shutting down`);
				return;
			}

			this.stop().catch(noop);
		};

		const stdioErrorHandler = () => {
			// when the pipe is broken, we need to noop stdout errors to prevent recursive crashes
			// otherwise, any logging during shutdown will cause a loop of errors
			// thanks graphile-worker for this tip!
			process.stdout.on("error", noop);
			process.stdout.off("error", stdioErrorHandler);
			process.stderr.on("error", noop);
			process.stderr.off("error", stdioErrorHandler);

			// trigger graceful handler
			shutdownHandler("SIGPIPE");
		};

		for (const signal of SIGNALS) {
			/*
			 * Though SIGPIPE is a terminal signal _normally_, this isn't the case for
			 * Node.js since libuv handles it. From the Node docs:
			 *
			 * > 'SIGPIPE' is ignored by default. It can have a listener installed.
			 * > -- https://nodejs.org/api/process.html
			 *
			 * We don't want the process to exit on SIGPIPE, so we ignore it (and rely on
			 * Node.js to handle it through the normal error channels).
			 */
			if (signal === "SIGPIPE") {
				continue;
			}
			process.on(signal, shutdownHandler);
		}
		process.stdout.on("error", stdioErrorHandler);
		process.stderr.on("error", stdioErrorHandler);

		this.releaseSignalHandlers = () => {
			for (const signal of SIGNALS) {
				process.off(signal, shutdownHandler);
			}
			process.stdout.off("error", stdioErrorHandler);
			process.stderr.off("error", stdioErrorHandler);
			this.releaseSignalHandlers = null;
		};
	}

	/**
	 * Clean up resources:
	 * - Stop heartbeat
	 * - Remove orchestrator from database and release locked executions
	 */
	private async cleanup(heartbeatDb: DatabaseClient): Promise<void> {
		try {
			// Stop heartbeat; stopping cancels a statement in flight
			this.heartbeatController?.abort();
			await this.heartbeats;

			// Remove ourselves from orchestrators table and release locked executions, retrying until the
			// deadline, which also cancels a statement still running
			const deadline = AbortSignal.timeout(CLEANUP_TIMEOUT_MS);
			await heartbeatDb.orchestratorShutdown(
				{ orchestratorId: this.orchestratorId },
				{ signal: deadline, deadline },
			);
		} finally {
			await heartbeatDb.close();
			this.heartbeats = null;
			this.heartbeatController = null;
			this._stopDeferred = null;
			this._startDeferred = null;
			this._abortController = null;
			this.releaseSignalHandlers?.();
		}
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

	get isStarted(): boolean {
		return this._startDeferred !== null;
	}

	get isStopped(): boolean {
		return this._stopDeferred === null;
	}

	get isShuttingDown(): boolean {
		return this._abortController?.signal.aborted || false;
	}
}
