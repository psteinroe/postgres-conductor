import { MigrationStore } from "./migration-store.js";
import { waitFor } from "./lib/wait-for.js";
import type { DatabaseClient } from "./database-client.js";

const POLL_INTERVAL_MS = 2000;
const POLL_JITTER_MS = 500;

/**
 * Handles all schema-related operations:
 * - Schema installation
 * - Migration execution with locking
 * - Live migration support
 * - Version tracking
 */
export class SchemaManager {
	private readonly migrationStore: MigrationStore;

	constructor(
		private readonly db: DatabaseClient,
		options: { schemaName?: string } = {},
	) {
		this.migrationStore = new MigrationStore(options.schemaName || "pgconductor");
	}

	/**
	 * Apply the migrations the database lacks, one transaction each under the migration lock.
	 * A breaking migration first signals older orchestrators to shut down and waits for them to
	 * exit. Reports `shouldShutdown` when the database is newer than this version.
	 */
	async ensureLatest(signal: AbortSignal): Promise<{
		migrated: boolean;
		shouldShutdown: boolean;
	}> {
		let installedVersion = await this.db.getInstalledMigrationNumber({ signal });
		const ourLatest = this.migrationStore.getLatestMigrationNumber();

		if (installedVersion > ourLatest) {
			console.error(
				`Database has migrations up to version ${installedVersion}, but this binary only knows ${ourLatest}. Please deploy a newer orchestrator first.`,
			);
			return { migrated: false, shouldShutdown: true };
		}

		let migrated = false;

		while (true) {
			signal.throwIfAborted();
			const nextMigration = this.migrationStore.getMigration(installedVersion + 1);
			if (!nextMigration) {
				return { migrated, shouldShutdown: false };
			}

			let fence = false;
			if (nextMigration.breaking) {
				await this.db.sweepOrchestrators(
					{
						migrationNumber: nextMigration.version,
					},
					{ signal },
				);
				fence = await this.waitForOlderOrchestratorsToExit(nextMigration.version, signal);
			}

			const status = await this.db.applyMigration(nextMigration, { fence, signal });

			if (status === "applied") {
				migrated = true;
				installedVersion = nextMigration.version;
				continue;
			}

			await waitFor(POLL_INTERVAL_MS, {
				jitter: POLL_JITTER_MS,
				signal,
			});
		}
	}

	private async waitForOlderOrchestratorsToExit(
		targetVersion: number,
		signal: AbortSignal,
		maxWaitMs: number = 4 * 60 * 60 * 1000,
	): Promise<boolean> {
		const start = Date.now();

		while (Date.now() - start < maxWaitMs) {
			signal.throwIfAborted();
			const remaining = await this.db.countActiveOrchestratorsBelow(
				{
					version: targetVersion,
				},
				{ signal },
			);

			if (remaining === 0) {
				return true;
			}

			await waitFor(POLL_INTERVAL_MS, {
				jitter: POLL_JITTER_MS,
				signal,
			});
		}

		console.warn(
			`Timed out waiting for older orchestrators to exit within ${maxWaitMs}ms, continuing with migrations`,
		);
		return false;
	}
}
