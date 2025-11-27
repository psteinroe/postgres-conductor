import { MigrationStore } from "./migration-store";
import { waitFor } from "./lib/wait-for";
import type { DatabaseClient } from "./database-client";

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
	 * Ensure schema is at the latest version by:
	 * 1. Installing the bootstrap schema if nothing exists yet
	 * 2. Seeding the migration catalog via pgconductor.enqueue_migrations()
	 * 3. Looping on pgconductor.apply_next_migration() until it reports "done"
	 */
	async ensureLatest(signal: AbortSignal): Promise<{
		migrated: boolean;
		shouldShutdown: boolean;
	}> {
		let installedVersion = await this.db.getInstalledMigrationNumber();
		const ourLatest = this.migrationStore.getLatestMigrationNumber();

		if (installedVersion > ourLatest) {
			console.error(
				`Database has migrations up to version ${installedVersion}, but this binary only knows ${ourLatest}. Please deploy a newer orchestrator first.`,
			);
			return { migrated: false, shouldShutdown: true };
		}

		let migrated = false;

		while (true) {
			const nextMigration = this.migrationStore.getMigration(installedVersion + 1);
			if (!nextMigration) {
				if (!migrated) {
					console.log("Schema is up to date");
				}
				return { migrated, shouldShutdown: false };
			}

			if (nextMigration.breaking) {
				await this.db.sweepOrchestrators({
					migrationNumber: nextMigration.version,
				});
				await this.waitForOlderOrchestratorsToExit(nextMigration.version, signal);
			}

			const status = await this.db.applyMigration(nextMigration);

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
	): Promise<void> {
		const start = Date.now();

		while (Date.now() - start < maxWaitMs) {
			const remaining = await this.db.countActiveOrchestratorsBelow({
				version: targetVersion,
			});

			if (remaining === 0) {
				return;
			}

			console.log(
				`Waiting for ${remaining} older orchestrator(s) to exit before migrating... (${Math.floor((Date.now() - start) / 1000)}s)`,
			);
			await waitFor(POLL_INTERVAL_MS, {
				jitter: POLL_JITTER_MS,
				signal,
			});
		}

		console.warn(
			`Timed out waiting for older orchestrators to exit within ${maxWaitMs}ms, continuing with migrations`,
		);
	}
}
