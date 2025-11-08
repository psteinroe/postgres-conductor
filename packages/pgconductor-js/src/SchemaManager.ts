import { VERSION } from "./version";
import { type Sql } from "postgres";
import { loadMigrations } from "./generated/sql";

export type SchemaManagerConfig = {
	schema: string;
};

// Handles all schema related operations and allows for easy schema migrations and versioning.
export class SchemaManager {
	private readonly migrations;

	constructor(
		private readonly db: Sql,
		private readonly config: SchemaManagerConfig,
	) {
		this.migrations = loadMigrations(this.config.schema);
	}

	async run() {
		const schemaVersion = await this.installedVersion();

		if (schemaVersion > VERSION) {
			await this.migrate(version);
		}
	}

	async assertUpToDate() {
		const installed = await this.isInstalled();

		if (!installed) {
			throw new Error("pgconductor is not installed");
		}

		const installedVersion = await this.installedVersion();

		if (VERSION !== installedVersion) {
			throw new Error("pgconductor database requires migrations");
		}
	}

	private async installedVersion() {
		const result = await this.db.executeSql(
			plans.getVersion(this.config.schema),
		);
		return result.rows.length ? parseInt(result.rows[0].version) : null;
	}

	private async isInstalled() {
		const result = await this.db.executeSql(
			plans.versionTableExists(this.config.schema),
		);
		return !!result.rows[0].name;
	}

	// ensureUpToDate
	// check installed and extensions exist
	// check if our version is after latest installed version
	//  if yes, acquire lock and start installation
	//  if migration is breaking, signal all other workers to shut down / stop processing. just manager still active.
	//  wait for us to be the only remaining worker
	//  install breaking migration
	//
	//
	//  bootstrap and shutdown routines
	//  graceful shutdown

	private async migrate(fromVersion: number) {
		const migrationKeys = Object.keys(this.migrations)
			.map((v) => {
				const [ts, ..._rest] = v.split("_");
				if (!ts) {
					throw new Error(`Invalid migration file name: ${v}`);
				}
				return parseInt(ts);
			})
			.filter((v) => v > fromVersion)
			.sort((a, b) => a - b);

		for (const version of migrationKeys) {
			const name = `${version}_migrations.sql`;
			const sql = this.migrations[name];
			if (!sql) {
				throw new Error(`Missing migration file: ${name}`);
			}
			// TODO: lock
			await this.db.unsafe(sql);
			await this.db.unsafe(
				plans.insertVersion(this.config.schema, version, name),
			);
		}
	}
}
