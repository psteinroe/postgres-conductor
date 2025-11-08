import { getMigrations } from "./generated/sql";
import { VERSION } from "./version";

export interface Migration {
	version: number;
	name: string;
	sql: string;
}

// Manages the retrieval of migration files and versions.
export class MigrationStore {
	private migrations: Map<number, Migration>;

	constructor(private schemaName: string = "pgconductor") {
		this.migrations = this.loadMigrations();
	}

	private loadMigrations(): Map<number, Migration> {
		const migrations = new Map<number, Migration>();
		const migrationMap = getMigrations(this.schemaName);

		for (const [filename, sql] of Object.entries(migrationMap)) {
			// Extract version and name from filename (e.g., "0000000001_setup.sql")
			const parts = filename.split("_");
			const version = parseInt(parts[0]!, 10);
			const name = parts.slice(1).join("_").replace(".sql", "");

			migrations.set(version, { version, name, sql });
		}

		return migrations;
	}

	getLatestVersion(): number {
		return VERSION;
	}

	getMigration(version: number): Migration | undefined {
		return this.migrations.get(version);
	}

	getAllMigrations(): Migration[] {
		return Array.from(this.migrations.values()).sort(
			(a, b) => a.version - b.version,
		);
	}

	getMigrationsToApply(currentVersion: number): Migration[] {
		return this.getAllMigrations().filter((m) => m.version > currentVersion);
	}

	hasMigration(version: number): boolean {
		return this.migrations.has(version);
	}
}
