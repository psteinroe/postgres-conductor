import postgres, { type Sql } from "postgres";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { inject } from "vitest";
import { DatabaseClient } from "../../src/database-client";
import { DefaultLogger } from "../../src/lib/logger";
import { createTestClock, type TestClock } from "../../src/testing";

export class TestDatabase {
	public readonly sql: Sql;
	public readonly client: DatabaseClient;
	public readonly clock: TestClock;
	public readonly name: string;
	public readonly url: string;
	private readonly masterUrl: string;

	private constructor(sql: Sql, name: string, masterUrl: string, url: string) {
		this.sql = sql;
		this.client = new DatabaseClient({ sql, logger: new DefaultLogger() });
		this.clock = createTestClock(sql);
		this.name = name;
		this.masterUrl = masterUrl;
		this.url = url;
	}

	static async create(masterUrl: string): Promise<TestDatabase> {
		const name = `pgc_test_${crypto.randomUUID().replace(/-/g, "_")}`;

		const master = postgres(masterUrl, { max: 1 });

		try {
			await master.unsafe(`CREATE DATABASE ${name}`);
		} finally {
			await master.end();
		}

		const testDbUrl = masterUrl.replace(/\/[^/]+$/, `/${name}`);
		// Use max: 1 to ensure only one connection, making fake time work reliably
		const sql = postgres(testDbUrl, { max: 1 });

		return new TestDatabase(sql, name, masterUrl, testDbUrl);
	}

	// Claims need a registered owner
	async registerOrchestrator(orchestratorId: string = crypto.randomUUID()): Promise<string> {
		await this.client.registerOrchestrator({ orchestratorId, version: "test", migrationNumber: 1 });
		return orchestratorId;
	}

	async close(): Promise<void> {
		await this.sql.end();
	}

	async destroy(): Promise<void> {
		await this.sql.end();

		const master = postgres(this.masterUrl, { max: 1, onnotice: () => {} });
		try {
			await master.unsafe(`DROP DATABASE IF EXISTS ${this.name}`);
		} finally {
			await master.end();
		}
	}
}

export async function startPostgres(): Promise<{ url: string; stop: () => Promise<unknown> }> {
	// If DATABASE_URL is set (e.g., in CI), use it instead of starting a container
	const databaseUrl = process.env.DATABASE_URL;
	if (databaseUrl) {
		return { url: databaseUrl, stop: async () => {} };
	}

	const container = await new PostgreSqlContainer("postgres:15")
		.withCommand(["postgres", "-c", "wal_level=logical"])
		.start();
	return { url: container.getConnectionUri(), stop: () => container.stop() };
}

export class TestDatabasePool {
	private readonly masterUrl: string;
	private readonly children: TestDatabase[] = [];

	private constructor(masterUrl: string) {
		this.masterUrl = masterUrl;
	}

	// The integration project's global setup starts one Postgres and provides its URL
	static async create(masterUrl = inject("databaseUrl")): Promise<TestDatabasePool> {
		return new TestDatabasePool(masterUrl);
	}

	async child(): Promise<TestDatabase> {
		const db = await TestDatabase.create(this.masterUrl);
		this.children.push(db);
		return db;
	}

	async destroy(): Promise<void> {
		// Close all child connections
		await Promise.all(this.children.map((child) => child.sql.end()));

		// Drop all databases
		const master = postgres(this.masterUrl, { max: 1, onnotice: () => {} });
		try {
			await Promise.all(
				this.children.map((child) => master.unsafe(`DROP DATABASE IF EXISTS ${child.name}`)),
			);
		} finally {
			await master.end();
		}

		this.children.length = 0;
	}
}
