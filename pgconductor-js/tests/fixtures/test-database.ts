import postgres, { type Sql } from "postgres";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { DatabaseClient } from "../../src/database-client";
import { DefaultLogger } from "../../src/lib/logger";

export class TestDatabase {
	public readonly sql: Sql;
	public readonly client: DatabaseClient;
	public readonly name: string;
	public readonly url: string;
	private readonly masterUrl: string;

	private constructor(sql: Sql, name: string, masterUrl: string, url: string) {
		this.sql = sql;
		this.client = new DatabaseClient({ sql, logger: new DefaultLogger() });
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

		await sql`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`;

		return new TestDatabase(sql, name, masterUrl, testDbUrl);
	}

	async close(): Promise<void> {
		await this.sql.end();
	}

	async destroy(): Promise<void> {
		await this.sql.end();

		const master = postgres(this.masterUrl, { max: 1 });
		try {
			await master.unsafe(`DROP DATABASE IF EXISTS ${this.name}`);
		} finally {
			await master.end();
		}
	}
}

export class TestDatabasePool {
	private readonly container: StartedTestContainer;
	private readonly masterUrl: string;
	private readonly children: TestDatabase[] = [];

	private constructor(container: StartedTestContainer, masterUrl: string) {
		this.container = container;
		this.masterUrl = masterUrl;
	}

	static async create(): Promise<TestDatabasePool> {
		const containerConfig = new GenericContainer("postgres:15")
			.withEnvironment({
				POSTGRES_PASSWORD: "postgres",
				POSTGRES_USER: "postgres",
				POSTGRES_DB: "postgres",
			})
			.withExposedPorts(5432)
			.withWaitStrategy(Wait.forLogMessage(/database system is ready to accept connections/, 2));

		const container = await containerConfig.start();

		const host = container.getHost();
		const port = container.getMappedPort(5432);
		const masterUrl = `postgres://postgres:postgres@${host}:${port}/postgres`;

		const testSql = postgres(masterUrl, { max: 1, connect_timeout: 5 });
		try {
			await testSql`SELECT 1`;
		} finally {
			await testSql.end();
		}

		return new TestDatabasePool(container, masterUrl);
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
		const master = postgres(this.masterUrl, { max: 1 });
		try {
			await Promise.all(
				this.children.map((child) => master.unsafe(`DROP DATABASE IF EXISTS ${child.name}`)),
			);
		} finally {
			await master.end();
		}

		this.children.length = 0;

		// Stop container
		await this.container.stop();
	}
}
