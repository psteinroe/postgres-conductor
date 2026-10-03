import { PGlite } from "@electric-sql/pglite";
import { btree_gist } from "@electric-sql/pglite/contrib/btree_gist";
import { PGLiteSocketServer } from "@electric-sql/pglite-socket";
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
	private pglite: { db: PGlite; server: PGLiteSocketServer; destroyed: boolean } | null = null;

	private constructor(sql: Sql, name: string, masterUrl: string, url: string) {
		this.sql = sql;
		this.client = new DatabaseClient({ sql, logger: new DefaultLogger() });
		this.name = name;
		this.masterUrl = masterUrl;
		this.url = url;
	}

	// research/in-memory-runtime: a fresh in-process PGlite served over the wire protocol so
	// postgres.js connects unchanged. All connections share PGlite's single backend session.
	static async createPglite(template: PGlite): Promise<TestDatabase> {
		const db = await template.clone();
		if (!(db instanceof PGlite)) throw new Error("PGlite clone is not a PGlite instance");
		const server = new PGLiteSocketServer({ db, port: 0, host: "127.0.0.1", maxConnections: 100 });
		await server.start();
		const url = `postgres://postgres:postgres@${server.getServerConn()}/postgres`;
		const sql = postgres(url, { max: 1 });
		const testDb = new TestDatabase(sql, "pglite", "", url);
		testDb.pglite = { db, server, destroyed: false };
		return testDb;
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

	async close(): Promise<void> {
		await this.sql.end();
	}

	async destroy(): Promise<void> {
		if (this.pglite) {
			// A connection left inside a transaction stalls every other connection, so bound teardown.
			const { db, server } = this.pglite;
			if (this.pglite.destroyed) return;
			this.pglite.destroyed = true;
			const teardown = (async () => {
				await this.sql.end({ timeout: 0 });
				await server.stop();
				// Socket close handlers still touch the database after stop() resolves.
				await Bun.sleep(250);
				await db.close();
			})().catch(() => {});
			await Promise.race([teardown, Bun.sleep(3000)]);
			return;
		}
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
	private readonly container: StartedTestContainer | null;
	private readonly masterUrl: string;
	private readonly children: TestDatabase[] = [];
	private pgliteTemplate: PGlite | null = null;

	private constructor(container: StartedTestContainer | null, masterUrl: string) {
		this.container = container;
		this.masterUrl = masterUrl;
	}

	static async create(): Promise<TestDatabasePool> {
		if (process.env.PGC_TEST_BACKEND === "pglite") {
			const pool = new TestDatabasePool(null, "");
			pool.pgliteTemplate = await PGlite.create({ extensions: { btree_gist } });
			return pool;
		}

		// If DATABASE_URL is set (e.g., in CI), use it instead of starting a container
		const databaseUrl = process.env.DATABASE_URL;

		if (databaseUrl) {
			// CI mode: use existing Postgres service
			const testSql = postgres(databaseUrl, { max: 1, connect_timeout: 5 });
			try {
				await testSql`SELECT 1`;
			} finally {
				await testSql.end();
			}

			// Return pool without container (container will be null)
			return new TestDatabasePool(null, databaseUrl);
		}

		// Local mode: start testcontainer
		const containerConfig = new GenericContainer("postgres:15")
			.withEnvironment({
				POSTGRES_PASSWORD: "postgres",
				POSTGRES_USER: "postgres",
				POSTGRES_DB: "postgres",
			})
			.withCommand(["postgres", "-c", "wal_level=logical"])
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
		const db = this.pgliteTemplate
			? await TestDatabase.createPglite(this.pgliteTemplate)
			: await TestDatabase.create(this.masterUrl);
		this.children.push(db);
		return db;
	}

	async destroy(): Promise<void> {
		if (this.pgliteTemplate) {
			await Promise.all(this.children.map((child) => child.destroy().catch(() => {})));
			this.children.length = 0;
			await this.pgliteTemplate.close();
			return;
		}

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

		// Stop container if running in local mode
		if (this.container) {
			await this.container.stop();
		}
	}
}
