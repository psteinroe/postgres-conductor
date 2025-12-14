import { test, expect, describe, beforeAll, afterAll, afterEach } from "bun:test";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { DatabaseClient } from "../../src/database-client";
import { SchemaManager } from "../../src/schema-manager";
import { DefaultLogger } from "../../src/lib/logger";

describe("SchemaManager", () => {
	let pool: TestDatabasePool;
	const databases: TestDatabase[] = [];

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterEach(async () => {
		await Promise.all(databases.map((db) => db.destroy()));
		databases.length = 0;
	});

	afterAll(async () => {
		await pool?.destroy();
	});

	test("ensureLatest works on clean database", async () => {
		const db = await pool.child();
		databases.push(db);
		const client = new DatabaseClient({ sql: db.sql, logger: new DefaultLogger() });
		const schemaManager = new SchemaManager(client);

		const controller = new AbortController();
		const result = await schemaManager.ensureLatest(controller.signal);

		expect(result.shouldShutdown).toBe(false);
		expect(result.migrated).toBe(true);

		const tables = await db.sql<{ table_name: string }[]>`
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = 'pgconductor'
			ORDER BY table_name
		`;

		const tableNames = tables.map((t) => t.table_name);
		expect(tableNames).toContain("schema_migrations");
		expect(tableNames).toContain("_private_tasks");
		expect(tableNames).toContain("_private_executions");

		// Calling again should report no migration needed
		const result2 = await schemaManager.ensureLatest(controller.signal);
		expect(result2.shouldShutdown).toBe(false);
		expect(result2.migrated).toBe(false);
	}, 30000);
});
