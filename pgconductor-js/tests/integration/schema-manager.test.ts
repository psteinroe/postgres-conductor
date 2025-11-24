import { test, expect, describe, beforeAll, afterAll } from "bun:test";
import { TestDatabasePool } from "../fixtures/test-database";
import { DatabaseClient } from "../../src/database-client";
import { SchemaManager } from "../../src/schema-manager";

describe("SchemaManager", () => {
	let pool: TestDatabasePool;

	beforeAll(async () => {
		pool = await TestDatabasePool.create();
	}, 60000);

	afterAll(async () => {
		await pool?.destroy();
	});

	test("ensureLatest works on clean database", async () => {
		const db = await pool.child();
		const client = new DatabaseClient({ sql: db.sql });
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
		expect(tableNames).toContain("tasks");
		expect(tableNames).toContain("executions");

		// Calling again should report no migration needed
		const result2 = await schemaManager.ensureLatest(controller.signal);
		expect(result2.shouldShutdown).toBe(false);
		expect(result2.migrated).toBe(false);
	}, 30000);
});
