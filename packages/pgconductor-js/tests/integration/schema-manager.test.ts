import { test, expect, describe, beforeAll, afterAll, afterEach, spyOn } from "bun:test";
import { TestDatabasePool } from "../fixtures/test-database";
import type { TestDatabase } from "../fixtures/test-database";
import { DatabaseClient } from "../../src/database-client";
import { SchemaManager } from "../../src/schema-manager";
import { MigrationStore } from "../../src/migration-store";
import { DefaultLogger } from "../../src/lib/logger";
import { waitFor } from "../../src/lib/wait-for";

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
		expect(tableNames).not.toContain("_private_event_subscriptions");

		// Calling again should report no migration needed
		const result2 = await schemaManager.ensureLatest(controller.signal);
		expect(result2.shouldShutdown).toBe(false);
		expect(result2.migrated).toBe(false);
	}, 30000);

	test("breaking migration waits for signalled older orchestrators to exit", async () => {
		const db = await pool.child();
		databases.push(db);
		const client = new DatabaseClient({ sql: db.sql, logger: new DefaultLogger() });
		const controller = new AbortController();
		await new SchemaManager(client).ensureLatest(controller.signal);

		const olderId = crypto.randomUUID();
		await client.orchestratorHeartbeat({
			orchestratorId: olderId,
			version: "old",
			migrationNumber: 1,
		});

		const getMigration = MigrationStore.prototype.getMigration;
		const spy = spyOn(MigrationStore.prototype, "getMigration").mockImplementation(function (
			this: MigrationStore,
			version: number,
		) {
			if (version === 2) {
				return { version, name: "breaking", sql: "select 1", breaking: true };
			}
			return getMigration.call(this, version);
		});

		try {
			const migrating = new SchemaManager(client).ensureLatest(controller.signal);

			const state = await Promise.race([
				migrating.then(() => "migrated"),
				waitFor(3000).then(() => "waiting"),
			]);
			expect(state).toBe("waiting");

			const signals = await client.orchestratorHeartbeat({
				orchestratorId: olderId,
				version: "old",
				migrationNumber: 1,
			});
			expect(signals.map((s) => s.signal_payload?.reason)).toEqual(["breaking_migration"]);
			await client.orchestratorShutdown({ orchestratorId: olderId });

			const result = await migrating;
			expect(result.migrated).toBe(true);
			expect(await client.getInstalledMigrationNumber()).toBe(2);
		} finally {
			spy.mockRestore();
		}
	}, 30000);
});
