import { DatabaseClient } from "../../src/database-client";
import { DefaultLogger } from "../../src/lib/logger";
import {
	createTestDatabase,
	type TestDatabase as Database,
	type TestDatabaseOptions,
} from "../../src/testing";

export type TestDatabase = Database & { client: DatabaseClient };

export class TestDatabasePool {
	private readonly children: TestDatabase[] = [];

	private constructor(private readonly url?: string) {}

	// Without a URL, uses the Postgres the integration project's global setup started
	static async create(url?: string): Promise<TestDatabasePool> {
		return new TestDatabasePool(url);
	}

	async child(options: Pick<TestDatabaseOptions, "migrate"> = {}): Promise<TestDatabase> {
		const db = await createTestDatabase({ url: this.url, ...options });
		const child = {
			...db,
			client: new DatabaseClient({ sql: db.sql, logger: new DefaultLogger() }),
		};
		this.children.push(child);
		return child;
	}

	async destroy(): Promise<void> {
		await Promise.all(this.children.map((child) => child.destroy()));
		this.children.length = 0;
	}
}
