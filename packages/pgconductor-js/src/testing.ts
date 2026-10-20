import { createHash, randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { DatabaseClient } from "./database-client.js";
import { refreshClocks } from "./lib/clock.js";
import { parseDuration, type DurationInput } from "./lib/duration.js";
import { DefaultLogger } from "./lib/logger.js";
import { MigrationStore } from "./migration-store.js";
import { SchemaManager } from "./schema-manager.js";

export type StartPostgresOptions = {
	/** Docker image for the testcontainers Postgres. Defaults to `postgres:17` */
	image?: string;
};

export type TestDatabaseOptions = {
	/** Postgres server to create the database on. Defaults to `DATABASE_URL`, then to a Postgres started with testcontainers */
	url?: string;
	/** Install the Postgres Conductor schema. Defaults to true */
	migrate?: boolean;
};

export type TestClock = {
	/** Fake database time */
	set(date: Date): Promise<void>;
	/** Move database time forward, starting from real time if it is not faked */
	advance(duration: DurationInput): Promise<void>;
	/** Return to real time */
	reset(): Promise<void>;
};

export type TestDatabase = {
	/** A single-connection pool, so fake time applies to every query that uses it */
	sql: Sql;
	url: string;
	name: string;
	/** Database time. Changes reach running workers of conductors that use `sql` */
	clock: TestClock;
	/** Close `sql` and drop the database */
	destroy(): Promise<void>;
};

/**
 * Use `DATABASE_URL`, or start a Postgres container with the optional `@testcontainers/postgresql` peer dependency.
 */
export async function startPostgres({ image = "postgres:17" }: StartPostgresOptions = {}): Promise<{
	url: string;
	stop: () => Promise<void>;
}> {
	const url = process.env.DATABASE_URL;
	if (url) {
		return { url, stop: async () => {} };
	}

	const { PostgreSqlContainer } = await import("@testcontainers/postgresql").catch((error) => {
		throw new Error("Set DATABASE_URL or install @testcontainers/postgresql", { cause: error });
	});
	const container = await new PostgreSqlContainer(image).start();
	return {
		url: container.getConnectionUri(),
		stop: async () => {
			await container.stop();
		},
	};
}

let defaultServer: Promise<string> | null = null;
const templates = new Map<string, Promise<string>>();

/**
 * Create an isolated database, cloned from a template database that is migrated once per server.
 */
export async function createTestDatabase({
	url,
	migrate = true,
}: TestDatabaseOptions = {}): Promise<TestDatabase> {
	// A started container lives until the process exits
	const serverUrl = url || (await (defaultServer ||= startPostgres().then((server) => server.url)));

	const name = `pgconductor_test_${randomUUID().replaceAll("-", "")}`;
	const template = migrate ? ` template ${await migratedTemplate(serverUrl)}` : "";
	await onServer(serverUrl, (server) => server.unsafe(`create database ${name}${template}`));

	const databaseUrl = withDatabase(serverUrl, name);
	// pgconductor.fake_now is a session setting, so every query must use the same connection
	const sql = postgres(databaseUrl, { max: 1 });

	const setFakeTime = async (value: string) => {
		await sql`select set_config('pgconductor.fake_now', ${value}, false)`;
		await refreshClocks(sql);
	};

	return {
		sql,
		url: databaseUrl,
		name,
		clock: {
			set: (date) => setFakeTime(date.toISOString()),
			advance: async (duration) => {
				await sql`
					select set_config(
						'pgconductor.fake_now',
						(pgconductor._private_current_time() + interval '1 millisecond' * ${parseDuration(duration)}::float8)::text,
						false
					)
				`;
				await refreshClocks(sql);
			},
			reset: () => setFakeTime(""),
		},
		destroy: async () => {
			await sql.end();
			await onServer(serverUrl, (server) =>
				server.unsafe(`drop database if exists ${name} with (force)`),
			);
		},
	};
}

function migratedTemplate(serverUrl: string): Promise<string> {
	let template = templates.get(serverUrl);
	if (!template) {
		template = createTemplate(serverUrl);
		void template.catch(() => templates.delete(serverUrl));
		templates.set(serverUrl, template);
	}
	return template;
}

// Templates are kept and shared by every process that uses the server, one per set of migrations
async function createTemplate(serverUrl: string): Promise<string> {
	const migrations = new MigrationStore().getAllMigrations().map((migration) => migration.sql);
	const hash = createHash("sha256").update(migrations.join("\n")).digest("hex").slice(0, 16);
	const name = `pgconductor_template_${hash}`;
	const building = `${name}_building`;

	await onServer(serverUrl, async (server) => {
		// Released when the connection ends
		await server`select pg_advisory_lock(hashtext(${name}))`;
		const [existing] = await server`select 1 from pg_database where datname = ${name}`;
		if (existing) return;

		await server.unsafe(`drop database if exists ${building}`);
		await server.unsafe(`create database ${building}`);
		const sql = postgres(withDatabase(serverUrl, building), { max: 1 });
		try {
			const db = new DatabaseClient({ sql, logger: new DefaultLogger() });
			await new SchemaManager(db).ensureLatest(new AbortController().signal);
		} finally {
			await sql.end();
		}
		// A process that fails while migrating leaves no template behind
		await server.unsafe(`alter database ${building} rename to ${name}`);
	});

	return name;
}

async function onServer<T>(serverUrl: string, run: (server: Sql) => Promise<T>): Promise<T> {
	const server = postgres(serverUrl, { max: 1, onnotice: () => {} });
	try {
		return await run(server);
	} finally {
		await server.end();
	}
}

function withDatabase(serverUrl: string, name: string): string {
	const url = new URL(serverUrl);
	url.pathname = `/${name}`;
	return url.toString();
}
