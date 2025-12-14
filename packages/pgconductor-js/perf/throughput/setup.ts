import postgres from "postgres";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";

let container: StartedTestContainer | null = null;
let connectionUri: string | null = null;

/**
 * Get or create a PostgreSQL testcontainer for performance testing.
 * Uses the same setup as integration tests.
 */
export async function getDatabase(): Promise<string> {
	// If DATABASE_URL is provided, use it
	if (process.env.DATABASE_URL) {
		return process.env.DATABASE_URL;
	}

	// Reuse existing container if available
	if (connectionUri) {
		return connectionUri;
	}

	console.log("Starting PostgreSQL testcontainer...");

	const containerConfig = new GenericContainer("postgres:16")
		.withEnvironment({
			POSTGRES_PASSWORD: "postgres",
			POSTGRES_USER: "postgres",
			POSTGRES_DB: "postgres",
		})
		.withExposedPorts(5432)
		.withWaitStrategy(Wait.forLogMessage(/database system is ready to accept connections/, 2));

	container = await containerConfig.start();

	const host = container.getHost();
	const port = container.getMappedPort(5432);
	connectionUri = `postgres://postgres:postgres@${host}:${port}/postgres`;

	// Verify connection and create perf test database
	console.log("Creating performance test database...");
	const masterSql = postgres(connectionUri, { max: 1, connect_timeout: 5 });
	try {
		await masterSql`SELECT 1`;
		await masterSql`DROP DATABASE IF EXISTS pgconductor_perf`;
		await masterSql`CREATE DATABASE pgconductor_perf`;
	} finally {
		await masterSql.end();
	}

	// Connect to new database and install extensions
	const perfDbUrl = connectionUri.replace(/\/postgres$/, "/pgconductor_perf");
	const perfSql = postgres(perfDbUrl, { max: 1, connect_timeout: 5 });
	try {
		await perfSql`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`;
	} finally {
		await perfSql.end();
	}

	console.log(`✓ PostgreSQL ready at ${perfDbUrl}`);
	console.log();

	connectionUri = perfDbUrl;
	return perfDbUrl;
}

/**
 * Stop and remove the container.
 */
export async function stopDatabase(): Promise<void> {
	if (container) {
		await container.stop();
		container = null;
		connectionUri = null;
	}
}
