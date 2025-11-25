import postgres from "postgres";
import { Conductor } from "../../src";
import { TaskSchemas } from "../../src/schemas";
import { perfTaskDefinitions } from "./tasks";

const COMMAND = process.env.COMMAND; // 'recreate' or 'queue'
const COUNT = Number(process.env.COUNT || 1000);
const TASK_TYPE = process.env.TASK_TYPE || "noop";
const DB_URL = process.env.DATABASE_URL!;

async function recreateDatabase() {
	// Connect to postgres database to drop/create target db
	const dbName = DB_URL.split("/").pop()!.split("?")[0];
	const postgresUrl = DB_URL.replace(/\/[^/]+(\?.*)?$/, "/postgres$1");

	const sql = postgres(postgresUrl);

	try {
		await sql.unsafe(`DROP DATABASE IF EXISTS ${dbName}`);
		await sql.unsafe(`CREATE DATABASE ${dbName}`);
	} finally {
		await sql.end();
	}

	// Install uuid-ossp extension before running migrations
	const dbSql = postgres(DB_URL);
	try {
		await dbSql.unsafe(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);
	} finally {
		await dbSql.end();
	}

	// Install schema
	const conductor = Conductor.create({
		connectionString: DB_URL,
		tasks: TaskSchemas.fromSchema(perfTaskDefinitions),
		context: {},
	});

	await conductor.ensureInstalled();
	await conductor.db.close();
}

async function queueTasks() {
	const conductor = Conductor.create({
		connectionString: DB_URL,
		tasks: TaskSchemas.fromSchema(perfTaskDefinitions),
		context: {},
	});

	// Bulk insert tasks
	const batchSize = 1000;
	for (let i = 0; i < COUNT; i += batchSize) {
		const batch = Math.min(batchSize, COUNT - i);
		const promises = [];
		for (let j = 0; j < batch; j++) {
			promises.push(conductor.invoke({ name: TASK_TYPE }, { id: i + j }));
		}
		await Promise.all(promises);
		if (batch >= 1000) {
			console.log(`Queued ${i + batch}/${COUNT}`);
		}
	}

	await conductor.db.close();
}

async function main() {
	if (COMMAND === "recreate") {
		await recreateDatabase();
	} else if (COMMAND === "queue") {
		await queueTasks();
	} else {
		throw new Error(`Unknown command: ${COMMAND}`);
	}
}

main().catch((err) => {
	console.error("Init error:", err);
	process.exit(1);
});
