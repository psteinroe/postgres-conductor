import { Conductor, Orchestrator } from "../../src";
import { TaskSchemas } from "../../src/schemas";
import { perfTaskDefinitions, createTaskA, createTaskB } from "./tasks";

const WORKER_ID = process.env.WORKER_ID || "0";
const DB_URL = process.env.DATABASE_URL!;
const TASK_A_CONCURRENCY = process.env.TASK_A_CONCURRENCY
	? Number(process.env.TASK_A_CONCURRENCY)
	: undefined;
const TASK_B_CONCURRENCY = process.env.TASK_B_CONCURRENCY
	? Number(process.env.TASK_B_CONCURRENCY)
	: undefined;

// Worker settings from environment
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 100);
const FLUSH_INTERVAL_MS = Number(process.env.FLUSH_INTERVAL_MS || 100);
const CONCURRENCY = Number(process.env.CONCURRENCY || 10);
const FETCH_BATCH_SIZE = Number(process.env.FETCH_BATCH_SIZE || 100);
const FLUSH_BATCH_SIZE = process.env.FLUSH_BATCH_SIZE
	? Number(process.env.FLUSH_BATCH_SIZE)
	: undefined;

async function main() {
	const conductor = Conductor.create({
		connectionString: DB_URL,
		tasks: TaskSchemas.fromSchema(perfTaskDefinitions),
		context: {},
	});

	const taskA = createTaskA(conductor, TASK_A_CONCURRENCY);
	const taskB = createTaskB(conductor, TASK_B_CONCURRENCY);

	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [taskA, taskB] as any,
		defaultWorker: {
			pollIntervalMs: POLL_INTERVAL_MS,
			flushIntervalMs: FLUSH_INTERVAL_MS,
			concurrency: CONCURRENCY,
			fetchBatchSize: FETCH_BATCH_SIZE,
			...(FLUSH_BATCH_SIZE !== undefined && { flushBatchSize: FLUSH_BATCH_SIZE }),
		},
	});

	// Start orchestrator and wait for it to be fully started
	void orchestrator.drain(); // Kick off drain (doesn't wait)

	// Wait for orchestrator to be started (workers registered)
	await orchestrator.started;
	const startupTime = Date.now();

	// Now measure actual execution time
	const execStart = Date.now();

	// Wait for orchestrator to stop (all work done)
	await orchestrator.stopped;
	const execTime = Date.now() - execStart;

	// Output timing data for coordinator to parse
	console.log(`WORKER_STARTUP:${startupTime}`);
	console.log(`WORKER_EXEC:${execTime}`);
	console.log(`WORKER_TASKS:0`); // Placeholder, coordinator will count total

	await conductor.db.close();

	process.exit(0);
}

main().catch((err) => {
	console.error(`Worker ${WORKER_ID} error:`, err);
	process.exit(1);
});
