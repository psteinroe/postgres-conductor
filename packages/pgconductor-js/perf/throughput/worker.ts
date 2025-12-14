import { Conductor, Orchestrator } from "../../src";
import { TaskSchemas } from "../../src/schemas";
import { perfTaskDefinitions, createTaskByType } from "./tasks";

const WORKER_ID = process.env.WORKER_ID || "0";
const DB_URL = process.env.DATABASE_URL!;
const TASK_TYPE = process.env.TASK_TYPE || "noop";
const MEASURE_STARTUP = process.env.MEASURE_STARTUP === "1";

// Worker settings from environment
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 100);
const FLUSH_INTERVAL_MS = Number(process.env.FLUSH_INTERVAL_MS || 100);
const CONCURRENCY = Number(process.env.CONCURRENCY || 10);
const FETCH_BATCH_SIZE = Number(process.env.FETCH_BATCH_SIZE || 100);
const FLUSH_BATCH_SIZE = process.env.FLUSH_BATCH_SIZE
	? Number(process.env.FLUSH_BATCH_SIZE)
	: undefined;

// Task-level concurrency (optional)
const TASK_CONCURRENCY = process.env.TASK_CONCURRENCY
	? Number(process.env.TASK_CONCURRENCY)
	: undefined;

async function main() {
	const conductor = Conductor.create({
		connectionString: DB_URL,
		tasks: TaskSchemas.fromSchema(perfTaskDefinitions),
		context: {},
	});

	const task = createTaskByType(conductor, TASK_TYPE, TASK_CONCURRENCY);

	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [task] as any,
		defaultWorker: {
			pollIntervalMs: POLL_INTERVAL_MS,
			flushIntervalMs: FLUSH_INTERVAL_MS,
			concurrency: CONCURRENCY,
			fetchBatchSize: FETCH_BATCH_SIZE,
			...(FLUSH_BATCH_SIZE !== undefined && { flushBatchSize: FLUSH_BATCH_SIZE }),
		},
	});

	if (MEASURE_STARTUP) {
		// Just measure startup time (no tasks to process)
		const startupStart = Date.now();
		await orchestrator.start(); // Returns when workers are started
		const startupTime = Date.now() - startupStart;

		console.log(`STARTUP_TIME:${startupTime}`);

		await orchestrator.stop();
		process.exit(0);
	}

	// Regular execution: measure startup and execution separately
	const startupStart = Date.now();

	// Start orchestrator and wait for it to be fully started
	void orchestrator.drain(); // Kick off drain (doesn't wait)

	// Wait for orchestrator to be started (workers registered)
	await orchestrator.started;
	const startupTime = Date.now() - startupStart;

	// Now measure actual execution time
	const execStart = Date.now();

	// Wait for orchestrator to stop (all work done)
	await orchestrator.stopped;
	const execTime = Date.now() - execStart;

	// Output timing data for coordinator to parse
	// Note: We don't track per-worker task counts as all workers share the DB
	// The coordinator will query total tasks processed at the end
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
