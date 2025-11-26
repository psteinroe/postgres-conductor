#!/usr/bin/env bun
import { spawn } from "child_process";
import { join } from "path";
import { scenarios, type ScenarioName } from "./scenarios";
import { getDatabase } from "./setup";

// Get scenario name from command line or default
const scenarioName = (process.argv[2] || "default") as ScenarioName;

if (!(scenarioName in scenarios)) {
	console.error(`Unknown scenario: ${scenarioName}`);
	console.error(`Available scenarios: ${Object.keys(scenarios).join(", ")}`);
	process.exit(1);
}

const scenario = scenarios[scenarioName];

async function spawnWorker(
	script: string,
	dbUrl: string,
	env: Record<string, string> = {},
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
	return new Promise((resolve, reject) => {
		const child = spawn("bun", [join(__dirname, script)], {
			env: { ...process.env, DATABASE_URL: dbUrl, ...env },
			stdio: ["ignore", "pipe", "pipe"],
		});

		let stdout = "";
		let stderr = "";

		child.stdout.on("data", (data) => {
			stdout += data.toString();
		});

		child.stderr.on("data", (data) => {
			stderr += data.toString();
		});

		child.on("close", (code) => {
			resolve({ stdout, stderr, exitCode: code || 0 });
		});

		child.on("error", reject);
	});
}

async function main() {
	// 0. Get database connection (starts testcontainer if needed)
	const DB_URL = await getDatabase();

	console.log("=== PgConductor Performance Test ===");
	console.log(`Scenario: ${scenarioName}`);
	console.log(`Workers: ${scenario.workers}`);
	console.log(`Tasks: ${scenario.tasks}`);
	console.log(`Task Type: ${scenario.taskType}`);
	console.log(`Worker Settings:`, scenario.workerSettings);
	console.log();

	// 1. Setup database
	console.log("Setting up database...");
	const setupStart = Date.now();
	const setupResult = await spawnWorker("init.ts", DB_URL, {
		COMMAND: "recreate",
	});
	if (setupResult.exitCode !== 0) {
		console.error("Setup failed:", setupResult.stderr);
		process.exit(1);
	}
	console.log(`✓ Database ready (${Date.now() - setupStart}ms)`);
	console.log();

	// 2. Measure startup time (single worker, empty queue)
	console.log("Measuring startup overhead...");
	const startupEnv: Record<string, string> = {
		WORKER_ID: "startup",
		MEASURE_STARTUP: "1",
		TASK_TYPE: scenario.taskType,
		POLL_INTERVAL_MS: scenario.workerSettings.pollIntervalMs.toString(),
		FLUSH_INTERVAL_MS: scenario.workerSettings.flushIntervalMs.toString(),
		CONCURRENCY: scenario.workerSettings.concurrency.toString(),
		FETCH_BATCH_SIZE: scenario.workerSettings.fetchBatchSize.toString(),
	};
	if ("flushBatchSize" in scenario.workerSettings && scenario.workerSettings.flushBatchSize !== undefined) {
		startupEnv.FLUSH_BATCH_SIZE = scenario.workerSettings.flushBatchSize.toString();
	}
	const startupResult = await spawnWorker("worker.ts", DB_URL, startupEnv);
	if (startupResult.exitCode !== 0) {
		console.error("Startup measurement failed:", startupResult.stderr);
		process.exit(1);
	}
	const startupMatch = startupResult.stdout.match(/STARTUP_TIME:(\d+)/);
	const startupTime = startupMatch ? Number(startupMatch[1]) : 0;
	console.log(`✓ Startup: ${startupTime}ms`);
	console.log();

	// 3. Queue tasks
	console.log(`Queuing ${scenario.tasks} tasks...`);
	const queueStart = Date.now();
	const queueResult = await spawnWorker("init.ts", DB_URL, {
		COMMAND: "queue",
		COUNT: scenario.tasks.toString(),
		TASK_TYPE: scenario.taskType,
	});
	if (queueResult.exitCode !== 0) {
		console.error("Queue failed:", queueResult.stderr);
		process.exit(1);
	}
	const queueDuration = Date.now() - queueStart;
	console.log(`✓ Queued in ${queueDuration}ms`);
	console.log(
		`  Rate: ${((scenario.tasks / queueDuration) * 1000).toFixed(2)} tasks/sec`,
	);
	console.log();

	// 4. Run N workers in parallel
	console.log(
		`Processing ${scenario.tasks} tasks with ${scenario.workers} workers...`,
	);
	const execStart = Date.now();

	const workerPromises = [];
	for (let i = 0; i < scenario.workers; i++) {
		const env: Record<string, string> = {
			WORKER_ID: i.toString(),
			TASK_TYPE: scenario.taskType,
			POLL_INTERVAL_MS: scenario.workerSettings.pollIntervalMs.toString(),
			FLUSH_INTERVAL_MS: scenario.workerSettings.flushIntervalMs.toString(),
			CONCURRENCY: scenario.workerSettings.concurrency.toString(),
			FETCH_BATCH_SIZE: scenario.workerSettings.fetchBatchSize.toString(),
		};
		if ("flushBatchSize" in scenario.workerSettings && scenario.workerSettings.flushBatchSize !== undefined) {
			env.FLUSH_BATCH_SIZE = scenario.workerSettings.flushBatchSize.toString();
		}
		workerPromises.push(spawnWorker("worker.ts", DB_URL, env));
	}

	const workerResults = await Promise.all(workerPromises);

	// Check for errors
	for (const result of workerResults) {
		if (result.exitCode !== 0) {
			console.error("Worker failed:", result.stderr);
			process.exit(1);
		}
	}

	const totalDuration = Date.now() - execStart;

	// Parse worker timings
	const workerTimings = workerResults.map((r) => {
		const startupMatch = r.stdout.match(/WORKER_STARTUP:(\d+)/);
		const execMatch = r.stdout.match(/WORKER_EXEC:(\d+)/);
		return {
			startup: startupMatch ? Number(startupMatch[1]) : 0,
			execution: execMatch ? Number(execMatch[1]) : 0,
		};
	});

	// Query total tasks completed from database
	const { default: postgres } = await import("postgres");
	const sql = postgres(DB_URL);
	const result = await sql<{ count: string }[]>`
		SELECT COUNT(*) as count
		FROM pgconductor.executions
		WHERE completed_at IS NOT NULL
	`;
	const totalProcessed = Number(result[0]?.count || 0);
	await sql.end();

	const avgStartup =
		workerTimings.reduce((sum, w) => sum + w.startup, 0) / scenario.workers;
	const maxExecTime = Math.max(...workerTimings.map((w) => w.execution));

	// 5. Report results
	console.log();
	console.log("=== Results ===");
	console.log(`Total time: ${totalDuration}ms`);
	console.log(`Tasks processed: ${totalProcessed}/${scenario.tasks}`);
	console.log();
	console.log("Timing breakdown:");
	console.log(`  Avg worker startup: ${avgStartup.toFixed(0)}ms`);
	console.log(`  Max execution time: ${maxExecTime}ms`);
	console.log();
	console.log("Throughput:");
	console.log(
		`  Overall: ${((totalProcessed / totalDuration) * 1000).toFixed(2)} tasks/sec`,
	);
	console.log(
		`  Per worker: ${((totalProcessed / scenario.workers / totalDuration) * 1000).toFixed(2)} tasks/sec`,
	);
	console.log();
	console.log("Per-worker timing:");
	workerTimings.forEach((timing, i) => {
		console.log(
			`  Worker ${i}: startup ${timing.startup}ms, execution ${timing.execution}ms`,
		);
	});

	process.exit(0);
}

main().catch((err) => {
	console.error("Error:", err);
	process.exit(1);
});
