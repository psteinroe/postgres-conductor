#!/usr/bin/env bun
/**
 * Benchmark to measure overhead of mixing tasks with and without concurrency limits
 *
 * Tests whether having ONE task with concurrency limit slows down OTHER tasks
 * that don't have limits in the same queue.
 */

import { spawn } from "child_process";
import { join } from "path";
import { getDatabase } from "./throughput/setup";
import postgres from "postgres";

interface Scenario {
	name: string;
	description: string;
	taskAConcurrency?: number;
	taskBConcurrency?: number;
}

const scenarios: Scenario[] = [
	{
		name: "both-unlimited",
		description: "Both tasks unlimited (baseline - no concurrency overhead)",
		// No concurrency limits
	},
	{
		name: "one-limited",
		description: "Task A limited to 1, Task B unlimited (mixed scenario)",
		taskAConcurrency: 1,
		// Task B unlimited
	},
	{
		name: "both-limited",
		description: "Both tasks limited (A=1, B=10)",
		taskAConcurrency: 1,
		taskBConcurrency: 10,
	},
];

const workerSettings = {
	pollIntervalMs: 50,
	flushIntervalMs: 50,
	concurrency: 50,
	fetchBatchSize: 100,
};

async function spawnWorker(
	script: string,
	dbUrl: string,
	env: Record<string, string> = {},
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
	return new Promise((resolve, reject) => {
		const child = spawn("bun", [join(__dirname, "throughput", script)], {
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

async function runScenario(scenario: Scenario, dbUrl: string) {
	console.log(`\n${"=".repeat(70)}`);
	console.log(`Running: ${scenario.name}`);
	console.log(`${scenario.description}`);
	console.log(`${"=".repeat(70)}\n`);

	// 1. Setup database
	console.log("Setting up database...");
	const setupResult = await spawnWorker("init-mixed.ts", dbUrl, {
		COMMAND: "recreate",
	});
	if (setupResult.exitCode !== 0) {
		throw new Error(`Setup failed: ${setupResult.stderr}`);
	}
	console.log("✓ Database ready\n");

	// 2. Queue 10k tasks (50% task-a, 50% task-b)
	const taskCount = 10000;
	console.log(`Queuing ${taskCount} tasks (50% task-a, 50% task-b)...`);
	const queueStart = Date.now();
	const queueResult = await spawnWorker("init-mixed.ts", dbUrl, {
		COMMAND: "queue",
		COUNT: taskCount.toString(),
	});
	if (queueResult.exitCode !== 0) {
		throw new Error(`Queue failed: ${queueResult.stderr}`);
	}
	const queueDuration = Date.now() - queueStart;
	console.log(`✓ Queued in ${queueDuration}ms\n`);

	// 3. Run 4 workers
	const workerCount = 4;
	console.log(`Processing with ${workerCount} workers...\n`);
	const execStart = Date.now();

	const workerPromises = [];
	for (let i = 0; i < workerCount; i++) {
		const env: Record<string, string> = {
			WORKER_ID: i.toString(),
			POLL_INTERVAL_MS: workerSettings.pollIntervalMs.toString(),
			FLUSH_INTERVAL_MS: workerSettings.flushIntervalMs.toString(),
			CONCURRENCY: workerSettings.concurrency.toString(),
			FETCH_BATCH_SIZE: workerSettings.fetchBatchSize.toString(),
		};

		if (scenario.taskAConcurrency !== undefined) {
			env.TASK_A_CONCURRENCY = scenario.taskAConcurrency.toString();
		}
		if (scenario.taskBConcurrency !== undefined) {
			env.TASK_B_CONCURRENCY = scenario.taskBConcurrency.toString();
		}

		workerPromises.push(spawnWorker("worker-mixed.ts", dbUrl, env));
	}

	const workerResults = await Promise.all(workerPromises);

	// Check for errors
	for (const result of workerResults) {
		if (result.exitCode !== 0) {
			throw new Error(`Worker failed: ${result.stderr}`);
		}
	}

	const totalDuration = Date.now() - execStart;

	// Query tasks by type
	const sql = postgres(dbUrl);
	const taskACount = await sql<{ count: string }[]>`
		SELECT COUNT(*) as count
		FROM pgconductor._private_executions
		WHERE task_key = 'task-a' AND completed_at IS NOT NULL
	`;
	const taskBCount = await sql<{ count: string }[]>`
		SELECT COUNT(*) as count
		FROM pgconductor._private_executions
		WHERE task_key = 'task-b' AND completed_at IS NOT NULL
	`;
	await sql.end();

	const taskAProcessed = Number(taskACount[0]?.count || 0);
	const taskBProcessed = Number(taskBCount[0]?.count || 0);
	const totalProcessed = taskAProcessed + taskBProcessed;

	console.log("=== Results ===");
	console.log(`Total time: ${totalDuration}ms`);
	console.log(`Tasks processed: ${totalProcessed}/${taskCount}`);
	console.log(`  Task A: ${taskAProcessed}`);
	console.log(`  Task B: ${taskBProcessed}`);
	console.log();
	console.log("Throughput:");
	console.log(`  Overall: ${((totalProcessed / totalDuration) * 1000).toFixed(2)} tasks/sec`);
	console.log(`  Task A: ${((taskAProcessed / totalDuration) * 1000).toFixed(2)} tasks/sec`);
	console.log(`  Task B: ${((taskBProcessed / totalDuration) * 1000).toFixed(2)} tasks/sec`);

	return {
		scenario: scenario.name,
		totalDuration,
		totalThroughput: (totalProcessed / totalDuration) * 1000,
		taskAThroughput: (taskAProcessed / totalDuration) * 1000,
		taskBThroughput: (taskBProcessed / totalDuration) * 1000,
	};
}

async function main() {
	const dbUrl = await getDatabase();

	console.log("=".repeat(70));
	console.log("Mixed Concurrency Overhead Benchmark");
	console.log("=".repeat(70));
	console.log();
	console.log("Goal: Measure if having ONE task with concurrency limit");
	console.log("      slows down OTHER tasks without limits in the same queue");
	console.log();
	console.log("Setup: 4 workers, 10k tasks (5k task-a + 5k task-b)");
	console.log(`       Worker settings: concurrency=${workerSettings.concurrency}`);
	console.log();

	const results = [];

	for (const scenario of scenarios) {
		const result = await runScenario(scenario, dbUrl);
		results.push(result);
	}

	// Print comparison table
	console.log("\n");
	console.log("=".repeat(70));
	console.log("Summary");
	console.log("=".repeat(70));
	console.log();

	const baseline = results[0];
	if (!baseline) {
		throw new Error("No baseline result found");
	}

	console.log("Scenario          │ Overall (t/s) │ Task A (t/s) │ Task B (t/s) │ Overhead");
	console.log("─".repeat(70));

	for (const result of results) {
		const scenario = result.scenario.padEnd(17);
		const overall = result.totalThroughput.toFixed(2).padStart(13);
		const taskA = result.taskAThroughput.toFixed(2).padStart(12);
		const taskB = result.taskBThroughput.toFixed(2).padStart(12);
		const overhead =
			`${((1 - result.totalThroughput / baseline.totalThroughput) * 100).toFixed(1)}%`.padStart(8);

		console.log(`${scenario} │ ${overall} │ ${taskA} │ ${taskB} │ ${overhead}`);
	}

	console.log();
	console.log("=".repeat(70));
	console.log();
	console.log("Key insights:");
	console.log("• both-unlimited: No concurrency slots - maximum throughput");
	console.log("• one-limited:    Task A uses slots, Task B doesn't");
	console.log("                  → Shows if slot mechanism adds overhead to unlimited tasks");
	console.log("• both-limited:   Both use slots");
	console.log();
	console.log("If 'one-limited' throughput ≈ 'both-unlimited', there's minimal overhead!");
	console.log();
}

main().catch((err) => {
	console.error("Benchmark failed:", err);
	process.exit(1);
});
