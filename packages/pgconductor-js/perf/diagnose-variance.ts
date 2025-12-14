#!/usr/bin/env bun
/**
 * Diagnose performance variance by running multiple iterations
 * and tracking different phases
 */

import { Conductor, Orchestrator } from "../src";
import { TaskSchemas } from "../src/schemas";
import { perfTaskDefinitions } from "./throughput/tasks";
import { getDatabase } from "./throughput/setup";

async function runIteration(iteration: number, dbUrl: string) {
	console.log(`\n=== Iteration ${iteration} ===`);

	// Clean database
	const postgres = await import("postgres");
	const sql = postgres.default(dbUrl);
	const cleanStart = Date.now();
	await sql.unsafe("DROP SCHEMA IF EXISTS pgconductor CASCADE");
	await sql.end();
	const cleanTime = Date.now() - cleanStart;
	console.log(`Clean: ${cleanTime}ms`);

	const conductor = Conductor.create({
		connectionString: dbUrl,
		tasks: TaskSchemas.fromSchema(perfTaskDefinitions),
		context: {},
	});

	const installStart = Date.now();
	await conductor.ensureInstalled();
	const installTime = Date.now() - installStart;
	console.log(`Install: ${installTime}ms`);

	// Create tasks
	const taskA = (conductor as any).createTask(
		{ name: "task-a", concurrency: 5 },
		{ invocable: true },
		async () => ({ processed: true }),
	);

	const taskB = (conductor as any).createTask(
		{ name: "task-b" },
		{ invocable: true },
		async () => ({ processed: true }),
	);

	// Queue tasks
	const taskCount = 1000; // Smaller for faster iteration
	const queueStart = Date.now();
	for (let i = 0; i < taskCount; i++) {
		const taskName = i % 2 === 0 ? "task-a" : "task-b";
		await conductor.invoke({ name: taskName }, { id: i });
	}
	const queueTime = Date.now() - queueStart;
	console.log(`Queue: ${queueTime}ms`);

	// Process
	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [taskA, taskB] as any,
		defaultWorker: {
			pollIntervalMs: 50,
			flushIntervalMs: 50,
			concurrency: 50,
			fetchBatchSize: 100,
		},
	});

	await orchestrator.start();
	const execStart = Date.now();

	// Poll for completion
	const postgres2 = await import("postgres");
	const sql2 = postgres2.default(dbUrl);

	let totalCompleted = 0;
	while (totalCompleted < taskCount) {
		await new Promise((resolve) => setTimeout(resolve, 100));
		const completed = await sql2<{ count: string }[]>`
			SELECT COUNT(*) as count
			FROM pgconductor._private_executions
			WHERE (task_key = 'task-a' OR task_key = 'task-b') AND completed_at IS NOT NULL
		`;
		totalCompleted = Number(completed[0]?.count || 0);
	}

	const execTime = Date.now() - execStart;
	await orchestrator.stop();
	await sql2.end();

	const throughput = (taskCount / execTime) * 1000;

	console.log(`Execute: ${execTime}ms`);
	console.log(`Throughput: ${throughput.toFixed(2)} t/s`);
	console.log(`Total: ${cleanTime + installTime + queueTime + execTime}ms`);

	return {
		iteration,
		cleanTime,
		installTime,
		queueTime,
		execTime,
		throughput,
	};
}

async function main() {
	const dbUrl = await getDatabase();
	const iterations = 10;
	const results = [];

	console.log("=".repeat(70));
	console.log(`Running ${iterations} iterations with outlier removal`);
	console.log("=".repeat(70));

	// Warmup run (not counted)
	console.log("\n🔥 Warmup run (not counted)...");
	await runIteration(0, dbUrl);

	console.log("\n📊 Collecting data...\n");
	for (let i = 1; i <= iterations; i++) {
		results.push(await runIteration(i, dbUrl));
	}

	// Analyze results
	console.log("\n");
	console.log("=".repeat(70));
	console.log("Results Summary");
	console.log("=".repeat(70));
	console.log();

	console.log("Iter │ Clean │ Install │ Queue  │ Execute │ Throughput │ Status");
	console.log("─".repeat(70));

	// Sort by throughput to identify outliers
	const sorted = [...results].sort((a, b) => a.throughput - b.throughput);
	const trimmed = sorted.slice(2, -2); // Remove top/bottom 2

	const outliers = [
		sorted[0],
		sorted[1],
		sorted[sorted.length - 1],
		sorted[sorted.length - 2],
	].filter((r): r is NonNullable<typeof r> => r !== undefined);

	const outlierIds = new Set(outliers.map((r) => r.iteration));

	for (const r of results) {
		const iter = r.iteration.toString().padStart(4);
		const clean = `${r.cleanTime}ms`.padStart(5);
		const install = `${r.installTime}ms`.padStart(7);
		const queue = `${r.queueTime}ms`.padStart(6);
		const exec = `${r.execTime}ms`.padStart(7);
		const throughput = `${r.throughput.toFixed(2)} t/s`.padStart(10);
		const status = outlierIds.has(r.iteration) ? " ❌ OUTLIER" : " ✓";
		console.log(`${iter} │ ${clean} │ ${install} │ ${queue} │ ${exec} │ ${throughput} │${status}`);
	}

	// Statistics
	const throughputs = results.map((r) => r.throughput);
	const min = Math.min(...throughputs);
	const max = Math.max(...throughputs);
	const avg = throughputs.reduce((a, b) => a + b, 0) / throughputs.length;

	const trimmedThroughputs = trimmed.map((r) => r.throughput);
	const median = trimmedThroughputs[Math.floor(trimmedThroughputs.length / 2)];
	if (!median) {
		throw new Error("No median value found - insufficient data");
	}
	const trimmedMin = Math.min(...trimmedThroughputs);
	const trimmedMax = Math.max(...trimmedThroughputs);
	const trimmedAvg = trimmedThroughputs.reduce((a, b) => a + b, 0) / trimmedThroughputs.length;

	console.log();
	console.log("📈 Raw Statistics:");
	console.log(`   Min: ${min.toFixed(2)} t/s`);
	console.log(`   Max: ${max.toFixed(2)} t/s`);
	console.log(`   Avg: ${avg.toFixed(2)} t/s`);
	console.log(`   Variance: ${(((max - min) / avg) * 100).toFixed(1)}%`);
	console.log();
	console.log("📊 Trimmed Statistics (outliers removed):");
	console.log(`   Min: ${trimmedMin.toFixed(2)} t/s`);
	console.log(`   Max: ${trimmedMax.toFixed(2)} t/s`);
	console.log(`   Avg: ${trimmedAvg.toFixed(2)} t/s`);
	console.log(`   Median: ${median.toFixed(2)} t/s ⭐`);
	console.log(`   Variance: ${(((trimmedMax - trimmedMin) / trimmedAvg) * 100).toFixed(1)}%`);
	console.log();

	// Time breakdown (using trimmed data)
	const avgClean = trimmed.reduce((a, r) => a + r.cleanTime, 0) / trimmed.length;
	const avgInstall = trimmed.reduce((a, r) => a + r.installTime, 0) / trimmed.length;
	const avgQueue = trimmed.reduce((a, r) => a + r.queueTime, 0) / trimmed.length;
	const avgExec = trimmed.reduce((a, r) => a + r.execTime, 0) / trimmed.length;

	const total = avgClean + avgInstall + avgQueue + avgExec;
	console.log("⏱️  Time breakdown (median run):");
	console.log(`   Clean:   ${avgClean.toFixed(0)}ms (${((avgClean / total) * 100).toFixed(1)}%)`);
	console.log(
		`   Install: ${avgInstall.toFixed(0)}ms (${((avgInstall / total) * 100).toFixed(1)}%)`,
	);
	console.log(`   Queue:   ${avgQueue.toFixed(0)}ms (${((avgQueue / total) * 100).toFixed(1)}%)`);
	console.log(`   Execute: ${avgExec.toFixed(0)}ms (${((avgExec / total) * 100).toFixed(1)}%)`);
	console.log();
	console.log("=".repeat(70));
	console.log(`🎯 RESULT: ${median.toFixed(2)} tasks/sec (median, outliers removed)`);
	console.log("=".repeat(70));

	process.exit(0);
}

main().catch((err) => {
	console.error("Failed:", err);
	process.exit(1);
});
