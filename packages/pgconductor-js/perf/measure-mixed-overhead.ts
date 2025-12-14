#!/usr/bin/env bun
/**
 * Simple in-process benchmark using drain() to measure overhead
 * of mixing tasks with and without concurrency limits.
 */

import { Conductor, Orchestrator } from "../src";
import { TaskSchemas } from "../src/schemas";
import { defineTask } from "../src/task-definition";
import { z } from "zod";
import { getDatabase } from "./throughput/setup";

const taskDefs = [
	defineTask({ name: "task-a", payload: z.object({ id: z.number() }) }),
	defineTask({ name: "task-b", payload: z.object({ id: z.number() }) }),
];

interface ScenarioConfig {
	name: string;
	description: string;
	taskAConcurrency?: number;
	taskBConcurrency?: number;
}

const scenarios: ScenarioConfig[] = [
	{
		name: "both-unlimited",
		description: "Both tasks unlimited (no concurrency overhead)",
	},
	{
		name: "one-limited",
		description: "Task A=1, Task B=unlimited (mixed)",
		taskAConcurrency: 1,
	},
	{
		name: "both-limited",
		description: "Task A=1, Task B=10 (both limited)",
		taskAConcurrency: 1,
		taskBConcurrency: 10,
	},
];

async function runScenario(dbUrl: string, scenario: ScenarioConfig) {
	console.log(`\nRunning: ${scenario.name}`);
	console.log(`  ${scenario.description}`);

	// Recreate schema using raw postgres connection
	const { default: postgres } = await import("postgres");
	const sql = postgres(dbUrl);
	await sql.unsafe(`DROP SCHEMA IF EXISTS pgconductor CASCADE`);
	await sql.end();

	// Create conductor
	const conductor = Conductor.create({
		connectionString: dbUrl,
		tasks: TaskSchemas.fromSchema(taskDefs),
		context: {},
	});

	await conductor.ensureInstalled();

	// Create tasks
	const taskA = conductor.createTask(
		{
			name: "task-a",
			...(scenario.taskAConcurrency && { concurrency: scenario.taskAConcurrency }),
		},
		{ invocable: true },
		async () => {},
	);

	const taskB = conductor.createTask(
		{
			name: "task-b",
			...(scenario.taskBConcurrency && { concurrency: scenario.taskBConcurrency }),
		},
		{ invocable: true },
		async () => {},
	);

	// Queue 10k tasks (50/50 split)
	const taskCount = 10000;
	console.log(`  Queuing ${taskCount} tasks...`);
	const queueStart = Date.now();

	const promises = [];
	for (let i = 0; i < taskCount; i++) {
		const taskName = i % 2 === 0 ? "task-a" : "task-b";
		promises.push(conductor.invoke({ name: taskName }, { id: i }));
	}
	await Promise.all(promises);
	const queueTime = Date.now() - queueStart;
	console.log(`  ✓ Queued in ${queueTime}ms`);

	// Create orchestrator with 4 workers
	const orchestrator = Orchestrator.create({
		conductor,
		tasks: [taskA, taskB],
		defaultWorker: {
			pollIntervalMs: 50,
			flushIntervalMs: 50,
			concurrency: 50,
			fetchBatchSize: 100,
		},
	});

	// Measure execution
	console.log(`  Processing...`);
	void orchestrator.drain();
	await orchestrator.started;
	const execStart = Date.now();
	await orchestrator.stopped;
	const execTime = Date.now() - execStart;

	await conductor.db.close();

	const throughput = (taskCount / execTime) * 1000;

	console.log(`  ✓ Completed in ${execTime}ms`);
	console.log(`  ✓ Throughput: ${throughput.toFixed(2)} tasks/sec`);

	return {
		scenario: scenario.name,
		execTime,
		throughput,
	};
}

async function main() {
	const dbUrl = await getDatabase();

	console.log("=".repeat(70));
	console.log("Mixed Concurrency Overhead Benchmark");
	console.log("=".repeat(70));
	console.log();
	console.log("Goal: Measure if having ONE task with concurrency limit");
	console.log("      slows down OTHER tasks without limits");
	console.log();
	console.log("Setup: 4 workers, 10k noop tasks (5k task-a + 5k task-b)");
	console.log();

	const results = [];

	for (const scenario of scenarios) {
		const result = await runScenario(dbUrl, scenario);
		results.push(result);
	}

	// Print comparison
	console.log("\n");
	console.log("=".repeat(70));
	console.log("Results");
	console.log("=".repeat(70));
	console.log();

	const baseline = results[0];
	if (!baseline) {
		throw new Error("No baseline result found");
	}

	console.log("Scenario          │  Throughput (t/s)  │  Time (ms)  │  Overhead");
	console.log("─".repeat(70));

	for (const result of results) {
		const scenario = result.scenario.padEnd(17);
		const throughput = result.throughput.toFixed(2).padStart(18);
		const time = result.execTime.toString().padStart(11);
		const overhead =
			`${((1 - result.throughput / baseline.throughput) * 100).toFixed(1)}%`.padStart(9);

		console.log(`${scenario} │  ${throughput}  │  ${time}  │  ${overhead}`);
	}

	console.log();
	console.log("=".repeat(70));
	console.log();
	console.log("✓ If overhead for 'one-limited' is ~0%, slot mechanism is efficient!");
	console.log("✓ Task B (unlimited) should not be slowed by Task A's concurrency limit");
	console.log();
}

main().catch((err) => {
	console.error("Error:", err);
	process.exit(1);
});
