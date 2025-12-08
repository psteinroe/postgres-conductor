#!/usr/bin/env bun
/**
 * Quick comparison of task-level concurrency impact
 *
 * Usage: bun perf/compare-task-concurrency.ts
 */

import { execSync } from "child_process";

const scenarios = [
	{ name: "task-concurrency-1", label: "Concurrency: 1" },
	{ name: "task-concurrency-5", label: "Concurrency: 5" },
	{ name: "task-concurrency-10", label: "Concurrency: 10" },
	{ name: "task-concurrency-25", label: "Concurrency: 25" },
	{ name: "task-concurrency-unlimited", label: "Concurrency: ∞" },
];

interface Result {
	label: string;
	throughput: number;
	duration: number;
}

const results: Result[] = [];

console.log("=".repeat(70));
console.log(" Task-Level Concurrency Impact on Performance");
console.log("=".repeat(70));
console.log();
console.log("Setup: 4 workers, 10k IO tasks (10ms delay), worker concurrency=50");
console.log();

for (const scenario of scenarios) {
	console.log(`Running: ${scenario.label}...`);

	try {
		const output = execSync(`bun perf/throughput/run.ts ${scenario.name}`, {
			encoding: "utf-8",
			stdio: ["inherit", "pipe", "pipe"],
		});

		// Parse throughput and duration
		const throughputMatch = output.match(/Overall:\s+([\d.]+)\s+tasks\/sec/);
		const durationMatch = output.match(/Total time:\s+(\d+)ms/);

		if (throughputMatch && durationMatch) {
			results.push({
				label: scenario.label,
				throughput: Number(throughputMatch[1]),
				duration: Number(durationMatch[1]),
			});
			console.log(`  ✓ ${throughputMatch[1]} tasks/sec (${durationMatch[1]}ms)\n`);
		} else {
			console.log(`  ✗ Failed to parse results\n`);
		}
	} catch (err) {
		console.log(`  ✗ Failed to run scenario\n`);
	}
}

// Print comparison table
console.log();
console.log("=".repeat(70));
console.log(" Results Summary");
console.log("=".repeat(70));
console.log();

const baseline = results[0]; // concurrency=1

console.log("Task Concurrency  │  Throughput (tasks/s)  │  Duration (ms)  │  Speedup");
console.log("─".repeat(70));

for (const result of results) {
	const concurrency = result.label.split(": ")[1].padEnd(16);
	const throughput = result.throughput.toFixed(2).padStart(22);
	const duration = result.duration.toString().padStart(15);
	const speedup = `${(result.throughput / baseline.throughput).toFixed(2)}x`.padStart(8);

	console.log(`${concurrency}  │${throughput}  │${duration}  │${speedup}`);
}

console.log();
console.log("=".repeat(70));
console.log();
console.log("Key findings:");
console.log("• concurrency=1:  Serial execution, only 1 task runs at a time (baseline)");
console.log("• concurrency=5:  Up to 5 concurrent tasks across all workers");
console.log("• concurrency=10: Moderate parallelism");
console.log("• concurrency=25: High parallelism");
console.log("• concurrency=∞:  No task-level limit (limited only by worker concurrency)");
console.log();
console.log("Higher concurrency → More parallel execution → Higher throughput");
console.log("Lower concurrency → Good for rate limiting, resource protection");
console.log();
