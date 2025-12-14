#!/usr/bin/env bun
/**
 * Benchmark script to test task-level concurrency impact
 */
import { spawn } from "child_process";
import { join } from "path";

const scenarios = [
	"task-concurrency-unlimited",
	"task-concurrency-1",
	"task-concurrency-5",
	"task-concurrency-10",
	"task-concurrency-25",
	"task-concurrency-50",
];

async function runScenario(scenarioName: string): Promise<{
	scenario: string;
	throughput: number;
	duration: number;
	error?: string;
}> {
	return new Promise((resolve) => {
		console.log(`\n📊 Running scenario: ${scenarioName}\n`);

		const child = spawn("bun", [join(__dirname, "run.ts"), scenarioName], {
			env: process.env,
			stdio: ["ignore", "pipe", "pipe"],
		});

		let stdout = "";
		let stderr = "";

		child.stdout.on("data", (data) => {
			const output = data.toString();
			stdout += output;
			process.stdout.write(output);
		});

		child.stderr.on("data", (data) => {
			stderr += data.toString();
		});

		child.on("close", (code) => {
			// Parse results
			const overallMatch = stdout.match(/Overall:\s+([\d.]+)\s+tasks\/sec/);
			const durationMatch = stdout.match(/Total time:\s+(\d+)ms/);

			if (code !== 0 || !overallMatch || !durationMatch) {
				resolve({
					scenario: scenarioName,
					throughput: 0,
					duration: 0,
					error: stderr || "Failed to parse results",
				});
			} else {
				resolve({
					scenario: scenarioName,
					throughput: Number(overallMatch[1]),
					duration: Number(durationMatch[1]),
				});
			}
		});
	});
}

async function main() {
	console.log("=".repeat(60));
	console.log("Task-Level Concurrency Impact Benchmark");
	console.log("=".repeat(60));

	const results = [];

	for (const scenario of scenarios) {
		const result = await runScenario(scenario);
		results.push(result);
	}

	// Print summary table
	console.log("\n");
	console.log("=".repeat(60));
	console.log("Summary");
	console.log("=".repeat(60));
	console.log();

	// Find the concurrency value from scenario name
	const getConcurrency = (name: string) => {
		if (name.includes("unlimited")) return "∞";
		const match = name.match(/concurrency-(\d+)/);
		return match && match[1] ? match[1] : "?";
	};

	// Calculate speedup relative to concurrency=1
	const baseline = results.find((r) => r.scenario.includes("concurrency-1"));
	const baselineThroughput = baseline?.throughput || 1;

	console.log("Task Concurrency │ Throughput (tasks/sec) │ Duration (ms) │ Speedup");
	console.log("─".repeat(70));

	for (const result of results) {
		if (result.error) {
			console.log(`${getConcurrency(result.scenario).padEnd(16)} │ ERROR: ${result.error}`);
		} else {
			const concurrency = getConcurrency(result.scenario).padEnd(16);
			const throughput = result.throughput.toFixed(2).padStart(22);
			const duration = result.duration.toString().padStart(13);
			const speedup = `${(result.throughput / baselineThroughput).toFixed(2)}x`.padStart(7);

			console.log(`${concurrency} │ ${throughput} │ ${duration} │ ${speedup}`);
		}
	}

	console.log();
	console.log("=".repeat(60));
	console.log();
	console.log("Key insights:");
	console.log("- Lower concurrency = more slots locked, lower parallelism");
	console.log("- Higher concurrency = more parallel execution");
	console.log("- Unlimited = no task-level limit (only worker concurrency)");
	console.log();
}

main().catch((err) => {
	console.error("Benchmark failed:", err);
	process.exit(1);
});
