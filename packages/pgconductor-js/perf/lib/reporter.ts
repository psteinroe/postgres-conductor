import type { BenchmarkResult } from "./benchmark-runner";

export class Reporter {
	static console(results: BenchmarkResult[]): void {
		console.log("\n" + "=".repeat(70));
		console.log("Benchmark Results");
		console.log("=".repeat(70) + "\n");

		for (const result of results) {
			console.log(`${result.name}`);
			console.log(`  Duration: ${result.durationMs.toFixed(2)}ms`);
			if (result.tasksProcessed) {
				const opsPerSec = (result.tasksProcessed / result.durationMs) * 1000;
				console.log(`  Tasks: ${result.tasksProcessed}`);
				console.log(`  Throughput: ${opsPerSec.toFixed(0)} ops/sec`);
			}
			console.log("");
		}

		console.log("=".repeat(70) + "\n");
	}

	static json(results: BenchmarkResult[]): string {
		return JSON.stringify(
			{
				timestamp: new Date().toISOString(),
				results: results.map((r) => ({
					name: r.name,
					durationMs: r.durationMs,
					tasksProcessed: r.tasksProcessed,
				})),
			},
			null,
			2,
		);
	}
}
