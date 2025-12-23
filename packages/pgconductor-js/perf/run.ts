import { BenchmarkDatabase } from "./fixtures/benchmark-database";
import { createBenchmarks } from "./benchmarks";
import { runScenario } from "./lib/benchmark-runner";
import { Reporter } from "./lib/reporter";

async function main() {
	const filter = process.argv[2];
	const jsonOutput = process.argv.includes("--json");

	console.log("Setting up benchmark environment...\n");
	const ctx = await BenchmarkDatabase.create();

	try {
		const benchmarks = createBenchmarks(ctx);
		const results = [];

		for (const [name, scenario] of Object.entries(benchmarks)) {
			// Filter scenarios if pattern provided
			if (filter && !name.startsWith(filter.replace("/*", "").replace("*", ""))) {
				continue;
			}

			if (!jsonOutput) {
				console.log(`Running ${name}...`);
			}

			const result = await runScenario(name, scenario, ctx);
			results.push(result);

			if (!jsonOutput) {
				console.log(`  ✓ Completed in ${result.durationMs.toFixed(2)}ms\n`);
			}
		}

		if (jsonOutput) {
			console.log(Reporter.json(results));
		} else {
			Reporter.console(results);
		}
	} finally {
		await BenchmarkDatabase.cleanup(ctx);
	}
}

main().catch((error) => {
	console.error("Benchmark failed:", error);
	process.exit(1);
});
