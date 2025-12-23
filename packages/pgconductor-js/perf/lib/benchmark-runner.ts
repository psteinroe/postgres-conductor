import type { Orchestrator } from "../../src/orchestrator";
import type { BenchmarkContext } from "../fixtures/benchmark-database";

export type BenchmarkScenario = {
	orchestrator: Orchestrator;
	invoke: () => Promise<void>;
};

export type BenchmarkResult = {
	name: string;
	durationMs: number;
	tasksProcessed?: number;
};

export async function runScenario(
	name: string,
	scenario: BenchmarkScenario,
	ctx: BenchmarkContext,
): Promise<BenchmarkResult> {
	const { orchestrator, invoke } = scenario;

	// 1. Ensure schema installed (not measured)
	await ctx.conductor.ensureInstalled();

	// 2. Start and stop orchestrator to register tasks and create partitions (not measured)
	await orchestrator.start();
	await orchestrator.stop();

	// 3. Invoke all tasks (not measured)
	await invoke();

	// 4. Measure drain time only
	const start = performance.now();
	await orchestrator.drain();
	const end = performance.now();

	return {
		name,
		durationMs: end - start,
	};
}
