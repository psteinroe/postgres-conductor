import { writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { TestDatabasePool } from "../../tests/fixtures/test-database";
import type { TestDatabase } from "../../tests/fixtures/test-database";
import { scenarios, type Scenario } from "./scenarios";
import { Fixtures } from "./lib/fixtures";
import { QueryBuilder } from "../../src/query-builder";
import type { ExecutionResult, GroupedExecutionResults } from "../../src/database-client";

// Query definition with variations
type QueryVariation = {
	name: string;
	description: string;
	setup: (db: TestDatabase, scenario: Scenario) => Promise<void>;
	execute: (qb: QueryBuilder, scenario: Scenario, db: TestDatabase) => any;
};

type QueryDefinition = {
	name: string;
	description: string;
	variations: QueryVariation[];
};

// Generate execution results for returnExecutions testing
// This version fetches real execution IDs from the database
async function generateResultsFromDb(
	db: TestDatabase,
	scenario: Scenario,
	config: {
		completed?: number;
		failed?: number;
		released?: number;
	},
): Promise<ExecutionResult[]> {
	const results: ExecutionResult[] = [];

	const totalNeeded = (config.completed || 0) + (config.failed || 0) + (config.released || 0);

	// Fetch real execution IDs from the database
	const executions = await db.sql<{ id: string; task_key: string; queue: string }[]>`
		select id, task_key, queue from pgconductor.executions
		where completed_at is null and failed_at is null
		limit ${totalNeeded}
	`;

	let idx = 0;

	// Generate completed results with real IDs
	for (let i = 0; i < (config.completed || 0) && idx < executions.length; i++) {
		results.push({
			execution_id: executions[idx]!.id,
			task_key: executions[idx]!.task_key,
			queue: executions[idx]!.queue,
			status: "completed",
			result: { value: i },
		});
		idx++;
	}

	// Generate failed results with real IDs
	for (let i = 0; i < (config.failed || 0) && idx < executions.length; i++) {
		results.push({
			execution_id: executions[idx]!.id,
			task_key: executions[idx]!.task_key,
			queue: executions[idx]!.queue,
			status: "failed",
			error: `Error ${i}`,
		});
		idx++;
	}

	// Generate released results with real IDs
	for (let i = 0; i < (config.released || 0) && idx < executions.length; i++) {
		results.push({
			execution_id: executions[idx]!.id,
			task_key: executions[idx]!.task_key,
			queue: executions[idx]!.queue,
			status: "released",
		});
		idx++;
	}

	return results;
}

function groupResults(results: ExecutionResult[]): GroupedExecutionResults {
	const grouped: GroupedExecutionResults = {
		completed: [],
		failed: [],
		released: [],
		invokeChild: [],
		waitForCustomEvent: [],
		waitForDbEvent: [],
		taskKeys: new Set<string>(),
	};

	for (const result of results) {
		grouped.taskKeys.add(result.task_key);
		if (result.status === "completed") {
			grouped.completed.push(result);
		} else if (result.status === "failed" || result.status === "permanently_failed") {
			grouped.failed.push(result);
		} else if (result.status === "released") {
			grouped.released.push(result);
		}
	}

	return grouped;
}

// Query definitions
const queries: QueryDefinition[] = [
	{
		name: "get_executions",
		description: "Fetch and claim ready executions",
		variations: [
			{
				name: "batch-10",
				description: "Batch size 10",
				setup: async () => {},
				execute: (qb, scenario) => {
					const orchestratorId = crypto.randomUUID();
					return qb.buildGetExecutions({
						orchestratorId,
						queueName: scenario.queues[0] || "default",
						batchSize: 10,
						filterTaskKeys: [],
					});
				},
			},
			{
				name: "batch-100",
				description: "Batch size 100",
				setup: async () => {},
				execute: (qb, scenario) => {
					const orchestratorId = crypto.randomUUID();
					return qb.buildGetExecutions({
						orchestratorId,
						queueName: scenario.queues[0] || "default",
						batchSize: 100,
						filterTaskKeys: [],
					});
				},
			},
			{
				name: "batch-10-filtered",
				description: "Batch size 10 with task filter",
				setup: async () => {},
				execute: (qb, scenario) => {
					const orchestratorId = crypto.randomUUID();
					const filterKeys = scenario.tasks.slice(0, 2).map((t) => t.key);
					return qb.buildGetExecutions({
						orchestratorId,
						queueName: scenario.queues[0] || "default",
						batchSize: 10,
						filterTaskKeys: filterKeys,
					});
				},
			},
		],
	},
	{
		name: "return_executions",
		description: "Return execution results",
		variations: [
			// Small batches (10)
			{
				name: "10-completed-keep",
				description: "10 completed, keep in DB",
				setup: async () => {},
				execute: async (qb, scenario, db) => {
					const results = await generateResultsFromDb(db, scenario, { completed: 10 });
					return qb.buildReturnExecutions(groupResults(results));
				},
			},
			{
				name: "10-completed-delete",
				description: "10 completed, delete from DB",
				setup: async () => {},
				execute: async (qb, scenario, db) => {
					const results = await generateResultsFromDb(db, scenario, { completed: 10 });
					return qb.buildReturnExecutions(groupResults(results));
				},
			},
			{
				name: "10-failed-retry",
				description: "10 failed, will retry",
				setup: async () => {},
				execute: async (qb, scenario, db) => {
					const results = await generateResultsFromDb(db, scenario, { failed: 10 });
					return qb.buildReturnExecutions(groupResults(results));
				},
			},
			{
				name: "10-released",
				description: "10 released",
				setup: async () => {},
				execute: async (qb, scenario, db) => {
					const results = await generateResultsFromDb(db, scenario, { released: 10 });
					return qb.buildReturnExecutions(groupResults(results));
				},
			},
			{
				name: "mixed-10",
				description: "5 completed, 3 failed, 2 released",
				setup: async () => {},
				execute: async (qb, scenario, db) => {
					const results = await generateResultsFromDb(db, scenario, {
						completed: 5,
						failed: 3,
						released: 2,
					});
					return qb.buildReturnExecutions(groupResults(results));
				},
			},
			// Medium batches (100)
			{
				name: "100-completed-keep",
				description: "100 completed, keep in DB",
				setup: async () => {},
				execute: async (qb, scenario, db) => {
					const results = await generateResultsFromDb(db, scenario, { completed: 100 });
					return qb.buildReturnExecutions(groupResults(results));
				},
			},
			{
				name: "100-completed-delete",
				description: "100 completed, delete from DB",
				setup: async () => {},
				execute: async (qb, scenario, db) => {
					const results = await generateResultsFromDb(db, scenario, { completed: 100 });
					return qb.buildReturnExecutions(groupResults(results));
				},
			},
			{
				name: "mixed-100",
				description: "50 completed, 30 failed, 20 released",
				setup: async () => {},
				execute: async (qb, scenario, db) => {
					const results = await generateResultsFromDb(db, scenario, {
						completed: 50,
						failed: 30,
						released: 20,
					});
					return qb.buildReturnExecutions(groupResults(results));
				},
			},
		],
	},
];

// Extract key metrics from EXPLAIN ANALYZE plan
function extractMetrics(plan: any): {
	planningTime: number;
	executionTime: number;
	sharedHit: number;
	sharedRead: number;
} {
	return {
		planningTime: plan["Planning Time"] || 0,
		executionTime: plan["Execution Time"] || 0,
		sharedHit: plan["Planning"]?.["Shared Hit Blocks"] || 0,
		sharedRead: plan["Planning"]?.["Shared Read Blocks"] || 0,
	};
}

async function runComparison(scenarioName: string, queryNames: string[], outputDir?: string) {
	const scenario = scenarios[scenarioName];
	if (!scenario) {
		console.error(`Unknown scenario: ${scenarioName}`);
		console.log("Available scenarios:", Object.keys(scenarios).join(", "));
		process.exit(1);
	}

	console.log(`\nScenario: ${scenarioName}`);
	console.log(`Description: ${scenario.description}`);
	console.log("=".repeat(60));

	const pool = await TestDatabasePool.create();

	try {
		const db = await pool.child();
		const fixtures = new Fixtures(db);
		await fixtures.install();
		await fixtures.createTasks(scenario.tasks);

		// Setup executions
		if (scenario.executions.pending) {
			await fixtures.createPendingExecutions(scenario.executions.pending);
		}
		if (scenario.executions.completed) {
			await fixtures.createCompletedExecutions(scenario.executions.completed);
		}
		if (scenario.executions.failed) {
			await fixtures.createFailedExecutions(scenario.executions.failed);
		}

		// Update statistics for better query planning
		await db.sql`ANALYZE pgconductor.executions`;

		const qb = new QueryBuilder(db.sql);
		const results: Record<string, any> = {
			scenario: scenarioName,
			description: scenario.description,
			timestamp: new Date().toISOString(),
			queries: {},
		};

		// Filter queries if specified
		const queriesToRun =
			queryNames.length > 0 ? queries.filter((q) => queryNames.includes(q.name)) : queries;

		for (const queryDef of queriesToRun) {
			console.log(`\nQuery: ${queryDef.name}`);
			console.log(`  ${queryDef.description}`);
			results.queries[queryDef.name] = { variations: {} };

			for (const variation of queryDef.variations) {
				await variation.setup(db, scenario);
				const query = await variation.execute(qb, scenario, db);

				if (!query) {
					console.log(`  ${variation.name}: (no query generated)`);
					continue;
				}

				// Run EXPLAIN ANALYZE (query already includes EXPLAIN)
				const explainResult = await query;
				const plan = (explainResult[0] as any)["QUERY PLAN"][0];
				const metrics = extractMetrics(plan);

				console.log(`  ${variation.name}:`);
				console.log(`    Planning: ${metrics.planningTime.toFixed(2)}ms`);
				console.log(`    Execution: ${metrics.executionTime.toFixed(2)}ms`);

				results.queries[queryDef.name].variations[variation.name] = {
					description: variation.description,
					metrics,
					plan,
				};
			}
		}

		// Save results if output dir specified
		if (outputDir) {
			mkdirSync(outputDir, { recursive: true });
			const filename = `${scenarioName}-${Date.now()}.json`;
			writeFileSync(join(outputDir, filename), JSON.stringify(results, null, 2));
			console.log(`\nResults saved to: ${join(outputDir, filename)}`);
		}

		await db.destroy();
	} finally {
		await pool.destroy();
	}
}

// CLI
async function main() {
	const args = process.argv.slice(2);

	if (args.length === 0 || args.includes("--help")) {
		console.log(`
Usage: bun perf/compare-plans.ts <scenario> [options]

Arguments:
  scenario          Scenario name (or --all for all scenarios)

Options:
  --query <name>    Run specific query (can be repeated)
  --output <dir>    Save results to directory
  --list            List available scenarios and queries
  --help            Show this help

Examples:
  bun perf/compare-plans.ts baseline
  bun perf/compare-plans.ts baseline --query return_executions --query return_executions_optimized
  bun perf/compare-plans.ts --all --output results/
  bun perf/compare-plans.ts --list
`);
		process.exit(0);
	}

	if (args.includes("--list")) {
		console.log("Scenarios:");
		for (const [name, scenario] of Object.entries(scenarios)) {
			console.log(`  ${name}: ${scenario.description}`);
		}
		console.log("\nQueries:");
		for (const query of queries) {
			console.log(`  ${query.name}: ${query.description}`);
			for (const v of query.variations) {
				console.log(`    - ${v.name}: ${v.description}`);
			}
		}
		process.exit(0);
	}

	// Parse args
	const queryNames: string[] = [];
	let outputDir: string | undefined;
	const scenarioNames: string[] = [];

	for (let i = 0; i < args.length; i++) {
		const arg = args[i]!;
		if (arg === "--query" && args[i + 1]) {
			queryNames.push(args[++i]!);
		} else if (arg === "--output" && args[i + 1]) {
			outputDir = args[++i]!;
		} else if (arg === "--all") {
			scenarioNames.push(...Object.keys(scenarios));
		} else if (!arg.startsWith("--")) {
			scenarioNames.push(arg);
		}
	}

	if (scenarioNames.length === 0) {
		console.error("No scenario specified");
		process.exit(1);
	}

	for (const scenarioName of scenarioNames) {
		await runComparison(scenarioName, queryNames, outputDir);
	}
}

main().catch(console.error);
