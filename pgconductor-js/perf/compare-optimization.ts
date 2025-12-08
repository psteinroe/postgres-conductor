#!/usr/bin/env bun
/**
 * Compare query optimization impact using robust benchmarking
 */

import { spawn } from "child_process";
import { readFileSync, writeFileSync } from "fs";
import { join } from "path";

const QUERY_BUILDER_PATH = join(__dirname, "../src/query-builder.ts");

// Backup original file
const originalContent = readFileSync(QUERY_BUILDER_PATH, "utf-8");

// Baseline query (before optimization) - merged locked_execs with ROW_NUMBER on all
const baselineSlowPath = `		// slow path: some tasks have concurrency limits
		return this.sql<Execution[]>\`
			with
				-- lock up to batchSize slots per concurrency task
				locked_slots_raw as (
					select t.task_key, ls.slot_group_number
					from unnest(\${this.sql.array(taskKeysWithConcurrency)}::text[]) as t(task_key)
					cross join lateral (
						select s.slot_group_number
						from pgconductor._private_concurrency_slots s
						where s.task_key = t.task_key
							and s.used = 0
						order by s.slot_group_number
						limit \${batchSize}::integer
						for update skip locked
					) as ls
				),

				-- count slots per task
				slots_per_task as (
					select task_key, count(*) as slot_count
					from locked_slots_raw
					group by task_key
				),

				-- lock all jobs (both concurrency and unlimited)
				locked_execs as (
					select
						t.task_key as match_task_key,
						le.*
					from slots_per_task t
					cross join lateral (
						select
							e.id,
							e.task_key,
							e.queue,
							e.payload,
							e.waiting_on_execution_id,
							e.waiting_step_key,
							e.cancelled,
							e.last_error,
							e.dedupe_key,
							e.cron_expression,
							e.priority,
							e.run_at
						from pgconductor._private_executions e
						where e.is_available = true
							and e.run_at <= pgconductor._private_current_time()
							and e.queue = \${queueName}::text
							and e.task_key = t.task_key
							\${filterTaskKeys.length ? this.sql\`and not (e.task_key = any(\${this.sql.array(filterTaskKeys)}::text[]))\` : this.sql\`\`}
						order by e.priority asc, e.run_at asc, e.id asc
						limit t.slot_count
						for update skip locked
					) as le
					union all
					select
						null as match_task_key,
						e.id,
						e.task_key,
						e.queue,
						e.payload,
						e.waiting_on_execution_id,
						e.waiting_step_key,
						e.cancelled,
						e.last_error,
						e.dedupe_key,
						e.cron_expression,
						e.priority,
						e.run_at
					from pgconductor._private_executions e
					where e.is_available = true
						and e.run_at <= pgconductor._private_current_time()
						and e.queue = \${queueName}::text
						and not (e.task_key = any(\${this.sql.array(taskKeysWithConcurrency)}::text[]))
						\${filterTaskKeys.length > 0 ? this.sql\`and not (e.task_key = any(\${this.sql.array(filterTaskKeys)}::text[]))\` : this.sql\`\`}
					order by e.priority asc, e.run_at asc, e.id asc
					limit \${batchSize}::integer
					for update skip locked
				),

				-- row number ALL executions (concurrency + unlimited)
				execs_rn as (
					select
						le.*,
						row_number() over (partition by le.task_key order by le.priority asc, le.run_at asc, le.id asc) as exec_rn
					from locked_execs le
				),

				-- row number slots by task
				slots_rn as (
					select
						ls.*,
						row_number() over (partition by ls.task_key order by ls.slot_group_number) as slot_rn
					from locked_slots_raw ls
				),

				-- pair executions with slots (or null for unlimited)
				paired as (
					select
						e.id,
						e.task_key,
						e.queue,
						e.payload,
						e.waiting_on_execution_id,
						e.waiting_step_key,
						e.cancelled,
						e.last_error,
						e.dedupe_key,
						e.cron_expression,
						s.slot_group_number
					from execs_rn e
					left join slots_rn s
						on e.match_task_key = s.task_key
						and e.exec_rn = s.slot_rn
				),

				-- mark slots as used
				mark_used as (
					update pgconductor._private_concurrency_slots cs
					set used = 1
					from paired p
					where cs.task_key = p.task_key
						and cs.slot_group_number = p.slot_group_number
						and p.slot_group_number is not null
				)

			-- update and return executions
			update pgconductor._private_executions e
			set
				attempts = e.attempts + 1,
				locked_by = \${orchestratorId}::uuid,
				locked_at = pgconductor._private_current_time()
			from paired p
			where e.id = p.id
			returning
				e.id,
				e.task_key,
				e.queue,
				e.payload,
				e.waiting_on_execution_id,
				e.waiting_step_key,
				e.cancelled,
				e.last_error,
				e.dedupe_key,
				e.cron_expression,
				p.slot_group_number
		\`;`;

function runBenchmark(): Promise<{ median: number; variance: number }> {
	return new Promise((resolve, reject) => {
		const child = spawn("bun", [join(__dirname, "diagnose-variance.ts")], {
			env: { ...process.env, AGENT: "1" },
			stdio: ["ignore", "pipe", "pipe"],
		});

		let stdout = "";
		let stderr = "";

		child.stdout.on("data", (data) => {
			stdout += data.toString();
			process.stdout.write(data);
		});

		child.stderr.on("data", (data) => {
			stderr += data.toString();
		});

		child.on("close", (code) => {
			if (code !== 0) {
				reject(new Error(`Benchmark failed: ${stderr}`));
				return;
			}

			// Extract median from output like "🎯 RESULT: 324.99 tasks/sec"
			const medianMatch = stdout.match(/🎯 RESULT: ([\d.]+) tasks/);
			// Extract variance from output like "Variance: 37.8%"
			const varianceMatch = stdout.match(/Trimmed.*?Variance: ([\d.]+)%/s);

			if (!medianMatch || !varianceMatch) {
				reject(new Error(`Could not parse results from output:\n${stdout.slice(-500)}`));
				return;
			}

			resolve({
				median: Number.parseFloat(medianMatch[1]),
				variance: Number.parseFloat(varianceMatch[1]),
			});
		});

		child.on("error", reject);
	});
}

async function main() {
	console.log("=".repeat(70));
	console.log("Query Optimization Comparison");
	console.log("=".repeat(70));
	console.log();
	console.log("Testing query performance before and after optimization");
	console.log("Each test: 10 iterations + warmup, outliers removed, median reported");
	console.log();

	try {
		// Test 1: Baseline (before optimization)
		console.log("\n📊 TEST 1: BASELINE (before optimization)");
		console.log("-".repeat(70));
		console.log("Query: Single locked_execs CTE with ROW_NUMBER on ALL executions");
		console.log();

		// Replace slow path with baseline
		const baselineContent = originalContent.replace(
			/\/\/ slow path: some tasks have concurrency limits\s+return this\.sql<Execution\[\]>`[\s\S]+?`;/,
			baselineSlowPath,
		);
		writeFileSync(QUERY_BUILDER_PATH, baselineContent);

		const baselineResult = await runBenchmark();

		// Test 2: Optimized (current)
		console.log("\n\n📊 TEST 2: OPTIMIZED (current)");
		console.log("-".repeat(70));
		console.log(
			"Query: Split into concurrency_execs_rn + unlimited_paired (no ROW_NUMBER on unlimited)",
		);
		console.log();

		// Restore original (optimized) version
		writeFileSync(QUERY_BUILDER_PATH, originalContent);

		const optimizedResult = await runBenchmark();

		// Print comparison
		console.log("\n\n");
		console.log("=".repeat(70));
		console.log("COMPARISON");
		console.log("=".repeat(70));
		console.log();

		const improvement =
			((optimizedResult.median - baselineResult.median) / baselineResult.median) * 100;
		const improvementSign = improvement >= 0 ? "+" : "";

		console.log(
			`Baseline:  ${baselineResult.median.toFixed(2)} tasks/sec (variance: ${baselineResult.variance.toFixed(1)}%)`,
		);
		console.log(
			`Optimized: ${optimizedResult.median.toFixed(2)} tasks/sec (variance: ${optimizedResult.variance.toFixed(1)}%)`,
		);
		console.log();
		console.log(
			`Improvement: ${improvementSign}${improvement.toFixed(1)}% ${improvement > 0 ? "🚀" : improvement < 0 ? "⚠️" : ""}`,
		);
		console.log();

		if (improvement > 5) {
			console.log("✅ Optimization is effective!");
		} else if (improvement < -5) {
			console.log("❌ Optimization made things worse!");
		} else {
			console.log("⚠️  No significant difference");
		}
		console.log();
		console.log("=".repeat(70));
	} finally {
		// Always restore original file
		writeFileSync(QUERY_BUILDER_PATH, originalContent);
	}
}

main().catch((err) => {
	// Restore file on error
	writeFileSync(QUERY_BUILDER_PATH, originalContent);
	console.error("Comparison failed:", err);
	process.exit(1);
});
