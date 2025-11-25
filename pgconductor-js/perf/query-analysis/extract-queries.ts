import { writeFileSync } from "node:fs";
import { QueryBuilder } from "../../src/query-builder";
import postgres from "postgres";

// Create a dummy sql instance just for query building
const sql = postgres({ max: 0 });
const qb = new QueryBuilder(sql);

// 1. heavy-single-queue batch-10-filtered
const orchestratorId = crypto.randomUUID();
const filterKeys = ["task-1", "task-2"];
const query1 = qb.buildGetExecutions({
	orchestratorId,
	queueName: "default",
	batchSize: 10,
	filterTaskKeys: filterKeys,
});
writeFileSync("perf/to-analyse/heavy-single-queue-batch-10-filtered-query.sql",
	`-- heavy-single-queue: batch-10-filtered (122ms execution)
-- Scenario: 1 queue, 1 task, 1M pending
-- Issue: Task filter not using index efficiently

EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)
WITH e AS (
    SELECT
        e.id,
        e.task_key
    FROM pgconductor.executions e
    WHERE e.queue = 'default'::text
        AND e.task_key != ANY(ARRAY['task-1', 'task-2']::text[])
        AND e.run_at <= pgconductor.current_time()
        AND e.is_available = true
    ORDER BY e.priority ASC, e.run_at ASC
    LIMIT 10
    FOR UPDATE SKIP LOCKED
)
UPDATE pgconductor.executions
SET
    attempts = executions.attempts + 1,
    locked_by = '${orchestratorId}'::uuid,
    locked_at = pgconductor.current_time()
FROM e
WHERE executions.id = e.id
    AND executions.queue = 'default'::text
RETURNING
    executions.id,
    executions.task_key,
    executions.queue,
    executions.payload,
    executions.waiting_on_execution_id,
    executions.dedupe_key,
    executions.cron_expression;
`);

// 2. many-queues-sparse mixed-10
// This needs 500 tasks worth of config
const taskMaxAttempts: Record<string, number> = {};
const taskRemoveOnComplete: Record<string, boolean> = {};
const taskRemoveOnFail: Record<string, boolean> = {};
for (let i = 0; i < 500; i++) {
	const key = `task-${i}`;
	taskMaxAttempts[key] = 3;
	taskRemoveOnComplete[key] = false;
	taskRemoveOnFail[key] = false;
}

writeFileSync("perf/to-analyse/many-queues-sparse-mixed-10-query.sql",
	`-- many-queues-sparse: mixed-10 (31ms planning, 14ms execution)
-- Scenario: 100 queues, 5 tasks each = 500 tasks total
-- Issue: Planning time too high with many task configs

-- Query uses CTEs for task config lookups instead of CASE statements
-- with 500 tasks. The planning overhead comes from:
-- 1. jsonb_to_recordset for max_attempts lookup (500 entries)
-- 2. unnest arrays for removeOnComplete/removeOnFail task keys
-- 3. Multiple CTEs for different result types (completed, failed, released)

-- The actual query would be very long with 500 task configs embedded.
-- See the plan JSON for the full execution details.
`);

// 3. with-steps 100-completed-delete
writeFileSync("perf/to-analyse/with-steps-100-completed-delete-query.sql",
	`-- with-steps: 100-completed-delete (468ms execution)
-- Scenario: 10K pending with 10 steps each
-- Issue: Cascade delete of steps is extremely slow

-- When deleting completed executions, the steps table also needs
-- to be cleaned up. With 100 executions * 10 steps = 1000 step deletions,
-- this becomes a bottleneck.

-- The query uses:
-- DELETE FROM pgconductor.executions e
-- USING completed_delete_results r
-- WHERE e.id = r.execution_id

-- The steps table likely has a foreign key or trigger that cascades
-- the deletion, causing the slow performance.

-- Potential optimizations:
-- 1. Batch step deletions before execution deletions
-- 2. Use deferred foreign key constraints
-- 3. Add index on steps(execution_id)
`);

console.log("Query files created in perf/to-analyse/");
sql.end();
