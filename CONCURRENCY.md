# Task-Level Concurrency

Users can set a concurrency limit on a task level that works across all workers.

## Design: Slot Groups Pattern

### Schema

```sql
create table pgconductor._private_concurrency_slots (
  task_key text not null,
  slot_group_number int not null,
  capacity int not null,        -- how many slots this group represents
  used int default 0,            -- how many are currently claimed
  primary key (task_key, slot_group_number)
);
```

### Slot Group Allocation

To prevent creating hundreds/thousands of rows for high-concurrency tasks, we use **slot groups**:

- **MAX_SLOT_GROUPS = 50** (constant)
- Each group can hold multiple slots
- `slots_per_group = ceil(concurrency / 50)`
- `num_groups = ceil(concurrency / slots_per_group)`

**Examples:**
- Concurrency 4 → 4 groups, 1 slot each
- Concurrency 100 → 50 groups, 2 slots each
- Concurrency 500 → 50 groups, 10 slots each
- Concurrency 1000 → 50 groups, 20 slots each

### Query Pattern

**Performance optimization:** Use in-memory task configuration to dynamically build `get_executions()` query based on whether any tasks have concurrency limits.

#### Case 1: No tasks have concurrency limits (fastest path)

```sql
select e.*
from pgconductor._private_executions e
where e.is_available
  and e.queue = :queue
  and e.run_at <= pgconductor._private_current_time()
order by e.run_at
for update skip locked
limit :batch_size
```

Zero joins, zero overhead. Simple index scan.

#### Case 2: Some tasks have concurrency limits

```sql
with available_slots as (
  -- Only scan slots for tasks that actually have limits
  select cs.task_key, cs.slot_group_number,
    row_number() over (partition by cs.task_key order by cs.slot_group_number) as rn
  from pgconductor._private_concurrency_slots cs
  where cs.task_key = any(:task_keys_with_limits)
    and cs.used < cs.capacity
  for update skip locked
),
claimed_executions as (
  select e.*, slots.slot_group_number
  from pgconductor._private_executions e
  left join available_slots slots
    on slots.task_key = e.task_key and slots.rn = 1
  where e.is_available
    and e.queue = :queue
    and e.run_at <= pgconductor._private_current_time()
    -- Only exclude executions that NEED a slot but DIDN'T GET one
    and not (e.task_key = any(:task_keys_with_limits) and slots.task_key is null)
  order by e.run_at
  for update of e skip locked
  limit :batch_size
)
-- Claim the slot groups
update pgconductor._private_concurrency_slots cs
set used = used + 1
from claimed_executions ce
where cs.task_key = ce.task_key
  and cs.slot_group_number = ce.slot_group_number
  and ce.slot_group_number is not null
returning ce.*;
```

**Why this is optimal:**
- CTE only scans slots for tasks in `:task_keys_with_limits` array (e.g., 1 task out of 100)
- LEFT JOIN means tasks without limits pass through with NULL slot
- Global ordering by `run_at` ensures fair scheduling
- Single query, no N+1 issues

#### Releasing slots

On completion/failure, release slot:
```sql
update pgconductor._private_concurrency_slots
set used = used - 1
where task_key = :task_key
  and used > 0
limit 1;
```

#### Required Indexes

```sql
-- On executions
create index idx_executions_claim
  on pgconductor._private_executions (queue, is_available, run_at)
  where is_available = true;

-- On slots
create index idx_slots_claim
  on pgconductor._private_concurrency_slots (task_key, capacity, used);
```

### Performance Optimizations

- **Zero overhead for tasks without limits**: LEFT JOIN design means tasks without concurrency limits have zero penalty even when other tasks in the queue have limits
- **Dynamic query building**: Use in-memory task config to build different SQL queries—skip slot logic entirely when no tasks have concurrency limits
- **Minimal slots table access**: CTE only scans slots for tasks in `:task_keys_with_limits` array (e.g., 1 task out of 100)
- **Bounded table size**: Max 50 rows per task regardless of concurrency setting
- **Fine-grained locking**: `for update skip locked` allows parallel workers to claim different slot groups
- **Single query**: No N+1 issues, no complex merging logic in application code
- **Global ordering**: All executions sorted by `run_at` globally for fair scheduling

### Future: Group Concurrency

Slot groups pattern extends naturally to group concurrency by adding `group_key` column to `_private_concurrency_slots` table.

---

## Implementation Plan

### Current Status

**Existing:**
- `_private_tasks` table with: key, queue, max_attempts, window_start/end, remove_on_*_days
- `task_spec` SQL type matching table columns
- `TaskSpec` TypeScript interface in database-client.ts
- `QueryBuilder.buildGetExecutions()` - simple query, no slot logic
- `QueryBuilder.buildReturnExecutions()` - processes results, no slot release
- `QueryBuilder.buildRegisterWorker()` - upserts tasks, no slot creation

**Missing:**
- `concurrency_limit` column in `_private_tasks`
- `concurrency_limit` field in `task_spec` SQL type
- `concurrency` field in `TaskSpec` TypeScript interface
- `_private_concurrency_slots` table
- Slot allocation logic in `buildRegisterWorker()`
- Dynamic query building in `buildGetExecutions()`
- Slot release logic in `buildReturnExecutions()`

### Implementation Steps

1. **Migration changes** (`migrations/0000000001_setup.sql`)
   - Add `concurrency_limit integer` to `_private_tasks` table
   - Add `concurrency_limit integer` to `task_spec` SQL type
   - Create `_private_concurrency_slots` table with indexes
   - Run `just build-migrations` to regenerate types

2. **TypeScript types** (`database-client.ts`)
   - Add `concurrency?: number | null` to `TaskSpec` interface

3. **SQL function - slot allocation** (`migrations/0000000001_setup.sql`)
   - Update `_private_register_worker()` SQL function to:
     - After upserting tasks, calculate slot groups for tasks with `concurrency_limit`
     - For each task with concurrency: `slots_per_group = ceil(concurrency / 50)`, `num_groups = ceil(concurrency / slots_per_group)`
     - Upsert slot groups into `_private_concurrency_slots`
     - **Cleanup orphaned slots (no foreign keys for performance):**
       - Delete slots for tasks removed from `_private_tasks` (no longer in worker spec)
       - Delete slots for tasks where `concurrency_limit` changed from value → NULL
       - Delete excess slots if `concurrency_limit` decreased (e.g., 500 → 10)

   **SQL approach for cleanup:**
   ```sql
   -- After upserting tasks in _private_register_worker(), clean up orphaned slots
   delete from pgconductor._private_concurrency_slots
   where task_key not in (
     select key from pgconductor._private_tasks
     where concurrency_limit is not null
   );
   ```

   - Update `buildRegisterWorker()` in `query-builder.ts` to pass `concurrency_limit` in task specs

4. **Query builder - get executions** (`query-builder.ts`)
   - Update `GetExecutionsArgs` to include `taskKeysWithConcurrency: string[]`
   - Update `buildGetExecutions()` to:
     - If `taskKeysWithConcurrency` is empty, use Case 1 query (fast path)
     - Otherwise, use Case 2 query (with slot logic)
     - Return executions with `slot_group_number` (nullable)

5. **Query builder - return executions** (`query-builder.ts`)
   - Update `buildReturnExecutions()` to:
     - Add CTE to release slots for completed/failed/released executions
     - Decrement `used` counter where `task_key` matches

6. **Worker logic** (`worker.ts`)
   - Build `taskKeysWithConcurrency` array from in-memory task registry
   - Pass to `buildGetExecutions()` args
   - Store `slot_group_number` from fetched executions
   - Pass to `buildReturnExecutions()` for slot release

7. **Tests** (`tests/integration/`)
   - Test task with concurrency=1 (serial execution)
   - Test task with concurrency=4 (parallel execution up to limit)
   - Test mixed queue (99 tasks without limit, 1 with limit)
   - Test high concurrency (500+) uses slot groups correctly
   - Test concurrency change (decrease/increase) updates slots

8. **Performance benchmarking** (`perf/`)

   **Throughput testing** (`perf/throughput/scenarios.ts`):
   Add scenarios to measure performance impact:

   ```typescript
   // Baseline - no concurrency limits
   "concurrency-baseline": {
     workers: 4,
     tasks: 10000,
     taskType: "noop",
     workerSettings: { /* default */ },
     // No concurrency limits set on tasks
   },

   // With concurrency limits (Case 2 query)
   "concurrency-enabled": {
     workers: 4,
     tasks: 10000,
     taskType: "noop",
     workerSettings: { /* default */ },
     taskConcurrency: 10,  // All tasks have limit
   },

   // Mixed (99 without, 1 with) - test zero overhead claim
   "concurrency-mixed": {
     workers: 4,
     tasks: 10000,
     taskType: "noop",
     workerSettings: { /* default */ },
     mixedConcurrency: { tasksWithLimit: 1, tasksWithout: 99 },
   },

   // High concurrency (test slot groups)
   "concurrency-high": {
     workers: 8,
     tasks: 100000,
     taskType: "noop",
     workerSettings: { /* aggressive batching */ },
     taskConcurrency: 500,
   }
   ```

   **Query analysis** (`perf/query-analysis/scenarios.ts`):
   Add scenarios for EXPLAIN ANALYZE:

   ```typescript
   "concurrency-baseline": {
     description: "Baseline: 1M pending, no concurrency",
     // No concurrency_limit set
   },

   "concurrency-single-task": {
     description: "1M pending, 1 task with concurrency=10",
     // One task has concurrency_limit
   },

   "concurrency-all-tasks": {
     description: "1M pending, all tasks with concurrency=10",
     // All tasks have concurrency_limit
   }
   ```

   **Expected results:**
   - Case 1 (no limits): Same as current performance (no regression)
   - Case 2 (with limits): Measure overhead from LEFT JOIN + slot CTE
   - Mixed queue: Minimal overhead (<5%) for tasks without limits
   - Document acceptable performance degradation threshold

   **Run benchmarks:**
   ```bash
   # Throughput comparison
   AGENT=1 bun perf/throughput/run.ts concurrency-baseline
   AGENT=1 bun perf/throughput/run.ts concurrency-enabled
   AGENT=1 bun perf/throughput/run.ts concurrency-mixed
   AGENT=1 bun perf/throughput/run.ts concurrency-high

   # Query plan analysis
   bun perf/query-analysis/compare-plans.ts
   ```

   **Metrics to track:**
   - Overall throughput (tasks/sec)
   - Per-worker throughput
   - Query execution time (`buildGetExecutions`, `buildReturnExecutions`)
   - Slot table operations (lock acquisition, release)
   - Index usage (verify `idx_slots_claim` is used)

   **Performance acceptance criteria:**
   - **Case 1 (no concurrency)**: 0% regression (identical query path)
   - **Case 2 (all tasks with limits)**: <15% throughput decrease acceptable
   - **Mixed queue (1 with, 99 without)**: <5% overhead for tasks without limits
   - **Query execution time**: Document EXPLAIN ANALYZE results in `perf/to-analyse/`
   - **Slot operations**: Slot lock/release should be <1ms each

   **Compare:**
   ```bash
   # Compare baseline vs enabled
   diff <(AGENT=1 bun perf/throughput/run.ts concurrency-baseline 2>&1) \
        <(AGENT=1 bun perf/throughput/run.ts concurrency-enabled 2>&1)
   ```

9. **Run quality checks**
   - `bun test && bun run typecheck`
   - `just format`
   - `just lint-fix`





