# pgconductor

A durable task execution system built on Postgres.

> **Important**: Keep this documentation up-to-date. When making architectural changes, refactoring core components, or introducing new patterns worth mentioning, update this file accordingly.

## Project Structure

```
pgconductor/
├── justfile                          # Command runner (use `just` for all commands)
├── docs/                             # User documentation (zensical)
├── migrations/
│   └── 0000000001_setup.sql          # Core schema, tables, SQL functions
└── packages/
    └── pgconductor-js/
        ├── cli/                      # pgconductor CLI (database type generation)
        ├── src/
        │   ├── conductor.ts          # Task registry, invoke and emit entry point
        │   ├── orchestrator.ts       # Worker lifecycle manager
        │   ├── worker.ts             # Fetch→execute→flush pipeline
        │   ├── task.ts               # Task wrapper with execute method
        │   ├── task-definition.ts    # Standard Schema / type-only task definitions
        │   ├── task-context.ts       # Context API (step, sleep, invoke, waitForEvent)
        │   ├── event-definition.ts   # Custom event definitions
        │   ├── event-dispatch-task.ts # Internal event fan-out task
        │   ├── maintenance-task.ts   # Per-queue retention and cleanup task
        │   ├── telemetry.ts          # OpenTelemetry tracing
        │   ├── database-client.ts    # Postgres client wrapper
        │   ├── query-builder.ts      # SQL for claiming, settlement, registration
        │   ├── lib/                  # Utilities (deferred, async-queue, etc.)
        │   └── generated/
        │       └── sql.ts            # Auto-generated from migrations
        └── tests/
            ├── unit/                 # Unit and type tests (no DB)
            ├── integration/          # End-to-end tests with real DB
            ├── fixtures/             # Test utilities (TestDatabasePool)
            └── mocks/                # Minimal DatabaseClient mock
```

## Development Workflow

### Command Runner

Use `just` for all project commands (defined in `justfile`):

```bash
just build-migrations              # Rebuild src/generated/sql.ts from migrations
just lint                          # oxlint + typecheck
just format                        # oxfmt
```

### Running Tests

Tests use the Bun test runner. Run them from `packages/pgconductor-js`. **Always run typecheck with tests:**

```bash
bun test && bun run typecheck      # Run tests and type checking (ALWAYS)
bun test tests/unit/               # Unit tests only
bun test tests/integration/        # Integration tests only
```

**Unit Tests** (`tests/unit/`)
- Test utilities and type-level APIs in isolation (no database required)
- Examples: `lib/deferred.test.ts`, `lib/map-concurrent.test.ts`, `clock.test.ts`, `task-event-types.test.ts`

**Integration Tests** (`tests/integration/`)
- Test end-to-end workflows against a real Postgres (testcontainers, or `DATABASE_URL` if set)
- Use the `TestDatabasePool` fixture for isolated databases
- Prefer integration tests over mocks for anything that touches SQL behavior

```typescript
import { TestDatabasePool } from "../fixtures/test-database";

let pool: TestDatabasePool;

beforeAll(async () => {
  pool = await TestDatabasePool.create();
}, 60000);

afterAll(async () => {
  await pool?.destroy();
});

test("example", async () => {
  const db = await pool.child();  // Isolated database

  const conductor = Conductor.create({
    sql: db.sql,
    tasks: TaskSchemas.fromSchema([taskDefinition]),
    context: {},
  });

  const task = conductor.createTask(
    { name: "example-task" },
    { invocable: true },
    async (event, _ctx) => { /* handler */ },
  );

  const orchestrator = Orchestrator.create({ conductor, tasks: [task] });

  // Initialize schema before invoking tasks
  await conductor.ensureInstalled();
  await conductor.invoke({ name: "example-task" }, {});
  await orchestrator.drain();
});
```

**Important**: Integration tests must call `await conductor.ensureInstalled()` before invoking tasks. Without this, `conductor.invoke()` will fail with "schema pgconductor does not exist".

### Modifying Migrations

1. Edit `migrations/0000000001_setup.sql` directly (in-place)
2. Run `just build-migrations` to regenerate types
3. Run tests to verify changes

**Note**: This project is under active development. All SQL changes should be made **in-place** by editing the existing migration file, not by creating new migration files.

Tables live in the `pgconductor` schema with a `_private_` prefix (`_private_executions`, `_private_tasks`, `_private_steps`, `_private_queues`, `_private_orchestrators`, ...). `_private_executions` is list-partitioned by queue.

## Architecture

### Core Components

**Worker** (`worker.ts`)
- Implements async pipeline: fetch → execute → flush
- Polls the database for ready executions of one queue
- Executes tasks with concurrency control (via `mapConcurrent`)
- Batches and flushes results back to the database
- Handles graceful shutdown via AbortController

**TaskContext** (`task-context.ts`)
- Provides API to task functions: `step()`, `sleep()`, `invoke()`, `waitForEvent()`
- All operations are idempotent (use steps as memoization)
- Hangup pattern: abort execution and return never-resolving promise
- Resume happens automatically when database wakes execution

**Conductor** (`conductor.ts`)
- Task registry and factory
- Entry point for invoking tasks and emitting events
- Owns the database client: `close()` ends a pool created from `connectionString`; stopping an orchestrator never closes it

**Orchestrator** (`orchestrator.ts`)
- Manages multiple workers, plus the internal event-dispatch worker when events are configured
- Handles startup/shutdown coordination and heartbeats
- Every 8th heartbeat recovers stale orchestrators (unlocks their executions); an orchestrator whose row was recovered stops itself when its next heartbeat has to re-insert the row
- Provides `stopped` promise for graceful shutdown
- Runs the internal event dispatch worker only when the conductor has `events` configured

**DatabaseClient** (`database-client.ts`)
- All database access goes through this interface
- Queries are built in `query-builder.ts` or call SQL functions from the migration
- Example: `db.getExecutions()`, `db.returnExecutions()`, `db.invoke()`

### SQL

**Important**: All core logic lives in Postgres. The TypeScript layer is intentionally thin - it validates input and orchestrates calls via `DatabaseClient`.

- Claiming (`buildGetExecutions`) and settlement (`buildReturnExecutions`) are single statements in `query-builder.ts`
- SQL functions in the migration: `invoke()`, `invoke_batch()`, `cancel_execution()`, `emit_event()`, `drop_queue()`, `_private_register_worker()`, `_private_current_time()`, `_private_portable_uuidv7()`, ...

### Key Design Patterns

**Hangup/Resume**
- When a task calls `ctx.sleep()`, `ctx.invoke()` or `ctx.waitForEvent()`, the worker aborts the task
- The execution remains in the database with updated `run_at` or `waiting_on_execution_id`
- Worker polls and resumes execution when ready
- Steps provide memoization across hangups

**Step Memoization**
- `ctx.step(name, fn)` checks if step exists in database before executing
- If exists, returns cached result
- If not, executes fn and saves result
- This enables idempotent retries and resume after hangup

**Cascade Failures**
- When a child fails permanently (attempts >= max_attempts), its parent fails too
- Implemented in `buildReturnExecutions()` via the `permanently_failed_children` CTE
- Parent receives error like "Child execution failed: <child_error>"

**Infinity Pattern**
- Postgres `'infinity'::timestamptz` for indefinite waiting
- Used when `invoke()` is called without timeout
- Parent waits forever until child completes

**Custom Events**
- An emitted event is an execution of the internal `pgconductor.event-dispatch` task on the `pgconductor.internal` queue; its id is the event id and there is no separate event table
- `DatabaseClient.emitEvent` generates the event id once per call, so a retry after a lost response finds the stored event instead of emitting it twice
- The dispatcher matches subscriptions and inserts one delivery execution per matching subscription in a single statement; deliveries carry `subscription_id` and `parent_execution_id` (the event)
- Worker registration syncs a queue's trigger subscriptions; ids derive from the definition, so unchanged subscriptions keep their identity. Each trigger filter is compiled once into typed rows in `_private_event_filter_terms` (fields are ANDed, alternatives within a field are ORed)
- Fan-out is at-least-once: a retried dispatch re-evaluates current subscriptions, and a unique index deduplicates existing deliveries

### Task Configuration Options

```typescript
conductor.createTask(
  {
    name: "my-task",
    queue: "default",            // Queue name (default: "default")
    maxAttempts: 3,              // Max retry attempts before permanent failure (default: 3)
    window: ["09:00", "17:00"],  // Time window for execution [start, end]
    concurrency: 10,             // Max concurrent executions of this task
    batch: { size: 10, timeoutMs: 1000 }, // Process executions in batches
    removeOnComplete: { days: 7 },        // Retention
  },
  { invocable: true },
  handler,
);
```

Worker settings (`pollIntervalMs`, `flushIntervalMs`, `concurrency`, ...) are configured per worker. Lower `pollIntervalMs` values (e.g., 100ms) are useful in tests for faster execution cycles.

## Query Optimization Principles

Guidelines for SQL:

1. **Filter before joining**: Apply WHERE on small result sets before joining large tables
   ```sql
   -- Good: filter results first (0-1 rows), then join to find parent
   from results r
   where r.status = 'completed'
   join pgconductor._private_executions parent_e on parent_e.waiting_on_execution_id = r.execution_id
   ```

2. **Materialize expensive operations**: Use CTEs to compute once and reuse

3. **Prefer SQL functions with CTEs over plpgsql**: CTEs are declarative and easier to optimize

4. **Foreign keys**: Use cascading foreign keys for cold configuration and metadata tables. Avoid foreign keys on hot execution tables; maintain those relationships in SQL logic.

## Common Development Tasks

### Adding New Context Method

1. Add method to `TaskContext` class in `task-context.ts`
2. Implement using steps/database operations
3. Add integration test in `tests/integration/`
4. Update documentation in `docs/`

### Adding New SQL Function

1. Add function to `migrations/0000000001_setup.sql`
2. Run `just build-migrations` to regenerate types
3. Add a wrapper in `database-client.ts`
4. Add tests

### Debugging Test Failures

Common issues:
- **Timing issues**: Use fake time instead of waiting (backoff schedule is 15s, 30s, 60s...)
- **Cascade failures**: Check `permanently_failed_children` CTE logic
- **Infinity serialization**: postgres.js serializes infinity as null in JSON

Inspect database state in tests:
```typescript
const rows = await db.sql`select * from pgconductor._private_executions`;
```

### Controlling Time in Tests

`pgconductor._private_current_time()` returns `current_setting('pgconductor.fake_now')` when set. Test databases use a single connection (`max: 1`), so a session-level setting is seen by the test, conductor and workers alike:

```typescript
await db.client.setFakeTime({ date: new Date("2024-01-01T12:00:00Z") });
await db.client.setFakeTime({ date: new Date("2024-01-01T13:00:00Z") }); // advance
await db.client.clearFakeTime();
```

Useful for sleeps, timeouts, backoff schedules and time windows. Always clean up fake time at the end of tests.

Cron scheduling and time windows (worker fetch filtering, `ctx.step()` and `ctx.checkpoint()`) use the worker's `Clock` (`src/lib/clock.ts`): local time corrected by an offset sampled from the database when the worker starts and every 10 minutes. Set fake time before starting the orchestrator. To move time while a worker runs, restart the orchestrator or also shift the local clock with `setSystemTime` from `bun:test` (see `tests/integration/window-execution.test.ts`).

## Development Environment

This project uses Bun for running TypeScript, tests, and package management.

### Code Quality

- **Console logs**: Always remove debug `console.log()` statements before committing
- **Tests**: Ensure all tests pass and clean up resources (database connections, fake time, etc.)
- **Type checking**: Always run both tests AND typecheck together:
  ```bash
  bun run typecheck && bun test
  ```
**Format**: Ensure the code follows the formatting guidelines:
```bash
just format
```
**Linter**: Ensure the code does not emit any lint warnings:
```bash
just lint-fix
```

### Code Style Guidelines

**TypeScript**:
- **Array types**: Always use `T[]` syntax, never `Array<T>`
  ```typescript
  // ✅ Correct
  const tasks: Task[] = [];
  function process(items: string[]): number[] { }

  // ❌ Wrong
  const tasks: Array<Task> = [];
  function process(items: Array<string>): Array<number> { }
  ```
- always add AGENT=1 as an env var when running anything
- always use lower-case when writing SQL, also for keywords
- prefer || ofer ?? in typescript. good: runAtMs || null. bad: runAtMs ?? null
- NEVER use `as unknown` or `as any`
- never use `!` in typescript
- Always use "Postgres Conductor" when referring to this project
- Always use "Postgres" over "PostgreSQL"
