# pgconductor

A durable task execution system built on PostgreSQL.

> **Important**: Keep this documentation up-to-date. When making architectural changes, refactoring core components, or introducing new patterns worth mentioning, update this file accordingly.

## Project Structure

```
pgconductor/
├── justfile                          # Command runner (use `just` for all commands)
├── migrations/
│   └── 0000000001_setup.sql          # Core schema, tables, SQL functions
└── packages/
    └── pgconductor-js/
        ├── src/
        │   ├── conductor.ts          # Task registry and invocation entry point
        │   ├── orchestrator.ts       # Worker lifecycle manager
        │   ├── worker.ts             # Fetch→execute→flush pipeline
        │   ├── task.ts               # Task wrapper with execute method
        │   ├── task-definition.ts    # Zod-based task definitions
        │   ├── task-context.ts       # Context API (step, sleep, invoke)
        │   ├── database-client.ts    # PostgreSQL client wrapper
        │   ├── lib/                  # Utilities (deferred, async-queue, etc.)
        │   └── generated/
        │       └── sql.ts            # Auto-generated types from SQL
        └── tests/
            ├── unit/                 # Unit tests for utilities (no DB)
            ├── integration/          # End-to-end tests with real DB
            └── fixtures/             # Test utilities (TestDatabasePool)
```

## Development Workflow

### Command Runner

Use `just` for all project commands (defined in `justfile`):

```bash
just build-migrations              # Rebuild TypeScript types from migrations
```

### Running Tests

Tests use Bun test runner:

```bash
bun test                           # All tests (unit + integration)
bun test tests/unit/               # Unit tests only
bun test tests/integration/        # Integration tests only
```

**Unit Tests** (`tests/unit/`)
- Test utility functions in isolation (no database required)
- Examples: `deferred.test.ts`, `map-concurrent.test.ts`, `async-queue.test.ts`, `wait-for.test.ts`
- Fast, no setup required

```typescript
import { test, expect } from "bun:test";
import { Deferred } from "../../../src/lib/deferred";

test("deferred resolves", async () => {
  const deferred = new Deferred<number>();
  deferred.resolve(42);
  expect(await deferred.promise).toBe(42);
});
```

**Integration Tests** (`tests/integration/`)
- Test end-to-end workflows with real PostgreSQL database
- Examples: `basic-execution.test.ts`, `step-support.test.ts`, `invoke-support.test.ts`
- Use `TestDatabasePool` fixture for isolated database instances

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
  const db = await pool.child();  // Isolated connection
  // test code
});
```

### Modifying Migrations

1. Edit `migrations/0000000001_setup.sql` directly (in-place)
2. Run `just build-migrations` to regenerate types
3. Run tests to verify changes

**Note**: This project is under active development. All SQL changes should be made **in-place** by editing the existing migration file, not by creating new migration files.

The migration file contains:
- Table schemas (`tasks`, `executions`, `steps`, `failed_executions`, `test_config`)
- SQL functions (all logic is in SQL, not application code)
- Indexes

**Note**: The `test_config` table is used for testing purposes (e.g., controlling time with `fake_now`).

## Architecture

### Core Components

**Worker** (`worker.ts`)
- Implements async pipeline: fetch → execute → flush
- Polls database for ready executions using `get_executions()`
- Executes tasks with concurrency control (via `mapConcurrent`)
- Batches and flushes results back to database
- Handles graceful shutdown via AbortController

**TaskContext** (`task-context.ts`)
- Provides API to task functions: `step()`, `sleep()`, `invoke()`
- All operations are idempotent (use steps as memoization)
- Hangup pattern: abort execution and return never-resolving promise
- Resume happens automatically when database wakes execution

**Conductor** (`conductor.ts`)
- Task registry and factory
- Entry point for invoking tasks
- Manages database client lifecycle

**Orchestrator** (`orchestrator.ts`)
- Manages multiple workers
- Handles startup/shutdown coordination
- Provides `stopped` promise for graceful shutdown

**DatabaseClient** (`database-client.ts`)
- Wraps all SQL function calls for easier unit testing and mocking
- All SQL functions are called through this interface
- Example: `db.getExecutions()`, `db.returnExecutions()`, `db.invoke()`

### SQL Functions

**Important**: All core logic lives in PostgreSQL functions (not application code). The TypeScript layer is intentionally thin - it only orchestrates calls to SQL functions via `DatabaseClient`.

SQL functions in `migrations/0000000001_setup.sql`:
- `get_executions()`: Fetch and claim ready executions
- `return_executions()`: Process results (completed/failed/released)
- `invoke()`: Create child execution and set parent waiting state
- `backoff_seconds()`: Return retry delay based on attempt number
- Helper functions: `current_time()`, `portable_uuidv7()`, etc.

### Key Design Patterns

**Hangup/Resume**
- When a task calls `ctx.sleep()` or `ctx.invoke()`, the worker aborts the task
- The execution remains in the database with updated `run_at` or `waiting_on_execution_id`
- Worker polls and resumes execution when ready
- Steps provide memoization across hangups

**Step Memoization**
- `ctx.step(name, fn)` checks if step exists in database before executing
- If exists, returns cached result
- If not, executes fn and saves result
- This enables idempotent retries and resume after hangup

**Cascade Failures**
- When child fails permanently (attempts >= max_attempts), parent is moved to `failed_executions`
- Implemented in `return_executions()` via `permanently_failed_children` CTE
- Parent receives error like "Child execution failed: <child_error>"

**Infinity Pattern**
- PostgreSQL `'infinity'::timestamptz` for indefinite waiting
- Used when `invoke()` called without timeout
- Parent waits forever until child completes

### Task Configuration Options

Tasks accept a configuration object with these options:

```typescript
conductor.createTask("my-task", handler, {
  maxAttempts: 3,        // Max retry attempts before permanent failure (default: from DB)
  flushInterval: 2000,   // How often to flush results to DB in ms (default: 2000)
  pollInterval: 1000,    // How often to poll for new executions in ms (default: 1000)
  partition: false,      // Enable partitioning (default: false)
  window: ["09:00", "17:00"],  // Time window for execution [start, end]
});
```

**Note**: Lower `pollInterval` values (e.g., 100ms) are useful in tests for faster execution cycles.

## Query Optimization Principles

Guidelines for SQL functions:

1. **Filter before joining**: Apply WHERE on small result sets before joining large tables
   ```sql
   -- Good: filter results first (0-1 rows), then join to find parent
   FROM results r
   WHERE r.status = 'completed'
   JOIN pgconductor.executions parent_e ON parent_e.waiting_on_execution_id = r.execution_id
   ```

2. **Materialize expensive operations**: Use CTEs to compute once and reuse
   ```sql
   permanently_failed_children AS (
     SELECT r.execution_id, r.error
     FROM results r
     JOIN pgconductor.executions e ON e.id = r.execution_id
     JOIN pgconductor.tasks w ON w.key = e.task_key
     WHERE r.status = 'failed' AND e.attempts >= w.max_attempts
   )
   -- Reuse in multiple places without recomputing join
   ```

3. **Prefer SQL functions with CTEs over plpgsql**: CTEs are declarative and easier to optimize

4. **No foreign keys**: Performance optimization - rely on application/SQL function logic

## Common Development Tasks

### Adding New Context Method

1. Add method to `TaskContext` class in `task-context.ts`
2. Implement using steps/database operations
3. Add integration test in `tests/integration/`
4. Update documentation

### Adding New SQL Function

1. Add function to `migrations/0000000001_setup.sql`
2. Run `just build-migrations` to regenerate types
3. Update `database-client.ts` if wrapper needed
4. Add tests

### Debugging Test Failures

Common issues:
- **Timing issues**: Increase wait times (backoff schedule is 15s, 30s, 60s...)
- **Cascade failures**: Check `permanently_failed_children` CTE logic
- **Infinity serialization**: postgres.js serializes infinity as null in JSON

Use `console.log()` in tests to inspect database state:
```typescript
const rows = await db.sql`SELECT * FROM pgconductor.executions`;
console.log(JSON.stringify(rows, null, 2));
```

### Controlling Time in Tests

The `pgconductor.current_time()` function checks a test configuration table before returning the real time. This allows tests to control time without waiting:

```typescript
// Set fake time (after orchestrator.start() so table exists)
const fakeTime = new Date("2024-01-01T12:00:00Z");
await db.sql`
  INSERT INTO pgconductor.test_config (key, value)
  VALUES ('fake_now', ${fakeTime.toISOString()})
  ON CONFLICT (key) DO UPDATE SET value = ${fakeTime.toISOString()}
`;

// Now all operations use fake time
await db.sql`SELECT pgconductor.current_time()`; // Returns 2024-01-01 12:00:00

// Advance time by 1 hour
const laterTime = new Date(fakeTime.getTime() + 3600000);
await db.sql`
  UPDATE pgconductor.test_config
  SET value = ${laterTime.toISOString()}
  WHERE key = 'fake_now'
`;

// Reset to real time
await db.sql`DELETE FROM pgconductor.test_config WHERE key = 'fake_now'`;
```

This is particularly useful for testing:
- Sleep/delayed executions
- Timeouts
- Backoff schedules
- Time-based windows

**Note**: Always clean up fake_now at the end of tests to avoid affecting other tests.

---

## Development Environment

### Bun Runtime

This project uses Bun for running TypeScript, tests, and package management.

```bash
# Package management
bun install                        # Install dependencies

# Running code
bun <file.ts>                      # Run TypeScript directly
bun run <script>                   # Run package.json script

# Testing
bun test                           # Run all tests
bun test tests/unit/               # Run unit tests
bun test tests/integration/        # Run integration tests
```

### Code Quality

- **Console logs**: Always remove debug `console.log()` statements before committing
- **Tests**: Ensure all tests pass and clean up resources (database connections, fake_now, etc.)
