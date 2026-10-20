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
        │   ├── task-context.ts       # Context API (step, sleep, invoke, invokeAndWait, waitForResult, waitForEvent, waitForAny, onSignal)
        │   ├── signal-inbox.ts       # In-process delivery of signals to ctx.onSignal handlers
        │   ├── middleware.ts         # Per-attempt middleware types
        │   ├── event-definition.ts   # Custom event definitions
        │   ├── event-dispatch-task.ts # Internal event fan-out task
        │   ├── maintenance-task.ts   # Per-queue retention and cleanup task
        │   ├── telemetry.ts          # OpenTelemetry tracing
        │   ├── database-client.ts    # Postgres client wrapper
        │   ├── query-builder.ts      # SQL for claiming, settlement, registration
        │   ├── testing.ts            # pgconductor-js/testing: createTestClock (fake time)
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

Tests use Vitest. Run them from `packages/pgconductor-js`. **Always run typecheck with tests:**

```bash
pnpm test && pnpm typecheck                    # Run tests and type checking (ALWAYS)
pnpm test:unit                                 # Unit tests only
pnpm test:integration                          # Integration tests only
pnpm vitest run tests/integration/<file>       # One file
```

`vitest.config.ts` has a `unit` and an `integration` project. The integration project's global setup starts one Postgres container (or uses `DATABASE_URL`) and provides its URL to every test file; `TestDatabasePool.create()` reads it with `inject("databaseUrl")`. Files run in parallel on 2 workers, each test in its own database.

`tests/integration/testing.test.ts` imports `pgconductor-js` and `pgconductor-js/testing` like a consumer (aliased to `src/` in `vitest.config.ts` and `tsconfig.json`) and reads `DATABASE_URL`, which the global setup also sets. `scripts/check-package.sh` runs it against the packed tarball, so it may only use the public API.

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
- Polls the database for ready executions of one queue, claiming only the tasks it registers
- Executes tasks with concurrency control (via `mapConcurrent`)
- Batches and flushes results back to the database
- Claiming returns each execution's undelivered signals, which prime its `SignalInbox` before the handler runs
- The claim statement also reads the cancellation flag and newer undelivered signals of the worker's running executions (`running` in `getExecutions`), aborts cancelled ones and hands signals to their `SignalInbox`. While all slots are busy the worker still runs it every `pollIntervalMs`, with a batch size of 0, as long as an execution runs
- Handles graceful shutdown via AbortController

**TaskContext** (`task-context.ts`)
- Provides API to task functions: `step()`, `sleep()`, `invoke()`, `invokeAndWait()`, `waitForResult()`, `waitForEvent()`, `subscribe()`, `waitForAny()`, `onSignal()`, `sendSignal()`, `signalWithStart()`, plus `executionId` and `queue`
- `invoke()` inserts an independent execution and memoizes its id as a step in one statement. Only with `cancelWithParent` does it set `parent_execution_id`, together with `cancel_with_parent`; settlement fails a completed child that has a parent but neither a waiter nor that flag
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
- Heartbeats keep running while workers drain after shutdown and stop in cleanup, so a slow handler is not recovered as stale
- Heartbeat signals carry only shutdowns; cancellation and application signals are read by the worker's claim
- Every 8th heartbeat recovers stale orchestrators (unlocks their executions); an orchestrator whose row was recovered stops itself when its next heartbeat has to re-insert the row
- Provides `stopped` promise for graceful shutdown
- Runs the internal event dispatch worker only when the conductor has `events` configured
- One queue = one codebase: every orchestrator polling a queue runs the same task set, so worker registration owns the whole queue and replaces its cron schedules and trigger subscriptions

**DatabaseClient** (`database-client.ts`)
- All database access goes through this interface
- Queries are built in `query-builder.ts` or call SQL functions from the migration
- Example: `db.getExecutions()`, `db.returnExecutions()`, `db.invoke()`

### SQL

**Important**: All core logic lives in Postgres. The TypeScript layer is intentionally thin - it validates input and orchestrates calls via `DatabaseClient`.

- Claiming (`buildGetExecutions`) and settlement (`buildReturnExecutions`) are single statements in `query-builder.ts`
- SQL functions in the migration: `invoke()`, `invoke_batch()`, `cancel_execution()`, `signal_execution()`, `signal_with_start()`, `emit_event()`, `drop_queue()`, `_private_register_worker()`, `_private_current_time()`, `_private_portable_uuidv7()`, ...

### Key Design Patterns

**Hangup/Resume**
- When a task calls `ctx.sleep()`, `ctx.invokeAndWait()` or `ctx.waitForEvent()`, the worker aborts the task
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

**Claims and Retries**
- Every claim sets a fresh `claim_token`. Settlement, `saveStep`, `registerEventWait` and `clearWaitingState` require it alongside `locked_by`, so a retried or late request from an earlier claim by the same orchestrator cannot modify a newer claim
- `DatabaseClient.query()` retries connection errors only when called with a `signal`, and a connection error is ambiguous: the statement may have committed. `getExecutions` and the periodic heartbeat are called without one and never retried; a failed poll or heartbeat is followed by the next one
- A claim whose response was lost is recovered like an expired lease: each worker tracks the executions it holds (claimed and not yet settled), and the heartbeat releases rows locked by its orchestrator that are not held and were locked more than one heartbeat interval ago
- `query()` retries by calling its callback again; build the query inside the callback, because a postgres.js query only runs once

**Middleware**
- `Conductor.create({ middleware })` wraps every attempt of non-batch, non-internal tasks in `worker.ts`
- `next(added)` merges `added` into the attempt's `TaskContext` and resolves on suspension too; the worker's outer race against the abort promise keeps a suspension from becoming a result or failure

**Cancel With Parent**
- `cancel_with_parent` on a child row links it to `parent_execution_id` without a waiting edge
- `cancel_execution()` on a pending execution cancels the linked children of every row it failed by calling itself, with that row's reason. A running execution only gets the flag; its settlement cascades
- Settlement cascades from `terminal_failures` (permanent failures, cancellations and failed waiting ancestors) in the same statement: `linked_descendants` follows linked children and, below the first level, the children they wait on. Running ones get the cancelled flag; pending ones fail as cancelled. A row in the same batch whose outcome would keep it alive (retry, sleep, child, signal re-queue) is cancelled instead, so those CTEs exclude `cancelled_descendants`

**Infinity Pattern**
- Postgres `'infinity'::timestamptz` for indefinite waiting
- Used when `ctx.invokeAndWait()` is called without timeout
- Parent waits forever until child completes

**Execution Metadata**
- `metadata jsonb` on `_private_executions` follows the same paths as `trace_context`: event → triggered executions, parent → child, terminal failure → dead-letter delivery, cron occurrence → next occurrence
- `TaskContext` passes its metadata explicitly to `ctx.invokeAndWait` children, `ctx.emit` and `ctx.schedule`
- `pgconductor._private_check_metadata()` enforces the 8 KB limit where metadata enters (`invoke()`, `emit_event()`, and `ctx.invokeAndWait` overrides before hang-up, so settlement never fails on size)

**Custom Events**
- An emitted event is an execution of the internal `pgconductor.event-dispatch` task on the `pgconductor.internal` queue; its id is the event id and there is no separate event table
- `DatabaseClient.emitEvent` generates the event id once per call, so a retry after a lost response finds the stored event instead of emitting it twice
- `emit(..., { dedupe_key })` stores `'<event name>:<dedupe_key>'` as the dispatch execution's `dedupe_key`; a repeated key emits nothing and returns the original event id. Completed dispatches are kept for one day (`removeOnComplete: { days: 1 }`), so keys are remembered until maintenance of the internal queue removes them
- The dispatcher matches subscriptions and inserts one delivery execution per matching subscription in a single statement; deliveries carry `subscription_id` and `parent_execution_id` (the event)
- Worker registration syncs a queue's trigger subscriptions; ids derive from the definition, so unchanged subscriptions keep their identity. Each trigger filter is compiled once into typed rows in `_private_event_filter_terms` (fields are ANDed, alternatives within a field are ORed)
- Fan-out is at-least-once: a retried dispatch re-evaluates current subscriptions, and a unique index deduplicates existing deliveries
- Event waits are `execution_wait` subscriptions keyed by `(execution_id, step_key)`. Dispatch stores the first matching event as the step result and wakes the execution only if it is suspended at an event wait (`waiting_step_key`), which may be at another of its subscriptions; `ctx.subscribe()` registers without suspending, so events matched while the execution runs are kept for the later wait
- `ctx.waitForAny()` suspends at its own step key over existing `ctx.subscribe()` subscriptions; dispatch stores branch events under their own keys as usual. `_private_register_event_race` picks the winner (earliest stored, then branch order), caches it under the waitForAny key and removes and closes all branch subscriptions

**Result Waits**
- `{ execution: id }` branches of `ctx.waitForAny()` are `execution_wait` subscriptions with `awaited_execution_id` set and no event key, so race, timeout and cleanup work as for event waits. `ctx.waitForResult()` is a `waitForAny()` with one such branch
- `_private_register_event_race` locks awaited executions before the waiter (the order settlement uses), stores the outcome of a settled one at once, and otherwise appends `{execution_id, queue, step_key}` to the awaited row's `result_waiters`
- Every path that ends an execution (settlement's `resolved_result_waits`, `cancel_execution()`, `invoke()` superseding a running execution, `clearWaitingState`, and removing pending schedule executions) reads `result_waiters` from the locked or deleted row, like `signal_count`, so a wait registered after its snapshot is not missed. It passes the waits with their outcomes to `_private_resolve_result_waits`, which stores `{status: 'resolved', outcome}` under each wait's step key and wakes a waiter suspended at a wait, like dispatch does. A new path that ends or removes executions must do the same; `drop_queue()` does not
- A race that settles removes its entries from the `result_waiters` of awaited executions still running, so timeouts and lost races do not accumulate entries
- The delivery is a plpgsql function called only when a settled execution has waiters: settlement is planned on every call, and each reference to the partitioned executions table adds planning cost per queue
- Waiting creates no parent link: a failed or cancelled execution never fails its waiter, and timeouts or cancelling the waiter leave it running. An unknown execution raises at registration

**Signals**
- `_private_execution_signals` stores one row per `(execution_id, id)`; `seq` orders delivery and `delivered_at` is set when a `ctx.onSignal` handler returned (at least once per id)
- Senders lock the execution row and increment `_private_executions.signal_count`. Settlement reads `signal_count` from its locked row, which is newer than its snapshot: a completion whose count exceeds the signals it can see, or that has undelivered signals for names the attempt listened for (`signal_names` in the result), is re-queued instead of completed
- Claiming an execution reads its undelivered signals under the row lock senders take, so signals stored later have a higher `seq` and later claims read them with `seq > after_seq`. Signal reads and acknowledgements are fenced by the claim token. `SignalInbox` delivers in `seq` order and skips a signal whose name has no handler yet; it stays undelivered, so settlement re-queues if a handler for it registers later. On completion the worker drains the inbox (`close({ drain: true })`): signals already read are delivered before settling, and only ones stored after the last read re-queue
- Re-queues are unbounded by design: each needs a signal stored after the previous attempt started, which the next attempt receives at claim time, so an execution re-runs only while signals keep arriving
- `key` on `_private_executions` with a partial unique index makes at most one non-terminal, non-cancelled execution per `(task_key, key, queue)`; `signal_with_start()` takes a transaction advisory lock on the key, then signals the existing execution or inserts one

### Task Configuration Options

```typescript
conductor.createTask(
  {
    name: "my-task",
    queue: "default",            // Queue name (default: "default")
    maxAttempts: 3,              // Max retry attempts before permanent failure (default: 3)
    timeoutMs: 60000,            // Abort ctx.signal and fail the attempt after this long
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
await db.clock.set(new Date("2024-01-01T12:00:00Z"));
await db.clock.advance("1h");
await db.clock.reset();
```

Useful for sleeps, timeouts, backoff schedules and time windows. Each test database is dropped afterwards, so fake time needs no cleanup. `db.client.setFakeTime()` sets the same setting but does not move the workers' clock.

Cron scheduling and time windows (worker fetch filtering, `ctx.step()` and `ctx.checkpoint()`) use the worker's `Clock` (`src/lib/clock.ts`): local time corrected by an offset sampled from the database when the worker starts and every 10 minutes. `Conductor.create({ clock })` replaces the local time. `db.clock` (`createTestClock(sql)` from `src/testing.ts`) sets `fake_now` and returns the same time from `now()`, so the offset stays zero; pass `clock: db.clock` to the conductor and running workers follow `db.clock.set()` and `advance()`. Use `vi.setSystemTime()` only for code that reads `Date` directly. Without `vi.useFakeTimers()` it only fakes `Date` and timers keep running; `vi.useRealTimers()` restores it. `vi.useFakeTimers()` fakes timers and `Date` but not `setImmediate`, which postgres.js needs to send queries (`fakeTimers` in `vitest.config.ts`).

## Development Environment

This project uses Node (LTS), pnpm for package management and Vitest for tests. The package builds with plain `tsc` (`tsconfig.build.json`, NodeNext), so relative imports in `src/` and `cli/` need a `.js` extension; `scripts/check-package.sh` packs the package and checks it from a Node consumer. Scripts that run TypeScript directly (`pnpm perf`, subprocess tests) use `tsx`, because the source uses parameter properties that Node's type stripping does not support.

### Code Quality

- **Console logs**: Always remove debug `console.log()` statements before committing
- **Tests**: Ensure all tests pass and clean up resources (database connections, fake time, etc.)
- **Type checking**: Always run both tests AND typecheck together:
  ```bash
  pnpm typecheck && pnpm test
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
