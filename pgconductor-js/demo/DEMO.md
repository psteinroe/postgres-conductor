# pgconductor Demo

Progressive walkthrough of pgconductor features, from basic task execution to advanced patterns.

## Prerequisites

```bash
# Start PostgreSQL
docker compose up -d

# Set environment variable (DATABASE_URL defaults to postgres://postgres:postgres@localhost:5432/postgres)
export AGENT=1
```

## How to Use

Each demo has two parts:
1. **Worker script** (`XX-worker.ts`) - Starts a worker that runs continuously until you press Ctrl+C
2. **Invoke script** (`XX-invoke.ts`) - Invokes tasks (can be run multiple times)

**Workflow:**
```bash
# Terminal 1: Start the worker
bun run demo/01-worker.ts

# Terminal 2: Invoke tasks (can run multiple times)
bun run demo/01-invoke.ts
bun run demo/01-invoke.ts Alice
bun run demo/01-invoke.ts Bob
```

## Demos

### 1. Basic Task Execution

**Demonstrates:**
- Creating a conductor with task schemas
- Defining a simple task with `invocable` trigger
- Starting an orchestrator
- Invoking a task

**Run:**
```bash
# Terminal 1
bun run demo/01-worker.ts

# Terminal 2
bun run demo/01-invoke.ts
bun run demo/01-invoke.ts Alice
bun run demo/01-invoke.ts Bob
```

**What happens:**
- Worker starts and waits for tasks
- Each invocation creates a task that prints "Hello, {name}!"
- Worker processes tasks as they arrive

---

### 2. Steps & Sleep

**Demonstrates:**
- `ctx.step()` - Memoized execution blocks
- `ctx.sleep()` - Delayed execution
- Multi-step workflow with automatic resume

**Run:**
```bash
# Terminal 1
bun run demo/02-worker.ts

# Terminal 2
bun run demo/02-invoke.ts
bun run demo/02-invoke.ts apple banana cherry
bun run demo/02-invoke.ts x y z
```

**What happens:**
- Three steps: validate → transform → save
- Each step is cached (idempotent on retry)
- Sleep pauses execution for 1 second
- Worker automatically resumes after sleep

**Key insight:** Steps enable fault-tolerant workflows - if a task fails after step 2, retry starts from step 3.

---

### 3. Child Task Invocation

**Demonstrates:**
- `ctx.invoke()` - Call child tasks and wait for results
- Parent-child task orchestration
- Type-safe payload and return values

**Run:**
```bash
# Terminal 1
bun run demo/03-worker.ts

# Terminal 2
bun run demo/03-invoke.ts
bun run demo/03-invoke.ts 10
bun run demo/03-invoke.ts 42
```

**What happens:**
- Parent invokes child with input value
- Child processes (multiplies by 2) and returns result
- Parent receives result and adds 10
- Example: input=5 → child returns 10 → parent returns 20

**Key insight:** Parent execution pauses (hangup) until child completes. Entire workflow is durable and survives restarts.

---

### 4. Cron Scheduling

**Demonstrates:**
- Cron-based task triggers
- Multiple trigger types on same task
- Automatic schedule management

**Run:**
```bash
# Terminal 1
bun run demo/04-worker.ts

# Terminal 2 (optional manual invocations)
bun run demo/04-invoke.ts
bun run demo/04-invoke.ts
```

**What happens:**
- Task executes every 3 seconds via cron (automatic)
- Can also be manually invoked
- Orchestrator manages schedule lifecycle
- Watch worker output to see both cron and manual executions

**Key insight:** Tasks support multiple triggers. Cron schedules are automatically created on worker startup and cleaned up on shutdown.

---

### 5. Dynamic Cron Scheduling

**Demonstrates:**
- `ctx.schedule()` - Dynamically schedule tasks at runtime
- `ctx.unschedule()` - Remove dynamic schedules
- Runtime control of cron schedules
- Programmatic task scheduling

**Run:**
```bash
# Terminal 1
bun run demo/05-worker.ts

# Terminal 2 (schedule the task)
bun run demo/05-invoke.ts schedule

# Terminal 2 (later, unschedule it)
bun run demo/05-invoke.ts unschedule
```

**What happens:**
- Worker starts with no scheduled tasks
- Invoke "schedule" - creates a cron that runs every 3 seconds
- Watch the dynamic task execute automatically
- Invoke "unschedule" - removes the cron schedule
- Task stops executing automatically

**Key insight:** Unlike demo 04 where cron schedules are defined statically in code, `ctx.schedule()` allows you to create and remove schedules dynamically at runtime. Perfect for user-configurable schedules or conditional automation.

---

### 6. Multi-Worker Scaling

**Demonstrates:**
- Multiple workers with different queues
- Per-queue concurrency configuration
- `conductor.createWorker()` API
- Queue-based task routing

**Run:**
```bash
# Terminal 1 (single orchestrator with 3 workers)
bun run demo/06-worker.ts

# Terminal 2 (invoke tasks)
bun run demo/06-invoke.ts 10
```

**What happens:**
- Single orchestrator manages 4 workers:
  - **Default worker**: 2 concurrent tasks (300ms) - auto-created from `tasks` array
  - **Fast queue** (custom): 5 concurrent tasks (100ms) - via `createWorker()`
  - **Slow queue** (custom): 1 concurrent task (2000ms) - via `createWorker()`
  - **Normal queue** (custom): 3 concurrent tasks (500ms) - via `createWorker()`
- 40 tasks enqueued (10 per queue)
- Fast queue processes quickly with high concurrency
- Slow queue processes sequentially (1 at a time)
- Normal queue balances speed and concurrency
- Default worker handles tasks in "default" queue

**Key insight:** Two ways to configure workers:
1. **Default worker**: Pass `tasks` array to orchestrator (processes "default" queue)
2. **Custom workers**: Use `conductor.createWorker()` for custom queues with explicit configuration

This demo shows both approaches running side-by-side in a single orchestrator!

---

### 7. Events 🌟

**Demonstrates:**
- `ctx.waitForEvent()` - Wait for custom events with timeout
- `ctx.emit()` - Emit typed events
- Event-driven task coordination
- Decoupled communication patterns

**Run:**
```bash
# Terminal 1
bun run demo/07-worker.ts

# Terminal 2 (start listeners)
bun run demo/07-invoke-listener.ts
bun run demo/07-invoke-listener.ts
bun run demo/07-invoke-listener.ts

# Terminal 3 (emit event to wake all listeners)
bun run demo/07-invoke-emitter.ts "Hello World"
```

**What happens:**
- Start multiple listener tasks (they wait for events)
- Each listener waits up to 60 seconds
- Emit an event - all waiting listeners wake up and receive the data
- Listeners complete with the event payload

**Key insight:** Tasks can coordinate via events without tight coupling. Supports both custom events and database change events. This enables powerful reactive patterns and microservice-style architectures. Multiple tasks can wait for the same event!

---

## Advanced Patterns

### Database Events

Wait for PostgreSQL table changes:

```typescript
await ctx.waitForEvent("wait-insert", {
	schema: "public",
	table: "users",
	operation: "insert",
	columns: "email,created_at",
	timeout: 60000,
});
```

### Dynamic Scheduling

Schedule tasks at runtime:

```typescript
await ctx.schedule(
	{ name: "report-task" },
	"daily-reports",
	{ cron: "0 9 * * *" },
	{ source: "admin" },
);

await ctx.unschedule({ name: "report-task" }, "daily-reports");
```

### Cancellation

Cancel running executions:

```typescript
const executionId = await conductor.invoke({ name: "long-task" }, {});
await conductor.cancel(executionId, { reason: "User requested" });
```

### Batch Invocation

Invoke multiple executions at once:

```typescript
await conductor.invoke({ name: "process-item" }, [
	{ payload: { id: 1 } },
	{ payload: { id: 2 } },
	{ payload: { id: 3 } },
]);
```

---

## Architecture Notes

### Hangup/Resume Pattern

When a task calls `ctx.sleep()`, `ctx.invoke()`, or `ctx.waitForEvent()`:
1. Task execution aborts immediately
2. State is saved to database with next run time
3. Worker polls and resumes execution when ready
4. Steps provide memoization across hangups

### Fault Tolerance

- All operations are stored in PostgreSQL
- Tasks survive process crashes and restarts
- Automatic retries with exponential backoff
- Parent tasks fail if child exceeds `maxAttempts`

### Concurrency

- Workers poll for ready executions
- Multiple workers can process same queue
- Configurable concurrency per worker
- Database handles coordination (no locks)

---

## Next Steps

1. **Explore tests:** See `tests/integration/` for comprehensive examples
2. **Read CLAUDE.md:** Detailed architecture and development guide
3. **Check migrations:** `migrations/0000000001_setup.sql` contains all SQL logic
4. **Performance:** See `perf/` directory for benchmarks
