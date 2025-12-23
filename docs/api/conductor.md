# Conductor API

The Conductor manages task definitions, database connections, and task invocation.

## Conductor.create()

Create a new Conductor instance:

```typescript
import { Conductor } from "pgconductor-js";
import { TaskSchemas } from "pgconductor-js";

const conductor = Conductor.create({
  sql,                  // postgres.js SQL instance (or use connectionString)
  tasks,                // TaskSchemas from defineTask
  context,              // Custom context object
  logger?,              // Optional custom logger
  events?,              // Optional EventSchemas
  database?,            // Optional DatabaseSchema
});
```

**Parameters:**

- `sql`: postgres.js SQL instance (mutually exclusive with `connectionString`)
- `connectionString`: Postgres connection string (mutually exclusive with `sql`)
- `tasks`: `TaskSchemas.fromSchema([...])` array of task definitions
- `context`: Object passed to task handlers as `ctx.{property}` (see [Custom Context](../crafting-tasks/custom-context.md))
- `logger`: (optional) Custom logger implementation
- `events`: (optional) `EventSchemas.fromSchema([...])` for typed custom events
- `database`: (optional) `DatabaseSchema.fromSchema({...})` for database event triggers

**Connection options:**

```typescript
// Option 1: Use existing postgres.js instance
const conductor = Conductor.create({
  sql: postgres(connectionString),
  tasks,
  context,
});

// Option 2: Provide connection string
const conductor = Conductor.create({
  connectionString: "postgres://localhost:5432/mydb",
  tasks,
  context,
});
```

## conductor.createTask()

Register a task handler:

```typescript
const task = conductor.createTask(
  taskRef,              // { name, queue?, maxAttempts?, window?, removeOnComplete?, removeOnFail? }
  triggers,             // Trigger config or array of configs
  handler               // async (event, ctx) => result
);
```

**taskRef:**

```typescript
{
  name: string;                           // Task name (required)
  queue?: string;                         // Queue name (default: "default")
  maxAttempts?: number;                   // Max retry attempts (default: 3)
  concurrency?: number;                   // Max concurrent executions (default: unlimited)
  window?: [string, string];              // Time window (e.g., ["09:00", "17:00"])
  removeOnComplete?: { days: number } | false;  // Retention policy
  removeOnFail?: { days: number } | false;      // Retention policy
  batch?: { size: number; timeoutMs: number };  // Batch processing config
}
```

**triggers:**

Single trigger:
```typescript
{ invocable: true }
{ cron: "0 9 * * *", name: "schedule-name" }
```

Multiple triggers:
```typescript
[
  { invocable: true },
  { cron: "0 9 * * *", name: "morning" },
  { cron: "0 17 * * *", name: "evening" },
]
```

**handler:**

```typescript
async (event, ctx) => {
  // event.name: "pgconductor.invoke" | <schedule-name>
  // For invocable: event.payload contains the task payload
  // For cron: event.name is the schedule name (e.g., "hourly", "daily")
  // ctx: Task context with step(), sleep(), invoke(), logger, etc.

  return result; // Optional return value
}
```

## conductor.invoke()

Invoke a task:

```typescript
const executionId = await conductor.invoke(
  { name: "task-name", queue?: "queue-name" },
  payload,
  options?
);
```

**Single invocation:**

```typescript
await conductor.invoke(
  { name: "send-email" },
  { to: "user@example.com", subject: "Hello" },
  {
    dedupe_key?: string,
    priority?: number,
    run_at?: Date,
    throttle?: { seconds: number },
    debounce?: { seconds: number },
  }
);
```

**Batch invocation:**

```typescript
await conductor.invoke(
  { name: "process-item" },
  [
    { payload: { id: 1 }, dedupe_key: "item-1" },
    { payload: { id: 2 }, dedupe_key: "item-2" },
    { payload: { id: 3 }, priority: 10 },
  ]
);
```

## conductor.createWorker()

Create a custom worker for a queue:

```typescript
const worker = conductor.createWorker({
  queue: "queue-name",
  tasks: [task1, task2],
  config: {
    concurrency?: number,
    pollIntervalMs?: number,
    flushIntervalMs?: number,
    fetchBatchSize?: number,
  },
});
```

## conductor.ensureInstalled()

Initialize database schema:

```typescript
await conductor.ensureInstalled();
```

Called automatically by Orchestrator.start(), but useful for manual schema setup.

## conductor.emit()

Emit a typed custom event:

```typescript
await conductor.emit(
  "event-name",
  { /* event payload */ }
);
```

Returns the event ID. Events trigger tasks with matching event triggers.

See [Task Triggers](../crafting-tasks/triggers.md) for details on event-triggered tasks.

## conductor.cancel()

Cancel an execution:

```typescript
const cancelled = await conductor.cancel(
  executionId,
  options?: { reason?: string }
);
```

Returns `true` if cancelled, `false` if already completed.

## What's Next?

- [Task Context API](task-context.md) - Available methods in task handlers
- [Orchestrator API](orchestrator.md) - Manage workers and task execution
- [Defining Tasks and Events](../crafting-tasks/defining-tasks.md) - Define tasks with Zod schemas
