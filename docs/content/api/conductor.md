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
  middleware?,          // Optional per-attempt middleware
  logger?,              // Optional custom logger
  events?,              // Optional event definitions
  eventRetention?,      // Optional: how long emitted events are kept
  metadata?,            // Optional Standard Schema for execution metadata
  clock?,               // Optional time source, for tests
  telemetry?,           // Set to false to disable OpenTelemetry tracing
});
```

**Parameters:**

- `sql`: postgres.js SQL instance (mutually exclusive with `connectionString`)
- `connectionString`: Postgres connection string (mutually exclusive with `sql`)
- `tasks`: `TaskSchemas.fromSchema([...])` array of task definitions
- `context`: Object passed to task handlers as `ctx.{property}` (see [Custom Context](../crafting-tasks/custom-context.md))
- `middleware`: (optional) Functions that run around every attempt of a non-batch task and can add to `ctx` (see [Middleware](../crafting-tasks/custom-context.md#middleware))
- `logger`: (optional) Custom logger implementation
- `events`: (optional) Array of event definitions from `defineEvent()` for typed custom events
- `eventRetention`: (optional) How long emitted events are kept after dispatch, as a [retention policy](../crafting-tasks/retention.md); default `{ days: 1 }`. It bounds what [`conductor.listEvents()`](#conductorlistevents) reads and how long `dedupe_key`s are remembered
- `metadata`: (optional) Standard Schema that types and validates execution metadata (see [Execution Metadata](../crafting-tasks/execution-metadata.md))
- `clock`: (optional) Object with `now(): Date` that workers use instead of the system clock for cron schedules and time windows, still corrected to database time. Tests pass `createTestClock(sql)` (see [Testing](../crafting-tasks/testing.md))
- `telemetry`: (optional) `false` disables OpenTelemetry spans and trace context propagation. Enabled by default, using the globally registered provider

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

With `connectionString`, the Conductor owns the connection pool. Call [`conductor.close()`](#conductorclose) when you are done with it. A `sql` instance you pass in stays yours to close.

## conductor.createTask()

Register a task handler:

```typescript
const task = conductor.createTask(
  taskRef,              // { name, queue?, maxAttempts?, timeoutMs?, window?, removeOnComplete?, removeOnFail? }
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
  timeoutMs?: number;                     // Fail an attempt that runs longer (default: none)
  concurrency?: number;                   // Max concurrent executions (default: unlimited)
  window?: [string, string];              // Time window (e.g., ["09:00", "17:00"])
  removeOnComplete?: boolean | { days: number };  // Retention policy (true: at the next maintenance run)
  removeOnFail?: boolean | { days: number };      // Retention policy
  batch?: { size: number; timeoutMs: number };  // Batch processing config
  deadLetter?: { queue: string; task?: Task };    // Final-failure destination
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
    metadata?: Metadata,  // Execution metadata, at most 8 KB
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

Called automatically by Orchestrator.start(), but useful for manual schema setup. Safe to call from several processes at once. Throws if the database schema is newer than this version of Postgres Conductor.

## conductor.emit()

Emit a typed custom event:

```typescript
await conductor.emit(
  "event-name",
  { /* event payload */ },
  options?: { metadata?: Metadata; dedupe_key?: string }
);
```

Returns the event ID. Events trigger tasks with matching event triggers. Triggered executions
inherit the event's `metadata`.

`dedupe_key` deduplicates the emit: repeating it for the same event name emits nothing and returns
the original event ID. Keys are scoped per event name and remembered while the event is kept: 1–2
days after it was dispatched by default (`eventRetention`). See [Deduplicating Emits](../crafting-tasks/triggers.md#deduplicating-emits).

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

Executions it waits on with `ctx.invokeAndWait()` and executions it started with `ctx.invoke(..., { cancelWithParent: true })` are cancelled with it. See [Cancellation](../task-execution/cancellation.md#cancellation-states).

## conductor.signal()

Send a signal to an execution by ID, or to the non-terminal execution of a task that holds a key (the key [`signalWithStart()`](#conductorsignalwithstart) addresses it by). Returns `false` if the execution is completed, failed, cancelled or unknown, or no non-terminal execution holds the key. Sending a signal id again stores it once. See [Signals](../task-execution/signals.md#sending-signals).

```typescript
await conductor.signal(executionId, { name: "input", id: requestId, payload: { text: "hi" } });

// by key: starts nothing
await conductor.signal(
  { task: { name: "session", queue: "default" }, key: "thread-1" }, // queue is optional
  { name: "stop", id: requestId },
);
```

By key, the lookup and the signal run in one transaction under the lock `signalWithStart()` takes on the task and key, so a signal sent while `signalWithStart()` starts the execution for that key waits for it and reaches the new execution.

## conductor.signalWithStart()

Signal the non-terminal execution of a task for a key, or start one that receives the signal first. Returns the execution ID.

```typescript
const executionId = await conductor.signalWithStart({
  task: "session",
  queue: "default", // optional
  key: "thread-1",
  signal: { name: "input", id: requestId, payload },
  input: {}, // used only when it starts an execution (default: {})
  state: {}, // optional: its state (ctx.setState()), used only when it starts an execution; validated against the task's state schema
  listen: { thread: { event: chatMessage, filter: { threadId: ["1"] } } }, // optional: ctx.listen()s when it starts one
  priority: 0, // optional
  metadata: {}, // optional
});
```

## conductor.getExecution()

Read an execution by ID, by key or by dedupe key:

```typescript
const execution = await conductor.getExecution(executionId);

// pass the task to type the payload, state and result
const execution = await conductor.getExecution({ name: "send-email" }, executionId);

// the non-terminal execution that holds a key (see signalWithStart())
const session = await conductor.getExecution({ task: { name: "session" }, key: "thread-1" });

// the execution invoked with a dedupe_key
const order = await conductor.getExecution({ task: { name: "process-order" }, dedupeKey: "order-123" });
```

- By key, it reads only a pending or running execution: a completed, failed or cancelled one no longer holds its key
- By dedupe key, it reads the execution that holds the `dedupe_key`, whatever its status, until retention removes it. Throttled and debounced executions are keyed by their time slot, so a key may have several of them, and this lookup does not read them
- Both read the task's unique index on the key, and both type the execution by the task

Returns `null` if nothing matches or the execution was removed by its [retention policy](../crafting-tasks/retention.md). Otherwise the execution below. Read with a task, `payload`, `state` and `result` are typed by the task's definition; by ID alone, `payload` and `state` are any JSON object and `result` is `unknown`:

```typescript
{
  id: string;
  taskKey: string;
  queue: string;
  status: "pending" | "running" | "completed" | "failed" | "cancelled";
  key: string | null;         // the key signalWithStart() addresses it by
  payload: TPayload;          // the execution's input
  state: TState | null;       // what it last set with ctx.setState()
  listens: string[];          // names of its ctx.listen()s, in the order they were created
  result: TResult | null;     // return value, set when completed
  error: string | null;       // last attempt's error, or the cancellation reason
  attempts: number;
  createdAt: Date;
  completedAt: Date | null;
  failedAt: Date | null;
}
```

`pending` covers executions that wait for their first attempt, a retry, a sleep, a child or an event. `cancelled` is reported once the cancellation has settled; until then a running execution stays `running`.

`listens` names the [listens](../crafting-tasks/triggers.md#listening-for-events) set by `ctx.listen()` and `signalWithStart({ listen })` and not removed by `ctx.unlisten()`. Listening again under a name with another event or filter creates it anew, so it moves to the end. An execution that ended keeps the listens it ended with, but no event reaches them any more.

## conductor.getEvent()

Read an emitted event by ID:

```typescript
const event = await conductor.getEvent(eventId);
// { id: string; name: string; payload: object; createdAt: Date }
```

Returns `null` if the ID is unknown or the event was removed by `eventRetention`.

## conductor.listEvents()

Read the kept events of one name, in the order they were stored:

```typescript
const thread = await conductor.listEvents({
  name: "slack.message",
  where: { channel: "C1", threadTs: "1.0" }, // payload contains these fields
  order: "asc",                             // or "desc", newest first
  limit: 100,                               // default 100
  offset: 0,
});
```

The payload is typed by the event's definition. Other filters:

- `since`: only events stored after this `Date`
- `after` / `before`: only events stored after or before a kept event, by ID. With `order: "desc"` and `limit: 1`, `before` reads the event just before it

`conductor.countEvents()` takes the same query without `order`, `limit` and `offset` and returns how many events it matches. Events are kept for `eventRetention` (default one day), so only those are read. Reads by name and by payload fields use indexes that cover only events.

## conductor.listExecutions()

List a task's executions, in the order they were created or newest first:

```typescript
const open = await conductor.listExecutions({
  task: { name: "approval" },
  where: { sessionId: "worker:w_1" }, // payload contains these fields
  open: true,                          // only pending, waiting or running ones
  order: "asc",                        // or "desc", newest first
  limit: 100,                          // default 100
  offset: 0,
});
```

Each entry has the fields of [`conductor.getExecution()`](#conductorgetexecution), with `payload`, `state` and `result` typed by the task's definition. `listens` is read only for the entries returned. Executions removed by their [retention policy](../crafting-tasks/retention.md) are not listed. Listing open executions uses an index that covers only those; a task's settled ones are read through the `task_key` index and filtered.

## conductor.listListeners()

The open executions an event would reach through their [listens](../crafting-tasks/triggers.md#listening-for-events), matched as dispatch matches them, without emitting anything. Each entry has the fields of [`conductor.getExecution()`](#conductorgetexecution), whose `listens` names all of its listens, and `matched`, the names of the listens the event matches, in the order they were created:

```typescript
const listeners = await conductor.listListeners({
  event: "chat.message",
  payload: { threadId: "1" },
  task: { name: "conversation" }, // optional: only this task's executions
  limit: 100,                      // default 100
});
```

With `task`, each entry's `payload`, `state` and `result` are typed by the task's definition, as `listExecutions()` types them. Without it, the entries may belong to any task and are typed as by `getExecution(executionId)`.

## conductor.waitForResult()

Poll an execution until it settles and return its result:

```typescript
const result = await conductor.waitForResult(
  { name: "send-email" },     // optional, types the result
  executionId,
  {
    timeout?: DurationInput,  // milliseconds or a string such as "30s"; rejects with a TimeoutError
    pollIntervalMs?: number,  // default: 1000
    signal?: AbortSignal,     // rejects with the abort reason
  },
);
```

Resolves with the return value of a completed execution. The timeout also cancels a read in flight, so a blocked database cannot delay the rejection, and `timeout: 0` rejects at once. Rejects with the error message if the execution failed or was cancelled, and rejects if the execution does not exist. The result is only available while the execution exists: with `removeOnComplete: true` the execution is removed when it completes, so `waitForResult` rejects with a not found error.

Inside a task, use [`ctx.waitForResult()`](task-context.md#ctxwaitforresult) instead: it suspends the task rather than polling and returns failures and cancellations as values.

## conductor.close()

Close the connection pool created from `connectionString`:

```typescript
await orchestrator.stop();
await conductor.close();
```

Stopping an orchestrator leaves the connection open, so the conductor can keep invoking tasks and the orchestrator can be started again. Close the conductor last. If you passed in a `sql` instance, `close()` does nothing; call `sql.end()` yourself.

## What's Next?

- [Task Context API](task-context.md) - Available methods in task handlers
- [Orchestrator API](orchestrator.md) - Manage workers and task execution
- [Defining Tasks and Events](../crafting-tasks/defining-tasks.md) - Define tasks with Zod schemas
