# Batching

Process multiple items efficiently with batch invocation and batch task processing.

## Batch Invocation

Invoke multiple tasks in a single database operation:

```typescript
await conductor.invoke({ name: "send-email" }, [
  { payload: { to: "user1@example.com", subject: "Hello" } },
  { payload: { to: "user2@example.com", subject: "Hello" } },
  { payload: { to: "user3@example.com", subject: "Hello" } },
]);
```

This creates three task executions in one transaction, reducing database roundtrips.

### With Options

Each item can have its own options:

```typescript
await conductor.invoke({ name: "process-item" }, [
  {
    payload: { id: 1 },
    dedupe_key: "item-1",
    priority: 10,
  },
  {
    payload: { id: 2 },
    dedupe_key: "item-2",
    priority: 5,
  },
  {
    payload: { id: 3 },
    dedupe_key: "item-3",
    run_at: Date.now() + 3600000, // 1 hour later
  },
]);
```

## Batch Task Processing

Process multiple executions together in a single handler call. This is useful if you want to export to an external API that supports batch operations.

### Configuration

Define a batch task with `batch` config:

```typescript
const processBatch = conductor.createTask(
  {
    name: "process-items",
    batch: {
      size: 10,        // Max batch size
      timeoutMs: 5000, // Max wait time before processing partial batch
    },
  },
  { invocable: true },
  async (events, ctx) => {
    // events is an array of 1-10 items
    console.log(`Processing ${events.length} items together`);
  }
);
```

**Config options:**

- `size` - Maximum number of events to batch together
- `timeoutMs` - Maximum time to wait for a full batch before processing what's available

### Batch Tasks with Returns

Return an array of results matching the input:

```typescript
const processDataTask = defineTask({
  name: "process-data",
  payload: z.object({ value: z.number() }),
  returns: z.object({ doubled: z.number() }),
});

const processData = conductor.createTask(
  {
    name: "process-data",
    batch: { size: 20, timeoutMs: 1000 },
  },
  { invocable: true },
  async (events, ctx) => {
    // Must return array with same length as events
    return events.map(e => ({
      doubled: e.payload.value * 2,
    }));
  }
);

// Invoke
await conductor.invoke(
  { name: "process-data" },
  { value: 5 }
);
```

**Important**: The returned array must have the same length as the input array. Each result corresponds to the event at the same index.

### Batch Context

Batch tasks receive `BatchTaskContext` instead of regular `TaskContext`:

```typescript
const batchTask = conductor.createTask(
  {
    name: "batch-task",
    batch: { size: 10, timeoutMs: 5000 },
  },
  { invocable: true },
  async (events, ctx) => {
    // ctx is BatchTaskContext
    ctx.logger.info(`Processing ${events.length} events`);
    ctx.signal; // AbortSignal for cancellation

    // NOT available in batch tasks:
    // ctx.step()
    // ctx.sleep()
    // ctx.invoke()
  }
);
```

**BatchTaskContext has:**

- `logger` - Logging methods
- `signal` - AbortSignal for cancellation

**BatchTaskContext does NOT have:**

- `step()` - No step memoization
- `sleep()` - No sleeping/delaying
- `invoke()` - No child task invocation
- `executionId` - No single execution ID (processing multiple)

## What's Next?

- [Rate Limiting & Deduplication](../controlling-execution/rate-limiting.md) - Control task execution frequency and prevent duplicates
- [Worker Configuration](../scaling/worker-config.md) - Tune fetch batch size and polling
