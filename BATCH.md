# Batch Processing Design

## Overview

Batch processing allows tasks to process multiple executions together in a single handler invocation. This is useful for optimizing bulk operations like sending emails via bulk APIs, batch database inserts, or processing items through ML APIs.

## Task-Level Configuration

Tasks can opt into batching by specifying a `batch` configuration:

```typescript
conductor.createTask(
  {
    name: "send-email",
    batch: { size: 50, timeoutMs: 3000 }
  },
  { invocable: true },
  async (events, ctx) => {
    // events is Array<Payload> when batch config is set
    await emailAPI.sendBulk(events);
  }
);
```

**Batch config:**
- `size`: Maximum number of executions to batch together
- `timeoutMs`: Maximum time to wait for batch to fill before processing partial batch

## Handler Signatures

The handler signature changes based on whether the task defines a return type:

### Void Tasks (All-or-Nothing)

If the task does not define a `returns` type, all executions succeed or fail together:

```typescript
conductor.createTask(
  {
    name: "send-email",
    batch: { size: 50, timeoutMs: 3000 }
  },
  { invocable: true },
  async (events, ctx) => {
    // events: Array<{ to: string }>
    await emailAPI.sendBulk(events);
    // No return - all executions complete with no result
  }
);

// Behavior:
// - Handler returns → all 50 executions marked completed
// - Handler throws → all 50 executions fail and retry
```

### Tasks with Returns (Array Results)

If the task defines a `returns` type, handler MUST return array of same length:

```typescript
conductor.createTask(
  {
    name: "enrich-users",
    batch: { size: 100, timeoutMs: 5000 }
  },
  { invocable: true },
  async (events, ctx) => {
    // events: Array<{ userId: string }>
    const results = await enrichmentAPI.bulkProcess(events);

    // MUST return array of same length as events
    return results.map(r => ({
      name: r.userName,
      score: r.enrichmentScore
    }));
  }
);

// Behavior:
// - results[i] corresponds to events[i]
// - Each execution gets its individual result
// - Handler throws → all executions fail
```

## Context API

Batched tasks have limited context API compared to regular tasks:

```typescript
class BatchTaskContext {
  logger: Logger;
  signal: AbortSignal;

  // Supported:
  sleep(duration: string): Promise<never>;

  // NOT supported (batch composition is non-deterministic):
  // - step() - results depend on batch composition
  // - invoke() - parent-child relationships break on retry
}
```

### Sleep Behavior

When `ctx.sleep()` is called, ALL executions in the batch are rescheduled:

```typescript
async (events, ctx) => {
  const results = await externalAPI.bulkProcess(events);

  if (results.rateLimited) {
    await ctx.sleep("1 minute"); // All executions reschedule to run_at = now + 1min
  }

  return results.items;
}
```

After sleeping, executions may batch with different executions on retry - this is expected and safe since no state is carried over.

## Implementation

### BatchingAsyncQueue

The worker uses a `BatchingAsyncQueue` that automatically accumulates executions based on batch configuration:

```typescript
const queue = new BatchingAsyncQueue<Execution>(
  (taskKey) => {
    const task = tasks.get(taskKey);
    return task?.batch || null; // Returns { size, timeoutMs } or null
  }
);

// Push individual executions
await queue.push(execution1);
await queue.push(execution2);
// ...

// Queue automatically batches and emits groups when:
// 1. Batch size reached, OR
// 2. Timeout expires
```

### Worker Pipeline

The worker pipeline processes groups (always arrays):

```
fetch → push to BatchingAsyncQueue → executeTasks(groups) → flushResults
```

- Non-batched tasks: emitted as single-item arrays `[execution]`
- Batched tasks: accumulated and emitted when size/timeout reached `[exec1, exec2, ..., execN]`

### Database Model

Each execution is tracked individually in the database:
- Separate row per execution
- Individual retry attempts
- Individual run_at timestamps
- Individual parent/child relationships

Batching happens only in-memory at execution time. The database is unaware of batching.

## Use Cases

**Good for batching:**
- Sending emails via bulk API
- Inserting records with batch insert
- Processing images through ML batch endpoint
- Rate-limited APIs (with ctx.sleep())

**Not good for batching (use regular tasks):**
- Complex multi-step workflows
- Conditional logic requiring step memoization
- Tasks that invoke children
- Tasks where individual executions have different control flow

## Edge Cases

### Batch Composition Changes

Executions may batch with different peers across retries:

```
First attempt: [A, B, C, D, E]
→ Handler throws
→ All fail, increment attempts

Second attempt: [A, F, G] and [B, C, H, I, J]
→ Different batch compositions
→ This is expected and safe
```

### Partial Completion Not Supported

Current design does not support partial batch completion. If one execution should fail while others succeed, don't use batching - process them individually or handle errors within the batch handler:

```typescript
async (events, ctx) => {
  const results = await api.bulkProcess(events);

  // Filter out failures before returning
  const successful = results.filter(r => r.success);

  if (successful.length < events.length) {
    // Log failures but complete all
    ctx.logger.warn(`${events.length - successful.length} items failed`);
  }

  return results; // Return all, marking batch as complete
}
```
