# Timeouts

Set time limits for task handlers and child task execution.

## Execution Timeout

Set `timeoutMs` to limit how long a handler may run:

```typescript
const task = conductor.createTask(
  { name: "sync-account", timeoutMs: 60000 }, // 1 minute
  { invocable: true },
  async (event, ctx) => {
    const response = await fetch(event.payload.url, { signal: ctx.signal });
    return response.json();
  }
);
```

When the timeout elapses, the worker:

1. Aborts `ctx.signal`
2. Fails the attempt with `Task timed out after 60000ms`
3. Frees the concurrency slot

The failed attempt is retried like any other failure until `maxAttempts` is reached (see [Retries & Backoff](retries.md)). For batch tasks, the timeout applies to the whole batch and every execution in it fails.

The timer covers a single run of the handler. When a task resumes after `ctx.sleep()`, `ctx.invoke()` or `ctx.waitForEvent()`, the timer starts again.

JavaScript cannot stop a running function. A handler that ignores `ctx.signal` keeps running after the timeout and may overlap with its own retry. Pass `ctx.signal` to I/O that supports it, or check `ctx.signal.aborted` in long loops. `ctx.step()` does not start new steps once the signal is aborted.

During shutdown, `stop()` waits for running handlers. The timeout still applies, so a handler that ignores `ctx.signal` delays shutdown by at most `timeoutMs`.

The timeout is enforced by the worker that runs the handler. If the worker process dies, the execution stays locked until stale orchestrator recovery releases it.

## Child Invocation Timeout

Pass a timeout when invoking child tasks:

```typescript
const parent = conductor.createTask(
  { name: "parent" },
  { invocable: true },
  async (event, ctx) => {
    try {
      const result = await ctx.invoke(
        "call-api",
        { name: "external-api" },
        { url: event.payload.url },
        { timeout: 30000 } // 30 second timeout
      );
      return result;
    } catch (error) {
      ctx.logger.error("API call timed out");
      throw error;
    }
  }
);
```

If the child doesn't complete within 30 seconds, the parent task fails with a timeout error.

## Timeout Behavior

When a timeout occurs:

1. The parent task receives an error
2. The child task continues running (not cancelled)
3. The parent task fails and moves to retries

The child task completes independently, but its result isn't returned to the parent.

## Infinite Timeout

Omit the timeout option to wait indefinitely:

```typescript
// Wait forever for child to complete
const result = await ctx.invoke(
  "process-step",
  { name: "long-task" },
  { data: largeDataset }
  // No timeout - waits until complete
);
```

This uses `'infinity'::timestamptz` in the database.

## What's Next?

- [Child Invocation](../crafting-tasks/child-invocation.md) - Learn about child tasks
- [Retries & Backoff](retries.md) - Understand retry behavior
- [Cancellation](cancellation.md) - Cancel tasks explicitly
