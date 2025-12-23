# Timeouts

Set time limits for child task execution.

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
        30000 // 30 second timeout
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

Omit the timeout parameter to wait indefinitely:

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
