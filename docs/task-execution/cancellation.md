# Cancellation

Cancel task executions before they complete.

## Cancel by Execution ID

Cancel a specific execution:

```typescript
// Invoke and get execution ID
const executionId = await conductor.invoke(
  { name: "long-task" },
  { data: largeDataset }
);

// Cancel it
const cancelled = await conductor.cancel(executionId);
console.log(cancelled); // true if cancelled, false if already completed
```

## Cancellation States

**Pending execution:**

- Execution is marked as failed
- Error set to "Cancelled by user"
- Never runs

**Running execution:**

- `ctx.signal` is aborted
- Task should check signal and exit gracefully
- Execution is marked as cancelled

## Graceful Cancellation

Cancellation is checked at step boundaries. If your task runs into a `ctx.checkpoint()` or `ctx.step()` while cancelled, it will stop the execution gracefully. For more fine-grained control, you can use the shutdown signal directly:

```typescript
const processItems = conductor.createTask(
  { name: "process-batch" },
  { invocable: true },
  async (event, ctx) => {
    const { items } = event.payload;

    for (const item of items) {
      // Check if cancelled
      if (ctx.signal.aborted) {
        ctx.logger.info("Task cancelled, cleaning up...");
        await cleanup();
        throw new Error("Task was cancelled");
      }

      await processItem(item);
    }

    return { processed: items.length };
  }
);
```

## Cancel with Reason

Provide a cancellation reason:

```typescript
await conductor.cancel(executionId, {
  reason: "User requested cancellation"
});
```

The reason is stored in `last_error`.

## Unschedule Dynamic Cron

For dynamically scheduled cron tasks, use `ctx.unschedule()`:

```typescript
// In a task handler
await ctx.unschedule(
  { name: "scheduled-task" },
  "schedule-name"
);
```

This cancels the current execution and prevents future ones.

See [Cron Scheduling](cron.md#dynamic-scheduling) for details.

## What's Next?

- [Cron Scheduling](cron.md) - Unscheduling cron tasks
- [Timeouts](timeouts.md) - Automatic timeout handling
- [Delayed Execution](delayed.md) - Cancel delayed tasks
