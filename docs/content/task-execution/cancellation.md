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

A cancelled execution ends with `failed_at` set, `cancelled = true` and the reason in `last_error`, so it can be told apart from a failed one. It is never retried and never delivered to a [dead letter queue](dead-letter-queue.md).

**Pending execution:**

- Settles immediately and never runs
- This includes executions suspended in `ctx.sleep()`, `ctx.invoke()` or `ctx.waitForEvent()`; an event wait is removed with it

**Running execution:**

- `ctx.signal` is aborted with a `CancelledError` as `ctx.signal.reason`
- Task should check signal and exit gracefully
- Execution settles as cancelled once the task stops, even if it was about to sleep or invoke a child

## Workflows

Cancelling an execution also cancels the child it is waiting on, and that child's children. To keep a child running when its parent is cancelled, invoke it with `cancelWithParent: false`:

```typescript
const audit = await ctx.invoke(
  "audit",
  { name: "audit-log" },
  { orderId },
  { cancelWithParent: false }
);
```

The parent still waits for the child. If the parent is cancelled, the child completes on its own.

When a child is cancelled, the parent waiting on it resumes and `ctx.invoke()` throws a `CancelledError` with the cancellation reason as its message:

```typescript
import { CancelledError } from "pgconductor-js";

try {
  await ctx.invoke("approval", { name: "request-approval" }, { requestId });
} catch (err) {
  if (err instanceof CancelledError) {
    return { status: "cancelled" };
  }
  throw err;
}
```

A task that lets a `CancelledError` escape is cancelled too, with the same reason. An uncaught child cancellation therefore cancels the whole workflow waiting on it.

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
        throw ctx.signal.reason;
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

The reason is stored in `last_error` and is the message of the `CancelledError`.

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
