# Delayed Execution

Schedule tasks to run at a specific time in the future.

## Basic Delay

Use `run_at` to schedule a task for later:

```typescript
// Run in 1 hour
const runAt = Date.now() + 3600000;

await conductor.invoke(
  { name: "send-reminder" },
  { userId: "123", message: "Don't forget!" },
  { run_at: runAt }
);
```

The task won't execute until the specified time.

## With Deduplication

Combine delayed execution with `dedupe_key`:

```typescript
// Ensure only one reminder per event
await conductor.invoke(
  { name: "send-reminder" },
  { eventId: "evt-123" },
  {
    dedupe_key: `reminder-evt-123`,
    run_at: reminderTime,
  }
);
```

If invoked multiple times, only one execution is created.

## Cancellation

To cancel a delayed task, you need its execution ID:

```typescript
// Schedule with dedupe_key
const executionId = await conductor.invoke(
  { name: "delayed-task" },
  {},
  {
    dedupe_key: "unique-key",
    run_at: Date.now() + 3600000,
  }
);

// Later: cancel it
await conductor.cancel({ name: "delayed-task" }, "unique-key");
```

See [Cancellation](cancellation.md) for details.

## What's Next?

- [Cron Scheduling](cron.md) - Recurring scheduled tasks
- [Cancellation](cancellation.md) - Cancel pending executions
