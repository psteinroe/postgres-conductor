# Priority

Control the order in which tasks are executed using priority values.

## Basic Priority

Set priority when invoking tasks:

```typescript
// High priority (processed first)
await conductor.invoke(
  { name: "critical-task" },
  { data: "urgent" },
  { priority: 10 }
);

// Low priority (processed last)
await conductor.invoke(
  { name: "background-task" },
  { data: "not-urgent" },
  { priority: 1 }
);
```

Higher numbers = higher priority. Tasks with higher priority are processed first.

## Default Priority

If not specified, priority defaults to `0`:

```typescript
// Priority = 0 (default)
await conductor.invoke({ name: "task" }, { data });
```

## What's Next?

- [Rate Limiting & Deduplication](../controlling-execution/rate-limiting.md) - Control task execution frequency
- [Worker Configuration](../api/worker-config.md) - Configure concurrency and polling
