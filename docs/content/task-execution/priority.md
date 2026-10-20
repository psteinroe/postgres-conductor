# Priority

Control the order in which tasks are executed using priority values.

## Basic Priority

Set priority when invoking tasks:

```typescript
// High priority (processed first)
await conductor.invoke(
  { name: "critical-task" },
  { data: "urgent" },
  { priority: -10 }
);

// Low priority (processed last)
await conductor.invoke(
  { name: "background-task" },
  { data: "not-urgent" },
  { priority: 10 }
);
```

Lower numbers = higher priority. Ready executions are claimed in ascending priority order, then by `run_at`.

## Default Priority

If not specified, priority defaults to `0`:

```typescript
// Priority = 0 (default)
await conductor.invoke({ name: "task" }, { data });
```

## What's Next?

- [Deduplication](deduplication.md) - Prevent duplicate executions
- [Worker Configuration](../api/worker-config.md) - Configure concurrency and polling
