# Worker Configuration

Tune worker performance with configuration options.

## Default Worker

Configure the default worker for tasks running on the default queue:

```typescript
const orchestrator = Orchestrator.create({
  conductor,
  tasks: [task1, task2, task3],
  defaultWorker: {
    concurrency: 10,          // Process 10 tasks simultaneously
    pollIntervalMs: 100,      // Check for work every 100ms
    flushIntervalMs: 2000,    // Flush results every 2 seconds
    fetchBatchSize: 20,       // Fetch up to 20 tasks per poll
  },
});
```

## Custom Workers

Configure custom workers according to their task requirements:

```typescript
const highPriorityWorker = conductor.createWorker({
  queue: "high-priority",
  tasks: [urgentTask, criticalTask],
  config: {
    concurrency: 5,
    pollIntervalMs: 50,  // Poll more frequently
  },
});

const backgroundWorker = conductor.createWorker({
  queue: "background",
  tasks: [cleanupTask, reportTask],
  config: {
    concurrency: 2,
    pollIntervalMs: 1000, // Poll less frequently
  },
});

const orchestrator = Orchestrator.create({
  conductor,
  workers: [highPriorityWorker, backgroundWorker],
});
```

## Configuration Options

### concurrency

Maximum tasks processed simultaneously:

```typescript
{
  concurrency: 5  // Up to 5 tasks at once
}
```

**Considerations:**

- CPU-bound tasks: Match CPU cores
- I/O-bound tasks: Higher concurrency
- Memory-intensive tasks: Lower concurrency

### pollIntervalMs

How often to check for new work (milliseconds):

```typescript
{
  pollIntervalMs: 1000  // Check every 1000ms
}
```

**Considerations:**

- **Lower (50-100ms):** Low latency, higher database load
- **Higher (1000-5000ms):** Lower database load, higher latency

### flushIntervalMs

How often to flush results to database:

```typescript
{
  flushIntervalMs: 2000  // Flush every 2 seconds
}
```

**Considerations:**

- **Lower:** More database writes, faster result visibility
- **Higher:** Fewer database writes, batched updates

### fetchBatchSize

Maximum executions fetched per poll:

```typescript
{
  fetchBatchSize: 10  // Fetch up to 10 tasks
}
```

**Considerations:**

- **Lower:** More polls, distributed across workers
- **Higher:** Fewer polls, more work per worker

## What's Next?

- [Concurrency](../task-execution/concurrency.md) - Control task concurrency
- [Orchestrator API](orchestrator.md) - Manage workers
- [Horizontal Scaling](../scaling/horizontal.md) - Run multiple workers
