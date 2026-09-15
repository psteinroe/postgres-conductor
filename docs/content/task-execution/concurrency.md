# Concurrency

Limit how many instances of a task can run simultaneously.

## Task-Level Concurrency

Control the maximum number of concurrent executions for a specific task:

```typescript
const processVideo = conductor.createTask(
  {
    name: "process-video",
    concurrency: 3, // Soft max of 3 videos at once
    groupConcurrency: 1, // Soft max of 1 per invocation group
  },
  { invocable: true },
  async (event, ctx) => {
    // Heavy video processing
    await encodeVideo(event.payload.videoId);
  }
);
```

When the limit is reached, additional executions wait in the queue until a slot becomes available.

## How It Works

Postgres Conductor evaluates active executions when claiming work. Task and group limits are coordinated with `FOR UPDATE SKIP LOCKED`; limits are intentionally soft across concurrent workers. Grouped candidates whose group is full do not consume task-level capacity, so another available group can be claimed in the same batch.

This happens entirely in Postgres - no external coordination needed.

> [!WARNING]
> Setting concurrency on any task in a queue reduces throughput by up to 50% for the entire queue, regardless of how many tasks have concurrency limits.

## Concurrency vs Worker Concurrency

**Task-level concurrency** (this page):

- Limits concurrent executions per task type
- Set on individual tasks with `concurrency` option
- Applies across all workers

**Worker concurrency** (see [Worker Configuration](../api/worker-config.md)):

- Controls how many tasks a single worker processes at once
- Set on worker/queue with `config: { concurrency }`
- Independent per worker instance

Child invocations inherit the group supplied to `ctx.invoke`. Dynamic cron schedules accept `group` alongside `cron`, and each next cron execution preserves the group.

## What's Next?

- [Worker Configuration](../api/worker-config.md) - Configure worker-level concurrency
- [Priority](priority.md) - Control execution order when waiting for slots
- [Batching](batching.md) - Process multiple executions together

## Group concurrency

`group` may be supplied when invoking a task. `groupConcurrency` limits active executions within each `(queue, task, group)` scope; ungrouped invocations bypass that limit. Task and group limits compose and are intentionally soft across concurrent workers.
