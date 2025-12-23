# Concurrency

Limit how many instances of a task can run simultaneously.

## Task-Level Concurrency

Control the maximum number of concurrent executions for a specific task:

```typescript
const processVideo = conductor.createTask(
  {
    name: "process-video",
    concurrency: 3, // Max 3 videos processing at once
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

Postgres Conductor uses a slot-based system to enforce concurrency limits:

1. **Slot allocation**: When a task has `concurrency: N`, Postgres creates N slots in the `_private_concurrency_slots` table
2. **Claiming slots**: Workers claim available slots using `FOR UPDATE SKIP LOCKED`
3. **Execution**: Task runs while holding the slot
4. **Release**: Slot is released when execution completes or fails

This happens entirely in Postgres - no external coordination needed.

## Concurrency vs Worker Concurrency

**Task-level concurrency** (this page):

- Limits concurrent executions per task type
- Set on individual tasks with `concurrency` option
- Applies across all workers

**Worker concurrency** (see [Worker Configuration](../api/worker-config.md)):

- Controls how many tasks a single worker processes at once
- Set on worker/queue with `config: { concurrency }`
- Independent per worker instance

## What's Next?

- [Worker Configuration](../api/worker-config.md) - Configure worker-level concurrency
- [Priority](priority.md) - Control execution order when waiting for slots
- [Batching](batching.md) - Process multiple executions together
