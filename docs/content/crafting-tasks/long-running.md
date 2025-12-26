# Long-Running Tasks

For tasks that process many items or run for extended periods, use steps to cache side effects and checkpoints to allow graceful interruption.

## Steps for Side Effects

Use steps to cache operations with side effects - they prevent duplicate work on retry:

```typescript
const processLargeDataset = conductor.createTask(
  { name: "process-dataset" },
  { invocable: true },
  async (event, ctx) => {
    const items = await ctx.step("fetch-items", async () => {
      return await fetchThousandsOfItems();
    });

    // Each item gets its own step - never processes twice
    for (const item of items) {
      await ctx.step(`process-${item.id}`, async () => {
        await processItem(item); // API call, database write, etc.
      });
    }

    return { processed: items.length };
  }
);
```

**What steps do:**

- Cache results of operations with side effects
- Prevent duplicate work on retry (no double charges, duplicate emails, etc.)
- Provide automatic release points for shutdown and time windows

**Use steps when:**

- Operations have side effects (API calls, payments, emails, database writes)
- You want to prevent duplicate work on retry
- You can create meaningful unique names for each operation

## Checkpoints for Graceful Interruption

Use checkpoints to allow graceful shutdown in loops where steps don't make sense:

```typescript
const deleteOldData = conductor.createTask(
  { name: "delete-old-data" },
  { invocable: true },
  async (event, ctx) => {
    let hasMore = true;

    while (hasMore) {
      // Delete batch of 1000 oldest records
      hasMore = await deleteBatch(1000);

      // Allow graceful shutdown or window boundary
      await ctx.checkpoint();
    }

    return { completed: true };
  }
);
```

**What checkpoints do:**

1. Check if worker wants to shutdown (SIGTERM/SIGINT)
2. Check if task is outside its time window
3. If either is true: release task to resume later
4. If both are false: continue execution

**Use checkpoints when:**

- You can't enumerate items upfront (batch deletion of "oldest N records")
- Continuous polling until condition is met
- Long computations without meaningful intermediate step names
- You want more frequent release opportunities than step boundaries provide

**⚠️ Important:** Checkpoints don't cache results. The operation must be idempotent or track its own progress.

## Graceful Shutdown

Checkpoints enable zero-downtime deployments. When a worker receives a shutdown signal:

1. **Stops accepting new work** - No new tasks fetched from database
2. **Waits for safe release point** - Current task runs until it hits a checkpoint, step boundary or task completion
3. **Releases task** - Task returned to queue with current state
4. **Shuts down cleanly** - Worker process exits

This ensures no work is lost during deployments or scale-downs.

## Time Windows

Time windows restrict tasks to specific hours. Tasks outside their window pause at checkpoint or step boundaries:

```typescript
const nightlyProcessing = conductor.createTask(
  {
    name: "heavy-processing",
    window: ["22:00", "06:00"],  // 10 PM to 6 AM only
  },
  { invocable: true },
  async (event, ctx) => {
    for (const item of event.payload.items) {
      await ctx.step(`process-${item.id}`, async () => {
        await processItem(item);
      });

      // If window closes (6 AM arrives), task pauses here
      await ctx.checkpoint();
    }
  }
);
```

**Window format:**
```typescript
window: [start, end]  // ["HH:MM", "HH:MM"] in 24-hour format
```

**Behavior:**

| Time | Task State |
|------|------------|
| **Inside window** | Runs normally |
| **Outside window** | Pauses at next checkpoint/step, resumes when window opens |
| **Window opens** | Paused tasks automatically resume |

**Use cases:**

**1. Database maintenance during off-hours:**

```typescript
const dbMaintenance = conductor.createTask(
  {
    name: "vacuum-database",
    window: ["02:00", "04:00"],  // 2-4 AM only
  },
  { cron: "0 2 * * *" },
  async (event, ctx) => {
    await ctx.step("vacuum", async () => {
      await vacuumDatabase();
    });

    await ctx.step("analyze", async () => {
      await analyzeDatabase();
    });
  }
);
```

**2. Business hours processing:**

```typescript
const customerSupport = conductor.createTask(
  {
    name: "process-support-tickets",
    window: ["09:00", "17:00"],  // 9 AM - 5 PM
  },
  { invocable: true },
  async (event, ctx) => {
    // Only processes during business hours
    await notifyAgent(event.payload.ticketId);
  }
);
```

**3. Heavy processing during off-peak hours:**

```typescript
const bulkExport = conductor.createTask(
  {
    name: "bulk-export",
    window: ["20:00", "06:00"],  // 8 PM - 6 AM
  },
  { invocable: true },
  async (event, ctx) => {
    const records = await ctx.step("fetch-records", async () => {
      return await fetchAllRecords();
    });

    for (let i = 0; i < records.length; i += 1000) {
      const batch = records.slice(i, i + 1000);

      await ctx.step(`export-batch-${i}`, async () => {
        await exportBatch(batch);
      });

      // If 6 AM arrives, pause here and resume at 8 PM
      await ctx.checkpoint();
    }
  }
);

```
## What's Next?

- [Retries and Steps](retries-and-steps.md) - Understand retry behavior
- [Testing](testing.md) - Test long-running tasks with drain mode
- [Worker Configuration](../scaling/worker-config.md) - Configure polling and concurrency
