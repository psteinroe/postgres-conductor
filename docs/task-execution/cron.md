# Cron Scheduling

Schedule tasks to run automatically using cron expressions, either statically at startup or dynamically at runtime.

## Static Cron Schedules

Define cron schedules when creating tasks:

```typescript
const dailyReport = conductor.createTask(
  { name: "daily-report" },
  { cron: "0 9 * * *", name: "morning-report" }, // 9 AM daily
  async (event, ctx) => {
    ctx.logger.info("Generating daily report");
    // Generate report
  }
);
```

The orchestrator automatically schedules the next execution after each run. If the task fails and retries, it still reschedules the next execution.

## Multiple Schedules & Event Handling

A task can have multiple cron schedules and be invocable. Use `event.name` to distinguish between triggers:

```typescript
const notification = conductor.createTask(
  { name: "send-digest" },
  [
    { invocable: true },
    { cron: "0 9 * * *", name: "morning" },    // 9 AM
    { cron: "0 17 * * *", name: "evening" },   // 5 PM
  ],
  async (event, ctx) => {
    if (event.name === "morning") {
      ctx.logger.info("Sending morning digest");
      // Morning-specific logic
    } else if (event.name === "evening") {
      ctx.logger.info("Sending evening digest");
      // Evening-specific logic
    } else if (event.name === "pgconductor.invoke") {
      // Manually invoked
      const payload = event.payload;
    }
  }
);
```

Each schedule runs independently. The `event.name` property contains the schedule name for cron triggers or `"pgconductor.invoke"` for manual invocations.

## Dynamic Scheduling

Create and manage cron schedules at runtime using `ctx.schedule()`:

```typescript
const userPreferences = conductor.createTask(
  { name: "apply-preferences" },
  { invocable: true },
  async (event, ctx) => {
    const { userId, reportTime } = event.payload;

    // Schedule personalized report
    await ctx.schedule(
      { name: "user-report" },
      `user-${userId}`, // Unique schedule name
      { cron: `0 ${reportTime} * * *` }, // Custom time
      { userId } // Payload for scheduled task
    );
  }
);
```

**Parameters:**
```typescript
await ctx.schedule(
  taskRef,        // { name: "task-name", queue?: "queue-name" }
  scheduleName,   // Unique identifier for this schedule
  cronOptions,    // { cron: "expression" }
  payload         // Payload passed to task when it runs
);
```

## Unschedule

Remove a dynamic schedule:

```typescript
const cancelReport = conductor.createTask(
  { name: "cancel-user-report" },
  { invocable: true },
  async (event, ctx) => {
    const { userId } = event.payload;

    // Remove the schedule
    await ctx.unschedule(
      { name: "user-report" },
      `user-${userId}`
    );
  }
);
```

This stops future executions and cancels any pending scheduled execution.

## Schedule Names

Schedule names must be unique per task:

```typescript
// ✅ Good - unique names
await ctx.schedule({ name: "task" }, "schedule-1", { cron: "..." }, {});
await ctx.schedule({ name: "task" }, "schedule-2", { cron: "..." }, {});

// ❌ Bad - duplicate name overwrites
await ctx.schedule({ name: "task" }, "schedule-1", { cron: "0 9 * * *" }, {});
await ctx.schedule({ name: "task" }, "schedule-1", { cron: "0 17 * * *" }, {}); // Overwrites
```

## Deduplication

Multiple workers with the same cron schedule won't create duplicates:

```typescript
// Worker 1
const orch1 = Orchestrator.create({
  conductor,
  tasks: [cronTask],
});

// Worker 2 - same task, same schedule
const orch2 = Orchestrator.create({
  conductor,
  tasks: [cronTask],
});

// Only one scheduled execution exists in database
```

## What's Next?

- [Delayed Execution](delayed.md) - One-time scheduled tasks
- [Task Triggers](../crafting-tasks/triggers.md) - Understand trigger types
