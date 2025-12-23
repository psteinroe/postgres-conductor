# Time Windows

Restrict task execution to specific hours of the day.

## Overview

Time windows ensure tasks only run during specified time periods (e.g., business hours, night hours, maintenance windows). Tasks outside their window are skipped until the window opens.

## Basic Usage

```typescript
const nightlyTask = conductor.createTask(
  {
    name: "nightly-processing",
    window: ["22:00", "06:00"],  // 10 PM to 6 AM
  },
  { invocable: true },
  async (event, ctx) => {
    ctx.logger.info("Running during night hours");
  }
);
```

## Window Format

Windows use 24-hour time format:

```typescript
{
  window: [start, end]  // ["HH:MM", "HH:MM"]
}
```

**Examples:**

```typescript
// Business hours only
window: ["09:00", "17:00"]

// Night processing
window: ["22:00", "06:00"]

// Afternoon only
window: ["12:00", "18:00"]

// Early morning
window: ["00:00", "04:00"]
```

## Behavior

**During window:**

- Worker fetches and executes tasks normally
- New invocations run immediately (if ready)

**Outside window:**

- Worker skips fetching executions for this task
- Pending executions wait until window opens
- Cron schedules trigger but wait for window

**At step boundaries:**

- Running tasks continue until next step boundary
- Task releases worker and waits for window
- Resumes automatically when window opens

## Use Cases

**1. Database maintenance during off-hours:**

```typescript
const dbMaintenance = conductor.createTask(
  {
    name: "db-vacuum",
    window: ["02:00", "04:00"],  // 2-4 AM
  },
  { cron: "0 2 * * *", name: "daily-vacuum" },
  async (event, ctx) => {
    await ctx.step("vacuum", () => vacuumDatabase());
    await ctx.step("analyze", () => analyzeDatabase());
  }
);
```

**2. Business hours only processing:**

```typescript
const customerSupport = conductor.createTask(
  {
    name: "process-support-ticket",
    window: ["09:00", "17:00"],  // 9 AM - 5 PM
  },
  { invocable: true },
  async (event, ctx) => {
    // Only processes tickets during business hours
    await notifyAgent(event.payload.ticketId);
  }
);
```

**3. Rate-limited external API (night hours preferred):**

```typescript
const bulkExport = conductor.createTask(
  {
    name: "bulk-export",
    window: ["20:00", "06:00"],  // 8 PM - 6 AM
  },
  { invocable: true },
  async (event, ctx) => {
    // Heavy API usage during off-peak hours
    await exportLargeDataset(event.payload.dataset);
  }
);
```

## What's Next?

- [Cancellation](cancellation.md) - Cancel running executions
- [Priority](priority.md) - Control execution order
- [Worker Configuration](../api/worker-config.md) - Configure polling and concurrency
