# Retention Policies

Configure how long to keep completed and failed executions.

## Task-Level Retention

Set retention when creating a task:

```typescript
const task = conductor.createTask(
  {
    name: "cleanup-task",
    removeOnComplete: { days: 7 },  // Remove after 7 days
    removeOnFail: { days: 30 },     // Keep failures longer
  },
  { invocable: true },
  async (event, ctx) => {
    // Task logic
  }
);
```

Old executions are automatically removed by the maintenance task.

## Configuration Options

**Keep completed for 7 days:**

```typescript
{
  removeOnComplete: { days: 7 }
}
```

**Keep failures for 90 days:**

```typescript
{
  removeOnFail: { days: 90 }
}
```

**Never remove completed (keep forever):**

```typescript
{
  removeOnComplete: false
}
```

**Never remove failures:**

```typescript
{
  removeOnFail: false
}
```

## Choosing Retention Periods

**Short retention for high-volume tasks:**

```typescript
const highVolumeTask = conductor.createTask(
  {
    name: "process-webhook",
    removeOnComplete: { days: 1 },  // Clean up quickly
    removeOnFail: { days: 7 },
  },
  { invocable: true },
  async (event, ctx) => {
    // Processes thousands per day
  }
);
```

**Long retention for auditing:**

```typescript
const auditedTask = conductor.createTask(
  {
    name: "financial-transaction",
    removeOnComplete: { days: 365 },  // Keep for 1 year
    removeOnFail: false,              // Never remove failures
  },
  { invocable: true },
  async (event, ctx) => {
    // Critical operations requiring audit trail
  }
);
```

**External archival before cleanup:**

```typescript
const archivedTask = conductor.createTask(
  {
    name: "important-task",
    removeOnComplete: { days: 30 },
  },
  { invocable: true },
  async (event, ctx) => {
    const result = await processData(event.payload);

    // Archive to external storage before cleanup
    await ctx.step("archive", async () => {
      await s3.put(`executions/${ctx.executionId}.json`, {
        event,
        result,
        completedAt: new Date(),
      });
    });

    return result;
  }
);
```

## How It Works

Postgres Conductor runs daily maintenance automatically on each queue:

- Removes old completed executions based on `removeOnComplete`
- Removes old failed executions based on `removeOnFail`
- Cleans up orphaned records
- Releases stuck executions

No manual intervention required.

## What's Next?

- [Logging](logging.md) - Configure task logging
- [Testing](testing.md) - Unit test tasks
