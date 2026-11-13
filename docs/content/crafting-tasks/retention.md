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

**Remove completed at the next maintenance run:**

```typescript
{
  removeOnComplete: true
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
    removeOnComplete: true,  // Clean up quickly
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

## How It Works

Postgres Conductor runs daily maintenance automatically on each queue:

- Removes old completed executions based on `removeOnComplete`
- Removes old failed executions based on `removeOnFail`
- Deletes the steps and other stored state of removed executions with them
- Fails pending executions of tasks the queue's workers no longer run (see [Maintenance](../scaling/maintenance.md#executions-of-removed-tasks))

No manual intervention required. Maintenance does not release executions locked by a crashed worker: running orchestrators recover those through their heartbeats.

An execution's result is stored on the execution, so [`conductor.getExecution()` and `conductor.waitForResult()`](../api/conductor.md#conductorgetexecution) can only read it until the execution is removed. [`ctx.waitForResult()`](../api/task-context.md#ctxwaitforresult) receives the outcome if it started waiting before the removal, and throws if it starts after.

## What's Next?

- [Logging](logging.md) - Configure task logging
- [Testing](testing.md) - Unit test tasks
