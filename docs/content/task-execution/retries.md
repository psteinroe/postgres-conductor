# Retries & Backoff

Tasks automatically retry with exponential backoff when they fail.

## Automatic Retries

If a task throws an error, Postgres Conductor retries it automatically:

```typescript
const unreliableTask = conductor.createTask(
  { name: "call-api" },
  { invocable: true },
  async (event, ctx) => {
    // Might fail due to network issues
    const response = await fetch(event.payload.url);
    if (!response.ok) {
      throw new Error(`API returned ${response.status}`);
    }
    return await response.json();
  }
);
```

The task retries until it succeeds or reaches max attempts.

## Backoff Schedule

After a failed attempt, the next attempt waits:

| Failed attempt | Delay      |
|----------------|------------|
| 1              | 15 seconds |
| 2              | 30 seconds |
| 3              | 1 minute   |
| 4              | 2 minutes  |
| 5              | 5 minutes  |
| 6              | 10 minutes |
| 7              | 20 minutes |
| 8              | 40 minutes |
| 9              | 1 hour     |
| 10 and later   | 2 hours    |

With the default of 3 attempts, a task retries after 15 and 30 seconds, then fails.

Resuming after `ctx.sleep()` or `ctx.waitForEvent()` does not count as an attempt. Resuming after `ctx.invoke()` does.

## Max Attempts

Configure max retry attempts per task:

```typescript
const criticalTask = conductor.createTask(
  { name: "process-payment", maxAttempts: 5 },
  { invocable: true },
  async (event, ctx) => {
    // Will retry up to 5 times total
    await processPayment(event.payload);
  }
);
```

Default is 3 attempts.

## Steps and Retries

Steps cache results across retries:

```typescript
const task = conductor.createTask(
  { name: "multi-step" },
  { invocable: true },
  async (event, ctx) => {
    // This only runs once, even across retries
    const data = await ctx.step("fetch", async () => {
      return await expensiveApiCall();
    });

    // If this fails, fetch() won't re-run on retry
    const processed = await ctx.step("process", async () => {
      return processData(data);
    });

    return { result: processed };
  }
);
```

See [Retries and Steps](../crafting-tasks/retries-and-steps.md) for details.

## Cascade Failures

If a child task fails permanently, the parent also fails:

```typescript
const parent = conductor.createTask(
  { name: "parent" },
  { invocable: true },
  async (event, ctx) => {
    const child = await ctx.invoke(
      "call-child",
      { name: "child" },
      { data: event.payload }
    );
    return child;
  }
);
```

If the child exhausts retries, the parent gets an error like:
```
Child execution failed: <child error message>
```

## What's Next?

- [Retries and Steps](../crafting-tasks/retries-and-steps.md) - Handle retries safely with steps
- [Timeouts](timeouts.md) - Set execution time limits
- [Cancellation](cancellation.md) - Stop task execution
