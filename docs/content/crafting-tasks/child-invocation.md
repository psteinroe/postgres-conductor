# Child Invocation

Tasks can invoke other tasks to create workflows. The parent task waits for the child to complete and receives its return value.

## Basic Invocation

Use `ctx.invoke()` to call another task:

```typescript
const childProcessor = conductor.createTask(
  { name: "child-processor" },
  { invocable: true },
  async (event, ctx) => {
    const { input } = event.payload;
    const result = input * 2;
    return { output: result };
  }
);

const parentWorkflow = conductor.createTask(
  { name: "parent-workflow" },
  { invocable: true },
  async (event, ctx) => {
    const { value } = event.payload;

    // Invoke child task
    const childResult = await ctx.invoke(
      "process-step", // Step name (for idempotency)
      { name: "child-processor" },
      { input: value }
    );

    const finalResult = childResult.output + 10;
    return { result: finalResult };
  }
);
```

## How It Works

When `ctx.invoke()` is called:

1. The child task execution is created in the database
2. The parent task is released by the worker
3. The child task executes
4. When complete, the parent task is executed again, skipping already completed steps and invocations
5. The parent receives the child's return value

This ensures workers aren't blocked while waiting for child tasks.

## Timeouts

Set a timeout for child execution:

```typescript
const result = await ctx.invoke(
  "fetch-data",
  { name: "external-api-call" },
  { url: "https://api.example.com" },
  { timeout: 30000 } // 30 seconds
);
```

If the child doesn't complete within the timeout, the parent task fails with a timeout error.

## Error Handling

If a child task fails permanently (exhausts all retries), the parent task will be moved to `failed_executions` with an error like:
```
Child execution failed: <child error message>
```

## Multiple Children

You can invoke multiple child tasks:

```typescript
const orchestrator = conductor.createTask(
  { name: "orchestrator" },
  { invocable: true },
  async (event, ctx) => {
    // Sequential execution
    const step1 = await ctx.invoke(
      "step-1",
      { name: "validate" },
      event.payload
    );

    const step2 = await ctx.invoke(
      "step-2",
      { name: "process" },
      { data: step1.validatedData }
    );

    const step3 = await ctx.invoke(
      "step-3",
      { name: "notify" },
      { result: step2.processedData }
    );

    return { success: true };
  }
);
```

Each child runs sequentially. The parent hangs up and resumes for each child.

## What's Next?

- [Retries and Steps](retries-and-steps.md) - Learn about durable execution patterns
- [Timeouts](../task-execution/timeouts.md) - More on timeout configuration
- [Retries & Backoff](../task-execution/retries.md) - How failed tasks retry
- [Long-Running Tasks](long-running.md) - Handle tasks that run for extended periods
