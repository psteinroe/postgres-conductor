# Child Invocation

Tasks can invoke other tasks to create workflows. With `ctx.invokeAndWait()` the parent waits for the child to complete and receives its return value. With `ctx.invoke()` the child runs on its own.

## Basic Invocation

Use `ctx.invokeAndWait()` to call another task:

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
    const childResult = await ctx.invokeAndWait(
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

When `ctx.invokeAndWait()` is called:

1. The child task execution is created in the database
2. The parent task is released by the worker
3. The child task executes
4. When complete, the parent task is executed again, skipping already completed steps and invocations
5. The parent receives the child's return value

This ensures workers aren't blocked while waiting for child tasks.

## Timeouts

Set a timeout for child execution:

```typescript
const result = await ctx.invokeAndWait(
  "fetch-data",
  { name: "external-api-call" },
  { url: "https://api.example.com" },
  { timeout: 30000 } // 30 seconds
);
```

If the child doesn't complete within the timeout, the parent task fails with a timeout error.

## Error Handling

If a child task fails permanently (exhausts all retries), the parent task fails with an error like:
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
    const step1 = await ctx.invokeAndWait(
      "step-1",
      { name: "validate" },
      event.payload
    );

    const step2 = await ctx.invokeAndWait(
      "step-2",
      { name: "process" },
      { data: step1.validatedData }
    );

    const step3 = await ctx.invokeAndWait(
      "step-3",
      { name: "notify" },
      { result: step2.processedData }
    );

    return { success: true };
  }
);
```

Each child runs sequentially. The parent hangs up and resumes for each child.

## Invoking Without Waiting

Use `ctx.invoke()` to start work that runs on its own, such as a handoff to another workflow. It returns the new execution's ID right away:

```typescript
const intake = conductor.createTask(
  { name: "intake" },
  { invocable: true },
  async (event, ctx) => {
    const handoffId = await ctx.invoke("handoff", { name: "support-flow" }, event.payload);
    return { handoffId };
  }
);
```

| | `ctx.invokeAndWait()` | `ctx.invoke()` |
|-|-----------------------|----------------|
| Returns | The child's result | The execution ID |
| Caller | Hangs up until the child settles | Continues immediately |
| Child fails | Caller fails | Caller is unaffected |
| Caller cancelled | Child is cancelled | Execution keeps running, unless `cancelWithParent` is set |
| Retries and resumes | Return the memoized result | Return the same execution ID |

Both inherit the caller's [metadata](execution-metadata.md) unless `metadata` overrides it.

Retrying the invoking attempt never starts a second execution, even if it crashed right after the start: the execution is deduplicated by a key derived from the caller and the step name. Pass `dedupe_key` to use your own key instead, for example to reconnect to a workflow for a conversation. If an execution of the task already holds the key, `ctx.invoke()` returns its ID without starting, superseding or updating anything.

### Cancelling with the Parent

Pass `cancelWithParent: true` to cancel the execution together with the caller: when the caller is cancelled or fails permanently, the execution is cancelled with the same reason, whatever state it is in (pending, running or suspended in a sleep, wait or child). This applies transitively to executions it started with `cancelWithParent` and to the children it waits on:

```typescript
const session = conductor.createTask(
  { name: "session" },
  { invocable: true },
  async (event, ctx) => {
    const toolId = await ctx.invoke("tool", { name: "run-tool" }, event.payload, {
      cancelWithParent: true,
    });
    // keep working while the tool runs
  }
);
```

An execution that completed before the caller ended stays completed, and a caller retry or completion leaves it running. The link is set only when the call starts the execution: an execution returned for an existing `dedupe_key` keeps its own.

## Waiting for a Started Execution

Use `ctx.waitForResult()` to wait for an execution started earlier, for example after other work or with a timeout that should not cancel it:

```typescript
const workflow = conductor.createTask(
  { name: "review" },
  { invocable: true },
  async (event, ctx) => {
    const checkId = await ctx.invoke("start-checks", { name: "run-checks" }, event.payload);
    await ctx.step("notify", () => notifyReviewers(event.payload));

    const outcome = await ctx.waitForResult("checks", checkId, { timeout: "1h" });
    if (outcome.status === "completed") return { checks: outcome.result };
    if (outcome.status === "timeout") return { checks: "still running" };
    return { checks: `not passed: ${outcome.status}` };
  }
);
```

Unlike `ctx.invokeAndWait()`, the outcome is a value: a failed or cancelled execution resolves the wait instead of failing the caller, and a timeout leaves the execution running. To race the execution against events, pass it as an `{ execution: id }` branch to [`ctx.waitForAny()`](../api/task-context.md#ctxwaitforany).

Outside a task, await an execution with [`conductor.waitForResult()`](../api/conductor.md) or read it with `conductor.getExecution()`.

## What's Next?

- [Retries and Steps](retries-and-steps.md) - Learn about durable execution patterns
- [Timeouts](../task-execution/timeouts.md) - More on timeout configuration
- [Retries & Backoff](../task-execution/retries.md) - How failed tasks retry
- [Long-Running Tasks](long-running.md) - Handle tasks that run for extended periods
