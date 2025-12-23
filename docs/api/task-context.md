# Task Context API

The task context (`ctx`) provides methods and properties available inside task handlers.

## ctx.step()

Execute idempotent steps with memoization:

```typescript
const result = await ctx.step(
  stepName: string,
  fn: () => Promise<T>
): Promise<T>
```

**Example:**

```typescript
const task = conductor.createTask(
  { name: "multi-step" },
  { invocable: true },
  async (event, ctx) => {
    const data = await ctx.step("fetch", async () => {
      return await fetchData();
    });

    const processed = await ctx.step("process", async () => {
      return processData(data);
    });

    return { result: processed };
  }
);
```

**Behavior:**
- First call: Executes `fn`, stores result in database
- Subsequent calls (retries): Returns cached result without executing `fn`
- Step names must be unique within a task execution

## ctx.sleep()

Pause execution for a duration:

```typescript
await ctx.sleep(
  stepName: string,
  durationMs: number
): Promise<void>
```

**Example:**

```typescript
const task = conductor.createTask(
  { name: "delayed-task" },
  { invocable: true },
  async (event, ctx) => {
    await ctx.step("step1", () => doWork());

    await ctx.sleep("pause", 3600000); // 1 hour

    await ctx.step("step2", () => moreWork());
  }
);
```

**Behavior:**
- Task hangs up (releases worker)
- Resumes after duration expires
- Execution continues from where it left off

## ctx.invoke()

Invoke a child task and wait for result:

```typescript
const result = await ctx.invoke<TResult>(
  stepName: string,
  taskRef: { name: string, queue?: string },
  payload: TPayload,
  timeout?: number
): Promise<TResult>
```

**Example:**

```typescript
const parent = conductor.createTask(
  { name: "parent" },
  { invocable: true },
  async (event, ctx) => {
    const childResult = await ctx.invoke(
      "call-child",
      { name: "child-task" },
      { input: event.payload.value },
      30000 // 30 second timeout
    );

    return { final: childResult.output + 10 };
  }
);
```

**Behavior:**
- Creates child execution
- Parent hangs up and waits
- Returns child's result
- Throws if child fails or times out

## ctx.checkpoint()

Save progress during long-running tasks:

```typescript
await ctx.checkpoint(): Promise<void>
```

**Example:**

```typescript
const task = conductor.createTask(
  { name: "process-batch" },
  { invocable: true },
  async (event, ctx) => {
    const { items } = event.payload;

    for (let i = 0; i < items.length; i++) {
      await processItem(items[i]);

      if (i % 100 === 0) {
        await ctx.checkpoint(); // Save progress
      }
    }
  }
);
```

**Behavior:**
- Commits current state to database
- Allows task to resume from checkpoint if interrupted
- Works with graceful shutdown

## ctx.schedule()

Dynamically schedule a cron task:

```typescript
await ctx.schedule(
  taskRef: { name: string, queue?: string },
  scheduleName: string,
  cronOptions: { cron: string },
  payload?: TPayload
): Promise<void>
```

**Example:**

```typescript
await ctx.schedule(
  { name: "report-task" },
  "user-123-daily",
  { cron: "0 9 * * *" },
  { userId: "123" }
);
```

## ctx.unschedule()

Remove a dynamic cron schedule:

```typescript
await ctx.unschedule(
  taskRef: { name: string, queue?: string },
  scheduleName: string
): Promise<void>
```

**Example:**

```typescript
await ctx.unschedule(
  { name: "report-task" },
  "user-123-daily"
);
```

## ctx.logger

Built-in logger:

```typescript
ctx.logger.info(message: string, ...args: unknown[]): void
ctx.logger.error(message: string, ...args: unknown[]): void
ctx.logger.debug(message: string, ...args: unknown[]): void
ctx.logger.warn(message: string, ...args: unknown[]): void
```

**Example:**

```typescript
ctx.logger.info("Processing order", { orderId: "123" });
ctx.logger.error("Failed to process", error);
```

## ctx.signal

AbortSignal for cancellation:

```typescript
ctx.signal: AbortSignal
```

**Example:**

```typescript
const task = conductor.createTask(
  { name: "cancellable" },
  { invocable: true },
  async (event, ctx) => {
    for (const item of items) {
      if (ctx.signal.aborted) {
        throw new Error("Task was cancelled");
      }
      await processItem(item);
    }
  }
);
```

## ctx.executionId

Current execution ID:

```typescript
ctx.executionId: string
```

**Example:**

```typescript
ctx.logger.info(`Execution ID: ${ctx.executionId}`);
```

## Custom Context

You can extend the task context with your own services and utilities.

See [Custom Context](../crafting-tasks/custom-context.md) for details.

## What's Next?

- [Custom Context](../crafting-tasks/custom-context.md) - Extend context with custom services
- [Conductor API](conductor.md) - Task creation and invocation
- [Orchestrator API](orchestrator.md) - Worker management
