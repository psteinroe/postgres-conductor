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

## ctx.waitForEvent()

Wait durably for the next matching custom event. The event must belong to the Conductor's
runtime `EventSchemas` catalog. Filters use the same typed operators and matching semantics as
event-triggered tasks. The subscription is persisted before the worker is released, so a restart
does not lose the wait.

```typescript
const event = await ctx.waitForEvent("payment", {
  event: paymentReceived,
  filter: { orderId: [orderId] },
  timeout: 60_000,
});
// event.name and event.payload are typed from paymentReceived
```

A matching event is delivered once and cached by the step key. The subscription becomes active
when registration commits; earlier events do not satisfy the wait. If the timeout wins,
`WaitForEventTimeoutError` is thrown. An event and the timeout never both win: the first one
recorded for the step key is final.

To wait for an event caused by your own side effect, use `ctx.subscribe()` so the
subscription is active before the side effect runs. To wait for the first of several
events, use `ctx.waitForAny()`.

## ctx.subscribe()

Subscribe to a custom event without suspending, then wait for it later:

```typescript
const subscription = await ctx.subscribe(
  stepKey: string,
  options: { event: EventDefinition, filter?: Filter }
): Promise<EventSubscription>

await subscription.wait(options?: { timeout?: DurationInput })
```

**Example:**

```typescript
const subscription = await ctx.subscribe("approval", {
  event: approvalDecided,
  filter: { approvalId: [approvalId] },
});
await ctx.step("post-card", () => postApprovalCard(approvalId));
const decision = await subscription.wait({ timeout: "24h" });
```

**Behavior:**
- The subscription is active once `subscribe()` resolves. The execution keeps running.
- One matching event emitted after that is stored under the step key, even while the
  execution is still running or retrying. Events emitted before `subscribe()` do not match.
- `wait()` returns the stored event immediately, or suspends like `ctx.waitForEvent()` until a
  matching event arrives. It throws `WaitForEventTimeoutError` if the timeout wins.
- The timeout starts when `wait()` is first called, not at `subscribe()`.
- The subscription is memoized by the step key: retries and resumes reuse it rather than
  creating a new one. `ctx.waitForEvent(stepKey, ...)` with the same key is equivalent to
  `wait()`.

## ctx.waitForAny()

Wait for whichever of several events arrives first, or a timeout:

```typescript
const winner = await ctx.waitForAny(
  stepKey: string,
  branches: Record<string, EventSubscription | { event: EventDefinition, filter?: Filter }>,
  options?: { timeout?: DurationInput }
): Promise<{ key: string, event } | { key: "timeout" }>
```

**Example:**

```typescript
const decision = await ctx.subscribe("decision", {
  event: approvalDecided,
  filter: { approvalId: [approvalId] },
});
await ctx.step("post-card", () => postApprovalCard(approvalId));

const winner = await ctx.waitForAny(
  "approval-or-reply",
  {
    decision,
    reply: { event: threadReplied, filter: { interactionId: [interactionId] } },
  },
  { timeout: "24h" },
);

if (winner.key === "decision") {
  // winner.event is typed as the approvalDecided event
} else if (winner.key === "reply") {
  // winner.event is typed as the threadReplied event
} else {
  // winner.key === "timeout"
}
```

**Behavior:**
- A branch is a `ctx.subscribe()` handle or an inline `{ event, filter }` that is subscribed
  when `waitForAny()` is called. Use a handle when the event is caused by a step that runs
  before the wait.
- Exactly one branch wins. A handle that already holds an event wins without suspending. If
  several do, the one whose event was dispatched first wins, and ties go to the branch listed
  first. Otherwise the execution suspends until the first matching event.
- The timeout starts when `waitForAny()` is first called. When it wins, the result is
  `{ key: "timeout" }` instead of an error. `timeout` cannot be used as a branch key.
- When the wait settles, every branch subscription is removed. Calling `wait()` on a handle
  that did not win throws, unless its event arrived before the wait settled.
- The result is memoized by the step key, so retries and resumes return the same winner.

## ctx.invoke()

Invoke a child task and wait for result:

```typescript
const result = await ctx.invoke<TResult>(
  stepName: string,
  taskRef: { name: string, queue?: string },
  payload: TPayload,
  options?: {
    timeout?: number,
    group?: string,
    metadata?: Metadata | ((metadata: Metadata | undefined) => Metadata),
  }
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
      { timeout: 30000 } // 30 second timeout
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
- The child inherits `ctx.metadata` unless `metadata` overrides it for that child

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

## ctx.metadata

Readonly metadata of the current execution, typed by the Conductor's `metadata` schema:

```typescript
ctx.metadata: Readonly<Metadata> | undefined
```

Children, emitted events and dynamic schedules created from this execution inherit it. See
[Execution Metadata](../crafting-tasks/execution-metadata.md).

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
