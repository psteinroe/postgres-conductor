# Task Context API

The task context (`ctx`) provides methods and properties available inside task handlers.

## ctx.step()

Run a callback and save its result under the step name:

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
- First call: Executes `fn`, then saves its result in the database
- Later calls (retries and resumes): Return the saved result without executing `fn`
- `fn` runs again if the attempt ends before its result is saved, for example when it throws or the worker crashes after its side effect. Pass idempotency keys to external services (see [What Steps Guarantee](../crafting-tasks/retries-and-steps.md#what-steps-guarantee))
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
- The handler runs again from the start: steps return their saved results and this sleep returns at once

## ctx.waitForEvent()

Wait durably for the next matching custom event. The event must belong to the Conductor's
runtime `events` catalog. Filters use the same typed operators and matching semantics as
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

Wait for whichever of several subscriptions receives an event first, an execution settles first, or a timeout:

```typescript
const winner = await ctx.waitForAny(
  stepKey: string,
  branches: Record<string, EventSubscription | { execution: string, task?: { name: string, queue?: string } }>,
  options?: { timeout?: DurationInput }
): Promise<{ key: string, event } | { key: string, outcome: ExecutionOutcome } | { key: "timeout" }>
```

**Example:**

```typescript
const decision = await ctx.subscribe("decision", {
  event: approvalDecided,
  filter: { approvalId: [approvalId] },
});
const reply = await ctx.subscribe("reply", {
  event: threadReplied,
  filter: { interactionId: [interactionId] },
});
await ctx.step("post-card", () => postApprovalCard(approvalId));

const winner = await ctx.waitForAny("approval-or-reply", { decision, reply }, { timeout: "24h" });

if (winner.key === "decision") {
  // winner.event is typed as the approvalDecided event
} else if (winner.key === "reply") {
  // winner.event is typed as the threadReplied event
} else {
  // winner.key === "timeout"
}
```

An `{ execution: id }` branch wins when that execution settles, with the same outcome as
[`ctx.waitForResult()`](#ctxwaitforresult):

```typescript
const toolId = await ctx.invoke("start-tool", { name: "run-tool" }, input);
const click = await ctx.subscribe("click", { event: buttonClicked, filter: { toolId: [toolId] } });

const winner = await ctx.waitForAny("tool-or-click", { tool: { execution: toolId }, click }, { timeout: "1h" });
if (winner.key === "tool") {
  // winner.outcome: { status: "completed", result } | { status: "failed", error } | { status: "cancelled", reason }
}
```

**Behavior:**
- Each value is a subscription from `ctx.subscribe()` or an `{ execution: id }` branch, and
  `winner.key` is the name you gave it in the object. An execution branch is registered by
  `waitForAny()` itself and memoized under the step key `<stepKey>:<name>`.
- Add `task` to an execution branch to type `winner.outcome.result` by that task's return type.
  An execution of another task is then not found.
- Exactly one branch wins. One that already holds an event or outcome wins without suspending,
  including an execution that settled before the call. If several do, the one stored first
  wins, and ties go to the one listed first. Otherwise the execution suspends until the first
  matching event or settlement.
- Losing executions keep running; the wait never cancels them.
- The timeout starts when `waitForAny()` is first called. When it wins, the result is
  `{ key: "timeout" }` instead of an error, so `timeout` cannot be used as a name.
- When the wait settles, all of the subscriptions are removed. Calling `wait()` on one that did
  not win throws, unless its event arrived before the wait settled.
- The result is memoized by the step key, so retries and resumes return the same winner.

## ctx.emit()

Emit a typed custom event from a task:

```typescript
const eventId = await ctx.emit(
  "event-name",
  { /* event payload */ },
  options?: { dedupe_key?: string }
);
```

Emits are not memoized, so a retried attempt emits again. Pass a `dedupe_key` derived from the
execution's input to emit once across attempts: repeating it for the same event name emits nothing
and returns the original event ID. Keys are scoped per event name and remembered for 1–2 days after
the event was dispatched. See [Deduplicating Emits](../crafting-tasks/triggers.md#deduplicating-emits).

## ctx.invokeAndWait()

Invoke a child task and wait for result:

```typescript
const result = await ctx.invokeAndWait(
  stepName: string,
  taskRef: { name: string, queue?: string },
  payload: TPayload,
  options?: {
    timeout?: number,
    group?: string,
    metadata?: Metadata | ((metadata: Metadata | undefined) => Metadata),
  }
): Promise<TResult> // inferred from the task's return type
```

**Example:**

```typescript
const parent = conductor.createTask(
  { name: "parent" },
  { invocable: true },
  async (event, ctx) => {
    const childResult = await ctx.invokeAndWait(
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
- Returns child's result, memoized by the step name
- Throws if the timeout elapses first; the child is cancelled
- If the child fails permanently or is cancelled, the parent fails permanently with
  `Child execution failed: <error>` without running its handler again, so a `catch` around
  `invokeAndWait()` does not see it. Use `ctx.invoke()` and
  [`ctx.waitForResult()`](#ctxwaitforresult) to handle a failed child as a value
- The child inherits `ctx.metadata` unless `metadata` overrides it for that child

## ctx.invoke()

Invoke a task without waiting for it and return the execution ID, like `conductor.invoke()`:

```typescript
const executionId = await ctx.invoke(
  stepName: string,
  taskRef: { name: string, queue?: string },
  payload: TPayload,
  options?: {
    dedupe_key?: string,
    metadata?: Metadata | ((metadata: Metadata | undefined) => Metadata),
    run_at?: Date,
    priority?: number,
    group?: string,
    cancelWithParent?: boolean,
  }
): Promise<string>
```

**Example:**

```typescript
const intake = conductor.createTask(
  { name: "intake" },
  { invocable: true },
  async (event, ctx) => {
    const handoffId = await ctx.invoke("handoff", { name: "support-flow" }, event.payload);
    return { handoffId };
  }
);

// elsewhere
const result = await conductor.waitForResult({ name: "support-flow" }, handoffId);
```

**Behavior:**
- Creates the execution and memoizes its ID under the step name in one statement, so retries
  and resumes return the same ID
- The execution is independent: this task does not wait for it, its failure does not fail this
  task, and cancelling this task does not cancel it
- With `cancelWithParent: true`, the execution is cancelled with this task's reason when this task
  is cancelled or fails permanently, in any state and transitively for executions it started the
  same way. One that completed first stays completed. The link is set only when this call starts
  the execution
- `dedupe_key` defaults to a key derived from this execution and the step name. Pass your own to
  reconnect to an execution: if one of the task already holds the key, its ID is returned and
  nothing is started, superseded or updated
- The execution inherits `ctx.metadata` unless `metadata` overrides it
- Wait for it with [`ctx.waitForResult()`](#ctxwaitforresult) or an `{ execution: id }` branch of
  `ctx.waitForAny()`, or read it by ID with `conductor.getExecution()`

## ctx.waitForResult()

Wait until an execution settles and return its outcome as a value:

```typescript
const outcome = await ctx.waitForResult(
  stepKey: string,
  taskRef?: { name: string, queue?: string },  // optional, types the result
  executionId: string,
  options?: { timeout?: DurationInput }
): Promise<
  | { status: "completed", result: TResult }
  | { status: "failed", error: string }
  | { status: "cancelled", reason: string }
  | { status: "timeout" }
>
```

**Example:**

```typescript
const id = await ctx.invoke("start-tool", { name: "run-tool" }, input);
await ctx.step("announce", () => postMessage("Tool started"));

const outcome = await ctx.waitForResult("tool-result", { name: "run-tool" }, id, { timeout: "24h" });
if (outcome.status === "completed") {
  // outcome.result
} else if (outcome.status === "timeout") {
  // the tool is still running
}
```

**Behavior:**
- Works for any execution ID, typically one started with `ctx.invoke()` or `conductor.invoke()`.
- The task suspends until the execution settles. Whatever ends the execution wakes it in the
  same transaction: a completion, a failure after the last retry, a cancellation while pending
  or running, being superseded by a reinvoke with the same `dedupe_key`, and the removal of a
  pending schedule execution (`cancelled` with reason `Schedule removed`). Dropping the queue
  of the execution does not wake it.
- A failed or cancelled execution never fails the waiting task; the outcome is the value.
  `failed` carries the last error and `cancelled` the cancellation reason.
- An execution that already settled returns at once without suspending.
- The timeout starts at the first call and resolves to `{ status: "timeout" }`. The execution
  keeps running.
- The outcome is memoized by the step key, so retries and resumes return it even after the
  execution has been removed by retention. It is a `ctx.waitForAny()` with one branch named
  `result`, so it also uses the step key `<stepKey>:result`.
- Throws if the execution does not exist when the wait starts, including one already removed by
  retention. A wait that started before the removal still receives the outcome.
- With a task, the result is typed by its return type and an execution of another task is not
  found. Without one, `ctx.waitForResult<TResult>(stepKey, executionId)` types the result as
  `TResult`. Neither validates the result.

## ctx.onSignal()

Listen for signals by name. Signals stored before the handler started are delivered first, in order. A signal handler that throws fails the attempt. See [Signals](../task-execution/signals.md).

```typescript
ctx.onSignal("input", async (payload, { id, name }) => {
  await handleInput(id, payload);
});
```

## ctx.sendSignal()

Send a signal to an execution. Idempotent by signal id. Returns `false` if the execution is completed, failed, cancelled or unknown.

```typescript
await ctx.sendSignal(executionId, { name: "input", id: "report-1", payload });
```

## ctx.signalWithStart()

Like `conductor.signalWithStart()`. Sends the signal and memoizes the execution ID under the step key in one statement, so retries and resumes return that ID, also after the execution completed. An execution it starts inherits this execution's metadata.

```typescript
const sessionId = await ctx.signalWithStart("start-session", {
  task: "session",
  key: threadKey,
  signal: { name: "input", id: messageId, payload },
});
```

## ctx.checkpoint()

Stop the handler here if `ctx.signal` is aborted or the task is outside its time window:

```typescript
await ctx.checkpoint(): Promise<void>
```

**Example:**

```typescript
const task = conductor.createTask(
  { name: "delete-old-data", window: ["02:00", "04:00"] },
  { invocable: true },
  async (event, ctx) => {
    let hasMore = true;

    while (hasMore) {
      hasMore = await deleteBatch(1000); // safe to repeat

      await ctx.checkpoint(); // stop here on shutdown or when the window closes
    }
  }
);
```

**Behavior:**
- Returns at once while `ctx.signal` is not aborted and the task is inside its time window
- Otherwise stops the handler. On shutdown or outside the window, the execution is released and
  runs again later; on cancellation or timeout, it ends like any cancelled or timed-out attempt
- Saves nothing: a released execution runs its handler again from the start, so the work since
  the last saved step repeats. Use `ctx.step()` for results that must not be recomputed
- `ctx.step()` checks the same before each step

## ctx.schedule()

Dynamically schedule a cron task. Not memoized: a retried or resumed attempt that reaches it
schedules again, which replaces the pending execution of `scheduleName`:

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

Remove a dynamic cron schedule. Not memoized: a retried or resumed attempt that reaches it removes
the schedule again:

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

## ctx.queue

Queue of the current execution:

```typescript
ctx.queue: string
```

Batch handlers have no `ctx.executionId` or `ctx.queue`; each event carries `event.execution.id`
and `event.execution.queue`.

## Custom Context

You can extend the task context with your own services and utilities.

See [Custom Context](../crafting-tasks/custom-context.md) for details.

## What's Next?

- [Custom Context](../crafting-tasks/custom-context.md) - Extend context with custom services
- [Conductor API](conductor.md) - Task creation and invocation
- [Orchestrator API](orchestrator.md) - Worker management
