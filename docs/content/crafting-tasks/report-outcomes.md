# Report an Execution's Outcome to Another Execution

Postgres Conductor has no hook that runs when an execution settles. To tell another execution how one ended, start a small task that waits for the outcome with [`ctx.waitForResult()`](../api/task-context.md#ctxwaitforresult) and reports it with a signal. The examples on this page are compiled and run by `tests/integration/outcome-recipe.test.ts`.

## Supervisor: Report to the Target

A supervisor starts the work, waits for its outcome and sends it to the target with [`ctx.signalWithStart()`](../task-execution/signals.md#signal-or-start):

```typescript
const watchToolCall = conductor.createTask(
  { name: "watch-tool-call" },
  { invocable: true },
  async (event, ctx) => {
    const id = await ctx.invoke("start", { name: "tool-call" }, event.payload);
    const outcome = await ctx.waitForResult("result", { name: "tool-call" }, id);
    await ctx.signalWithStart("report", {
      task: "session",
      key: event.payload.sessionKey,
      signal: {
        name: "tool-settled",
        id: `tool-settled:${id}`,
        payload: { executionId: id, outcome },
      },
    });
  }
);

const session = conductor.createTask(
  { name: "session" },
  { invocable: true },
  async (event, ctx) => {
    ctx.onSignal("tool-settled", async (payload, { id }) => {
      // payload is { executionId, outcome }, and outcome is { status: "completed", result },
      // { status: "failed", error }, { status: "cancelled", reason } or { status: "timeout" }
    });
    // ...
  }
);
```

Invoke the supervisor instead of the work: `conductor.invoke({ name: "watch-tool-call" }, { sessionKey, input })`.

- The outcome is a value: a failed or cancelled execution resolves the wait instead of failing the supervisor. Pass `timeout` to `ctx.waitForResult()` to report `{ status: "timeout" }` and leave the work running
- `ctx.signalWithStart()` signals the session running for the key, or starts one that receives the signal first. A short-lived target, such as a session that finished before the tool did, is started again for the report. Register the listener before the handler awaits anything, as for [any signal](../task-execution/signals.md#listening-for-signals)

## Deterministic Signal IDs

Derive the signal id from the watched execution, as `tool-settled:${id}` does, never from a random value or the time.

`ctx.signalWithStart()` is memoized by its step key, so a supervisor that is retried or resumed after the report does not send it again. `ctx.sendSignal()` is not memoized and runs again on every replay, and a signal id is stored once per execution, so a deterministic id keeps a replayed send from storing a second signal. Delivery to `ctx.onSignal()` is at least once per id either way, so the target dedupes by `id` as well.

## Watcher: React to the Caller

The other direction is an execution that watches its caller and shuts down the work it owns when the caller fails or is cancelled. The work starts the watcher, linked with `cancelWithParent` so the watcher ends with it:

```typescript
const sandbox = conductor.createTask(
  { name: "sandbox" },
  { invocable: true },
  async (event, ctx) => {
    ctx.onSignal("shutdown", async () => {
      await stopSandbox();
    });
    await ctx.invoke(
      "watch-caller",
      { name: "watch-caller" },
      { callerId: event.payload.callerId, workId: ctx.executionId },
      { cancelWithParent: true }
    );
    // ...
  }
);

const watchCaller = conductor.createTask(
  { name: "watch-caller" },
  { invocable: true },
  async (event, ctx) => {
    const { callerId, workId } = event.payload;
    const outcome = await ctx.waitForResult("caller", callerId);
    if (outcome.status === "completed") return;
    await ctx.sendSignal(workId, {
      name: "shutdown",
      id: `shutdown:${callerId}`,
      payload: outcome,
    });
  }
);
```

The caller starts the sandbox with `ctx.invoke("sandbox", { name: "sandbox" }, { callerId: ctx.executionId })`. To stop the work without letting it clean up, call `ctx.cancel(workId, { reason })` instead of sending a signal. Starting the work with [`cancelWithParent`](child-invocation.md#cancelling-with-the-parent) does that without a watcher, but only for work the caller starts itself.

## Race an Execution Against Events

A `{ execution: id }` branch of [`ctx.waitForAny()`](../api/task-context.md#ctxwaitforany) settles with the execution, so the first of an event and an outcome wins. For example, remove an approval card when the deploy ends before anyone decides:

```typescript
const approvalCard = conductor.createTask(
  { name: "approval-card" },
  { invocable: true },
  async (event, ctx) => {
    const { deployId } = event.payload;
    const decided = await ctx.subscribe("decided", {
      event: approvalDecided,
      filter: { executionId: [deployId] },
    });
    const winner = await ctx.waitForAny("decision", {
      decided,
      deploy: { execution: deployId },
    });
    if (winner.key === "deploy") {
      await ctx.step("remove-card", () => removeCard(deployId));
    }
  }
);
```

## Retention

The waiting task needs the watched execution's row, which retention removes. `ctx.waitForResult()` and execution branches of `ctx.waitForAny()` register on the row:

- A wait registered before the execution is removed always receives the outcome, also when the execution is removed at the moment it settles (`removeOnComplete: true`)
- A wait registered after the execution was removed throws `Execution <id> not found`. Uncaught, this fails the attempt like any error, and its retries throw again

[`conductor.waitForResult()`](../api/conductor.md) polls the row instead, so it rejects with `Execution <id> not found` once the execution is removed, even if it started before.

Give the watched task a retention that outlives the wait, or start the wait before the work can finish. The supervisor above calls `ctx.invoke()` and `ctx.waitForResult()` in one attempt, so with `removeOnComplete: true` or `removeOnFail: true` on the work, a tool that finishes in between is removed before the wait registers. With `{ days: n }`, maintenance removes an execution no earlier than `n` days after it settled, which leaves the supervisor ample time.

## Compared to a Settlement Hook

- Every watched call costs one extra execution: the supervisor or watcher
- The report is only as reliable as the supervisor: if it is cancelled or fails permanently and lands in a [dead-letter queue](../task-execution/dead-letter-queue.md), nothing is sent
