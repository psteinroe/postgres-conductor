# Signals

Send data to a running execution, or start one for a key when none is running.

## Listening for Signals

A handler listens for signals by name with `ctx.onSignal()`:

```typescript
const session = conductor.createTask(
  { name: "session" },
  { invocable: true },
  async (event, ctx) => {
    ctx.onSignal("input", async (payload, { id }) => {
      // delivery is at least once: dedupe by id
      if (await alreadyHandled(id)) return;
      await handleInput(payload);
    });

    await runUntilIdle();
  },
);
```

The second argument of a handler holds the signal's `id`, its `name` and `listens`: for a signal that delivers an event the execution [listens](../crafting-tasks/triggers.md#listening-for-events) for, the names of its listens the event matched, in the order they were created; empty for any other signal.

- Signals stored before the handler started are delivered first, in the order they were sent. The worker reads them when it claims the execution, so a listener receives them as soon as it registers
- Later signals are delivered while the handler runs. The worker reads them with its next claim, so they arrive within about one `pollIntervalMs`
- Handlers run one at a time. The payload is any JSON value
- A signal counts as delivered once its handler returns
- Register listeners before the handler awaits anything. A signal reached while its name has no listener is skipped; if a listener for it registers later in the attempt, the execution is re-queued and the next attempt receives it
- One listener per name and attempt. The handler re-runs from the top on every attempt, so register listeners again each time

## Waiting for a Signal

An execution that has nothing to do can suspend until its next signal instead of ending, with a `{ signal: name }` branch of [`ctx.waitForAny()`](../api/task-context.md#ctxwaitforany). It can wait for other branches, such as a tool execution, at the same time:

```typescript
const session = conductor.createTask(
  { name: "session" },
  { invocable: true },
  async (event, ctx) => {
    ctx.onSignal("input", (payload, { id }) => saveInput(id, payload));
    const { id: toolId } = await ctx.invoke("start-tool", { name: "run-tool" }, event.payload);

    for (let n = 0; ; n++) {
      await processSavedInputs(ctx, n);
      const winner = await ctx.waitForAny(
        `idle:${n}`,
        { input: { signal: "input" }, tool: { execution: toolId } },
        { timeout: "1h" },
      );
      if (winner.key === "timeout") return;
    }
  },
);
```

- The branch wins at once when the execution holds a signal with that name whose handler has not returned, including one stored before the wait. Otherwise the next signal with that name, from `conductor.signal()`, `ctx.sendSignal()` or `signalWithStart()`, wakes the execution
- `winner.signal` holds the signal's `id`, `name` and `payload` (`null` when it was sent without one). The winner is memoized under the step key, so a replay returns the same signal and payload
- The wait does not consume the signal. The next attempt delivers it to `ctx.onSignal()` handlers as usual
- Signals for other names are kept and do not wake the execution
- Annotate a branch as `SignalBranch<T>` to type `winner.signal.payload`; like the type of a `ctx.onSignal<T>()` handler, it is not checked. An unannotated branch's payload is a `JsonValue`

Since the payload is memoized with the winner, the signal itself can carry what the execution waits for. An approval request that waits for the first decision, a withdrawal or its expiry does not have to persist decisions from a handler:

```typescript
type Decision = { status: "approved" | "rejected"; comment?: string; by: string };

const approval = conductor.createTask(
  { name: "approval" },
  { invocable: true },
  async (event, ctx) => {
    ctx.onSignal("decision", () => {});
    ctx.onSignal("withdrawn", () => {});
    const decision: SignalBranch<Decision> = { signal: "decision" };

    const winner = await ctx.waitForAny(
      "decided",
      { decision, withdrawn: { signal: "withdrawn" } },
      { timeout: "7d" },
    );
    if (winner.key === "decision") {
      const { status, comment, by } = winner.signal.payload;
      await ctx.step("record", () => recordDecision(event.payload.requestId, status, comment, by));
    }
  },
);

// From the Slack and Linear webhooks: a fixed signal id per request makes the first decision win
await conductor.signal(approvalExecutionId, {
  name: "decision",
  id: `decision:${requestId}`,
  payload: { status: "approved", comment: "ship it", by: userId },
});
```
- A signal stays undelivered until a handler for its name returns, so register a handler for every signal branch. Without one, the signal wins every later wait at once

## Sending Signals

Signal a known execution:

```typescript
const stored = await conductor.signal(executionId, {
  name: "input",
  id: requestId,
  payload: { text: "hello" },
});
// false if the execution is completed, failed, cancelled or unknown
```

Each signal has an `id`. Sending an id again stores it once, so a retried send is safe.

### Delayed Signals

With `at`, a signal is stored now and delivered at that time:

```typescript
await conductor.signal(
  executionId,
  { name: "expire", id: `expire:${expiresAt.toISOString()}` },
  { at: expiresAt },
);
```

- Before `at`, no handler receives it and it wakes nothing. A running execution receives it with the worker's first poll after `at`; an idle [actor](actors.md) wakes at `at`, or at its alarm if that is earlier; an execution waiting in `ctx.waitForAny()` with a branch for its name wakes at `at` and the signal wins
- A time that passed delivers it at once
- It is deduplicated by id like any signal, and `ctx.sendSignal()` takes the same option
- An execution that completes before `at` drops it, like any undelivered signal
### By Key

An execution started by [`signalWithStart()`](#signal-or-start) holds its key while it is pending or running. Signal it by its task and key, without reading its ID first:

```typescript
const stopped = await conductor.signal(
  { task: { name: "agent-worker" }, key: workerId },
  { name: "stop", id: stopRequestId },
);
// false if no pending or running execution of the task holds the key
```

- It starts nothing. A completed, failed or cancelled execution no longer holds its key, so the call returns `false`
- The key is scoped to the task and its queue (`task: { name, queue }`, `queue` defaults to `"default"`)
- The lookup and the signal run in one transaction under the lock `signalWithStart()` takes on the task and key. A signal sent while `signalWithStart()` starts the execution for the key waits for it and reaches the new execution

`conductor.getExecution({ task, key })` reads the execution that holds a key.

## Signal or Start

`signalWithStart()` makes a task execution addressable by a key. It signals the execution of the task for that key, or starts one that receives the signal first:

```typescript
const { id, started } = await conductor.signalWithStart({
  task: "session",
  key: "slack:T1:C1:1712345678.000100",
  signal: { name: "input", id: requestId, payload },
  input: {}, // payload of the execution, used only when it starts one (default: {})
});
// started: whether this call started the execution
```

- At most one non-terminal execution exists per task and key. Concurrent calls for a key start one execution, and exactly one of them returns `started: true`
- A completed, failed or cancelled execution does not hold its key, so the next call starts a new execution
- The call runs in one transaction under a lock on the task and key

An execution that should live on for its key, sleeping between messages, is an [actor](actors.md).

## Sending from Tasks

```typescript
await ctx.sendSignal(executionId, { name: "input", id: "report-1", payload });
await ctx.sendSignal({ task: { name: "session" }, key: threadKey }, { name: "stop", id: "stop-1" });

// memoized by the step key like other context operations, with whether it started the execution
const { id: sessionId, started } = await ctx.signalWithStart("start-session", {
  task: "session",
  key: threadKey,
  signal: { name: "input", id: messageId, payload },
});
```

`ctx.signalWithStart()` passes the metadata of the calling execution to an execution it starts, unless its `metadata` option overrides it. `ctx.sendSignal()` starts nothing and takes no metadata.

## Delivery Guarantees

Delivery is **at least once per signal id**. A handler may see a signal again after a crash or a failed attempt, so make handlers idempotent by `id`.

No signal is lost when the handler returns:

- **Completion:** when the handler returns, the attempt first delivers the signals it has already read to their listeners. Settlement then checks, in the same transaction, for signals the attempt listens for but did not deliver, which are ones stored after the worker last read them. If there are any, the execution is not completed but re-queued, and the new attempt receives them when it starts. Its result is discarded and the re-run does not count as an attempt
- **Signals with no listener:** a signal whose name the attempt never listened for does not re-queue it. The execution completes and the signal is not delivered
- **Delayed signals** not yet due do not re-queue it either
- **`ctx.complete()`:** an execution that completes itself is never re-queued; its undelivered signals are dropped
- **Retries:** a failed attempt delivers its remaining signals to the next attempt
- **Sleeping or waiting:** signals for an execution that sleeps, waits for a child or waits for an event are kept and delivered when its handler next runs. A signal does not wake it, unless it waits in `ctx.waitForAny()` with a signal branch for its name
- **Pending executions** receive their signals when they first run
- **Cancellation and permanent failure:** undelivered signals are dropped. Signalling a cancelled execution returns `false`

When the handler returns while signal handlers are still running or signals it already read are waiting for them, the attempt waits for them before settling. `timeoutMs` and cancellation still apply while it waits, and a signal handler that suspends, for example with `ctx.sleep()`, suspends the attempt. Signals that arrive after the handler returned are left to the re-queued attempt.

A handler does not have to wait for its signals: one that registers listeners and returns right away still receives every signal stored before it started. Re-queues are not limited, because each one needs a signal stored after the previous attempt started, and the next attempt receives that signal when it starts. An execution therefore re-runs only while new signals keep arriving. To handle a stream of signals in one attempt, keep the handler running, for example until it has been idle for a while.

## Errors

A signal handler that throws fails the attempt like an uncaught error in the handler, with the error `Signal handler for "<name>" failed: <message>`. The signal is not marked delivered, so the retry receives it again. During `orchestrator.stop()`, a signal handler that throws, or a signal that cannot be marked delivered, releases the attempt once the handler returns, without using up a retry.
