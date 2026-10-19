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

- Signals stored before the handler started are delivered first, in the order they were sent. The worker reads them when it claims the execution, so a listener receives them as soon as it registers
- Later signals are delivered while the handler runs. The worker reads them on its poll loop, so they arrive within about one `pollIntervalMs`
- Handlers run one at a time. The payload is any JSON value
- A signal counts as delivered once its handler returns
- A signal whose name has no listener yet is kept until a listener for it registers in the same attempt
- One listener per name and attempt. The handler re-runs from the top on every attempt, so register listeners again each time

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

## Signal or Start

`signalWithStart()` makes a task execution addressable by a key. It signals the execution of the task for that key, or starts one that receives the signal first:

```typescript
const executionId = await conductor.signalWithStart({
  task: "session",
  key: "slack:T1:C1:1712345678.000100",
  signal: { name: "input", id: requestId, payload },
  input: {}, // payload of the execution, used only when it starts one
});
```

- At most one non-terminal execution exists per task and key. Concurrent calls for a key start one execution
- A completed, failed or cancelled execution does not hold its key, so the next call starts a new execution
- The call runs in one transaction under a lock on the task and key

## Sending from Tasks

```typescript
await ctx.sendSignal(executionId, { name: "input", id: "report-1", payload });

// memoized by the step key like other context operations
const sessionId = await ctx.signalWithStart("start-session", {
  task: "session",
  key: threadKey,
  signal: { name: "input", id: messageId, payload },
});
```

`ctx.signalWithStart()` passes the metadata of the calling execution to an execution it starts.

## Delivery Guarantees

Delivery is **at least once per signal id**. A handler may see a signal again after a crash or a failed attempt, so make handlers idempotent by `id`.

No signal is lost when the handler returns:

- **Completion:** when the handler returns, the attempt first delivers the signals it has already read to their listeners. Settlement then checks, in the same transaction, for signals the attempt listens for but did not deliver, which are ones stored after the worker last read them. If there are any, the execution is not completed but re-queued, and the new attempt receives them when it starts. Its result is discarded and the re-run does not count as an attempt
- **Signals with no listener:** a signal whose name the attempt never listened for does not re-queue it. The execution completes and the signal is not delivered
- **Retries:** a failed attempt delivers its remaining signals to the next attempt
- **Sleeping or waiting:** signals for an execution that sleeps, waits for a child or waits for an event are kept and delivered when its handler next runs. A signal does not wake it
- **Pending executions** receive their signals when they first run
- **Cancellation and permanent failure:** undelivered signals are dropped. Signalling a cancelled execution returns `false`

When the handler returns while signal handlers are still running or signals it already read are waiting for them, the attempt waits for them before settling. Signals that arrive after the handler returned are left to the re-queued attempt.

A handler does not have to wait for its signals: one that registers listeners and returns right away still receives every signal stored before it started. Re-queues are not limited, because each one needs a signal stored after the previous attempt started, and the next attempt receives that signal when it starts. An execution therefore re-runs only while new signals keep arriving. To handle a stream of signals in one attempt, keep the handler running, for example until it has been idle for a while.

## Errors

A signal handler that throws fails the attempt like an uncaught error in the handler, with the error `Signal handler for "<name>" failed: <message>`. The signal is not marked delivered, so the retry receives it again.
