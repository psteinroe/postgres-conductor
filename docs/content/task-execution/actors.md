# Actors

An actor is a task execution addressed by a key that lives on: it never completes, it sleeps between activations, and a signal or an alarm starts its next activation on any worker. Use one for something that receives messages over a long time and holds state while it works, such as a chat session.

## Defining an Actor

Set `actor: true` and address it by key with [`signalWithStart()`](signals.md#signal-or-start):

```typescript
const session = conductor.createTask(
  { name: "session", actor: true },
  { invocable: true },
  async (event, ctx) => {
    const session = await openSession(event.payload);
    ctx.onSignal("input", (payload, { id }) => session.deliver(id, payload));

    // returns once the session is idle and no signal handler ran meanwhile
    await ctx.untilIdle(() => session.waitUntilIdle());
    ctx.setAlarm(session.nextWakeAt()); // optional
    await session.close();
  },
);

await conductor.signalWithStart({
  task: "session",
  key: threadKey,
  signal: { name: "input", id: messageId, payload },
});
```

- Each run of the handler is an **activation**. When it returns, the execution does not complete: it sleeps until a signal is stored for it or its alarm is due, then the handler runs again from the start
- The execution, its id and its key stay the same across activations, so `signalWithStart()` keeps addressing it and `conductor.signal()` reaches it by id
- An activation that returns does not wait for signals it has read but not delivered: they start the next activation at once
- An actor returns no result, and its `returns` schema is not checked

## Going Idle

An activation usually opens something, such as a session, that its signal handlers use. Before it closes that, it must know that no handler is running or about to run. `ctx.untilIdle(fn)` gives it that point:

- It calls `fn` and, once `fn` resolves, checks whether a signal handler started meanwhile or a signal already read still waits for its handler. If so, it waits for them and calls `fn` again
- Otherwise it stops delivering signals to the activation and returns. Signals stored later stay undelivered and start the next activation
- `fn` decides what idle means, for example "no work in progress"

`ctx.untilIdle()` works in any task. In a task that is not an actor, signals stored after it returned re-queue the execution when it completes.

## Alarms

`ctx.setAlarm(at)` wakes the actor at `at` once the activation returned, unless a signal wakes it first. Each activation sets its own alarm; one that sets none sleeps until a signal. An alarm in the past starts the next activation at once.

## State

An actor can tell others what it is doing with [`ctx.setState()`](../api/task-context.md#ctxsetstate), for example its status or the topics it follows. Others read it with `conductor.getExecution()` and find actors by it with `conductor.listExecutions({ state })`. The state stays across activations, and `ctx.state` reads it at the start of the next one. `signalWithStart({ state })` sets it when it starts the actor, so the actor is found by it before its first activation.

## Retries and Failures

- A failed activation retries with the usual [backoff](retries.md). Its signals stay undelivered, so the retry receives them again
- `maxAttempts` counts failed activations in a row: an activation that returns resets the count
- When an actor fails for good, or is [cancelled](cancellation.md), its execution ends and the next `signalWithStart()` for its key starts a new one. An activation ends its own actor with `conductor.cancel(ctx.executionId, { reason })`

## Steps

Steps are memoized per execution, and an actor's execution spans all its activations. A step key used in one activation returns its stored result in every later one, so key steps per activation, for example by the id of the signal that the step handles.
