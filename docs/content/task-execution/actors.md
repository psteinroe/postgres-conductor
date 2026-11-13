# Actors

An actor is a task execution addressed by a key that lives on: it sleeps between activations, a signal or an alarm starts its next activation on any worker, and it completes only when it ends itself with `ctx.complete()`. Use one for something that receives messages over a long time and holds state while it works, such as a chat session.

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
- The execution, its id and its key stay the same across activations, so `signalWithStart()` keeps addressing it and `conductor.signal()` reaches it by id or by key
- An activation that returns does not wait for signals it has read but not delivered: they start the next activation at once
- What an activation returns is ignored, and its `returns` schema is not checked. An actor completes with a result only through [`ctx.complete()`](#ending-an-actor)

## Going Idle

An activation usually opens something, such as a session, that its signal handlers use. Before it closes that, it must know that no handler is running or about to run. `ctx.untilIdle(fn)` gives it that point:

- It calls `fn` and, once `fn` resolves, checks whether a signal handler started meanwhile or a signal already read still waits for its handler. If so, it waits for them and calls `fn` again
- Otherwise it stops delivering signals to the activation and returns. Signals stored later stay undelivered and start the next activation
- `fn` decides what idle means, for example "no work in progress"

`ctx.untilIdle()` works in any task. In a task that is not an actor, signals stored after it returned re-queue the execution when it completes.

## Alarms

`ctx.setAlarm(at)` wakes the actor at `at` once the activation returned, unless a signal wakes it first. Each activation sets its own alarm; one that sets none sleeps until a signal. An alarm in the past starts the next activation at once.

An alarm only wakes an actor that sleeps. For a time that should reach the actor also while an activation runs, such as an expiry, send it a [delayed signal](signals.md#delayed-signals): it reaches a running activation's handler when due, and wakes a sleeping actor at that time, or at its alarm if that is earlier. A fixed signal id per time makes a repeated send harmless:

```typescript
ctx.onSignal("expire", () => session.stop());
await ctx.sendSignal(
  ctx.executionId,
  { name: "expire", id: `expire:${expiresAt.toISOString()}` },
  { at: expiresAt },
);
```

## State

An actor can tell others what it is doing with [`ctx.setState()`](../api/task-context.md#ctxsetstate), for example its status. Others read it with `conductor.getExecution()` and `listExecutions()`. The state stays across activations, and `ctx.state` reads it at the start of the next one. `signalWithStart({ state })` sets it when it starts the actor.

Declare a [state schema](../crafting-tasks/defining-tasks.md#execution-state) on the task to type `ctx.state` and validate every write. When several handlers of an activation change the state at once, for example signal handlers and tool calls, [`ctx.updateState()`](../api/task-context.md#ctxupdatestate) applies their changes one after another, so none is lost:

```typescript
ctx.onSignal("topic", async (topic) => {
  await ctx.updateState((state) => ({
    status: "busy",
    topics: [...(state?.topics || []), String(topic)],
  }));
});
```

## Events

An actor receives the events it [listens](../crafting-tasks/triggers.md#listening-for-events) for as signals, which wake it like any other. Listens last across activations, so an actor listens once, for example in its first activation or with `signalWithStart({ listen })`. A handler learns which of its listens an event matched from `listens` in its second argument. An [exclusive listen](../crafting-tasks/triggers.md#exclusive-listens) makes an actor the only one of its task to follow, for example, a conversation.

## Ending an Actor

An activation ends its actor with `ctx.complete(result)`. The execution completes for good with `result`, typed and validated by the task's `returns` schema like a returned result, so `conductor.waitForResult()`, `ctx.waitForResult()` and `ctx.invokeAndWait()` receive it:

```typescript
ctx.onSignal("stop", async (payload) => {
  await ctx.complete({ summary: String(payload) });
});
```

- It hangs up the activation at once, also from a signal handler or while `ctx.untilIdle()` waits; steps still running save first
- Nothing re-opens the execution: signals stored meanwhile are not delivered, later ones are refused (`conductor.signal()` returns `false`), its [listens](../crafting-tasks/triggers.md#listening-for-events) end, and the next `signalWithStart()` for its key starts a new execution
- A task that is not an actor can complete the same way, for example from a signal handler

## Retries and Failures

- A failed activation retries with the usual [backoff](retries.md). Its signals stay undelivered, so the retry receives them again
- `maxAttempts` counts failed activations in a row: an activation that returns resets the count
- When an actor fails for good, or is [cancelled](cancellation.md), its execution ends and the next `signalWithStart()` for its key starts a new one

## Steps

Steps are memoized per execution, and an actor's execution spans all its activations. A step key used in one activation returns its stored result in every later one, so key steps per activation, for example by the id of the signal that the step handles.
