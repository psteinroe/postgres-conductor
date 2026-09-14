# FIFO execution

Set `fifo: true` on a task to serialize its executions in strict enqueue order:

```ts
const updateAccount = conductor.createTask(
  { name: "update-account", fifo: true },
  { invocable: true },
  async (event, ctx) => { /* ... */ },
);
```

FIFO is scoped to `(queue, task)`. It uses a durable database lane owner, so only
one execution can be active at a time across all workers. The owner remains with
the execution during retries, sleeps, child waits, and `waitForEvent` waits, and
is released on completion, cancellation, or permanent failure. A worker crash
therefore resumes the owner before admitting its successor.

Priorities are ignored for FIFO tasks. A future, never-started scheduled or
delayed execution does not block currently runnable work; once selected, it
retains the lane even when it becomes delayed. Different FIFO tasks (and queue
identities) run independently.

FIFO cannot be combined with `concurrency` or `groupConcurrency`:

```ts
{ name: "invalid", fifo: true, concurrency: 2 }
```

Use soft `concurrency` or `groupConcurrency` instead when strict ordering is
not required.
