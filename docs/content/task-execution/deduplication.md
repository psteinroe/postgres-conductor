# Deduplication

Keep one execution per key.

## Basic Usage

Invoking a task again with the same `dedupe_key` updates the pending execution instead of creating a new one:

```typescript
const first = await conductor.invoke(
  { name: "process-order" },
  { orderId: "123", status: "pending" },
  { dedupe_key: "order-123" }
);

const second = await conductor.invoke(
  { name: "process-order" },
  { orderId: "123", status: "paid" },
  { dedupe_key: "order-123" }
);

// first === second, and the execution runs once with status "paid"
```

## Duplicate Keys

What a repeated invoke does depends on the execution that holds the key:

| Existing execution | Result |
|--------------------|--------|
| Waiting to run | Its payload, `run_at`, `priority` and `group` are replaced. The same ID is returned. A sleeping execution resumes right away with the new payload. |
| Running | It is superseded: marked failed with `superseded by reinvoke`, and a new execution is created with the new payload. The running handler is not stopped, but its result is discarded. |
| Completed or failed | Nothing runs. Its stored payload is replaced and the same ID is returned. |

A key stays taken as long as its execution is stored. Once [retention](../crafting-tasks/retention.md) removes a completed or failed execution, the next invoke with that key creates a new execution.

## Deduplication Scope

Deduplication is per-task and per-queue:

```typescript
// These don't conflict (different tasks)
await conductor.invoke(
  { name: "task-a" },
  {},
  { dedupe_key: "key1" }
);

await conductor.invoke(
  { name: "task-b" },
  {},
  { dedupe_key: "key1" } // Same key, different task - OK
);
```

## Use Cases

**Ignore redelivered webhooks:**

```typescript
await conductor.invoke(
  { name: "process-webhook" },
  event,
  { dedupe_key: event.id }
);
```

A redelivery after the webhook was processed is ignored. A redelivery that arrives while it is still running starts a new execution, so the handler can still run twice. Steps do not help here, because each execution has its own steps. Make the side effects idempotent, for example with an idempotency key derived from `event.id`.

## Throttle and Debounce

`throttle` and `debounce` deduplicate per time slot: fixed windows of `seconds` length, aligned to the Unix epoch. With a `dedupe_key`, each key has its own slots; without one, all invokes of the task share them.

```typescript
await conductor.invoke({ name: "sync-customer" }, { customerId }, {
  dedupe_key: customerId,
  throttle: { seconds: 60 },
});
```

- `throttle`: the first invoke in a slot creates an execution. Later invokes in that slot return `null` while it is pending or running; it is never superseded.
- `debounce`: the execution runs at the start of the next slot. Later invokes before then replace its payload and return the same ID.

A slot is free again once its execution completed, failed or was cancelled. Keys of throttled and debounced invokes are separate from the same key used without them: an ordinary invoke never updates or supersedes a throttled or debounced execution, and a throttled or debounced execution that is still stored does not block its key in a later slot.

## Events

`conductor.emit()` and `ctx.emit()` take the same `dedupe_key` option:

```typescript
await conductor.emit("order.paid", { orderId: "123" }, { dedupe_key: "order-123" });
```

Unlike invoke, a repeated emit never updates anything: it emits nothing and returns the original event ID, even while the event is still being dispatched. Keys are scoped per event name and remembered for 1–2 days after dispatch. See [Deduplicating Emits](../crafting-tasks/triggers.md#deduplicating-emits).

**Push back a delayed execution:**

```typescript
// Each invoke moves the reminder to 24 hours from now
await conductor.invoke(
  { name: "send-reminder" },
  { userId },
  {
    dedupe_key: `reminder-${userId}`,
    run_at: new Date(Date.now() + 24 * 60 * 60 * 1000),
  }
);
```

## What's Next?

- [Batching](batching.md) - Bulk task invocation
- [Priority](priority.md) - Control execution order
