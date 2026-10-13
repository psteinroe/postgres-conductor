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

A redelivery after the webhook was processed is ignored. A redelivery that arrives while it is still running starts a new execution, so the handler can still run twice. Wrap side effects in [steps](../crafting-tasks/retries-and-steps.md) or make them idempotent.

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

- [Rate Limiting](rate-limiting.md) - Throttle and debounce task execution
- [Batching](batching.md) - Bulk task invocation
- [Priority](priority.md) - Control execution order
