# Deduplication

Prevent duplicate executions of the same work.

## Basic Usage

Tasks with the same `dedupe_key` won't create duplicate executions:

```typescript
await conductor.invoke(
  { name: "process-order" },
  { orderId: "123", status: "pending" },
  { dedupe_key: "order-123" }
);

// Returns existing execution ID - doesn't create duplicate
await conductor.invoke(
  { name: "process-order" },
  { orderId: "123", status: "pending" },
  { dedupe_key: "order-123" }
);
```

Both calls return the same execution ID.

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

**Prevent duplicate webhooks:**

```typescript
// Only process each webhook once
await conductor.invoke(
  { name: "process-webhook" },
  event,
  { dedupe_key: event.id }
);
```

**Idempotent operations:**

```typescript
// Ensure user is provisioned exactly once
await conductor.invoke(
  { name: "provision-user" },
  { userId, email },
  { dedupe_key: `user-${userId}` }
);
```

**Combine with delayed execution:**

```typescript
// Deduplicate and delay
await conductor.invoke(
  { name: "send-reminder" },
  { userId },
  {
    dedupe_key: `reminder-${userId}`,
    runAfter: { hours: 24 }
  }
);
```

## What's Next?

- [Rate Limiting](rate-limiting.md) - Throttle and debounce task execution
- [Batching](batching.md) - Bulk task invocation
- [Priority](priority.md) - Control execution order
