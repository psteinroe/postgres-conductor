# Retries and Steps

When tasks fail and retry, code without steps re-executes from the start. This guide shows how to handle retries safely using steps and sleep.

## The Problem: Code Re-executes on Retry

Without steps, your entire task re-runs on every retry. This causes duplicate side effects:

```typescript
// ❌ Problem: Charges customer twice on retry
const processOrder = conductor.createTask(
  { name: "process-order" },
  { invocable: true },
  async (event, ctx) => {
    // Charge payment
    const charge = await chargeCustomer(event.payload.amount);

    // Send email
    await sendConfirmation(event.payload.email);

    // Update database (fails here!)
    await database.save({ orderId: event.payload.id, chargeId: charge.id });
  }
);
```

**What happens:**

1. First attempt: Customer charged, email sent, database save fails
2. Retry: Customer charged **again**, email sent **again**

This is bad! The customer was double-charged.

## The Solution: Wrap Side Effects in Steps

Steps cache their results. On retry, cached steps return their saved result without re-executing:

```typescript
// ✅ Solution: Safe retries with steps
const processOrder = conductor.createTask(
  { name: "process-order" },
  { invocable: true },
  async (event, ctx) => {
    // Charge payment (runs once, cached)
    const charge = await ctx.step("charge-customer", async () => {
      return await chargeCustomer(event.payload.amount);
    });

    // Send email (runs once, cached)
    await ctx.step("send-confirmation", async () => {
      await sendConfirmation(event.payload.email);
    });

    // Update database (retries only this step if it fails)
    await ctx.step("save-order", async () => {
      await database.save({ orderId: event.payload.id, chargeId: charge.id });
    });

    return { orderId: event.payload.id, chargeId: charge.id };
  }
);
```

**What happens now:**

1. First attempt: Customer charged, email sent, database save fails
2. Retry: Steps 1 and 2 return cached results, only step 3 re-runs

The customer is charged once, email sent once.

## Working with Loops

Loops without steps re-process all items on retry:

```typescript
// ❌ Problem: Re-processes all orders on retry
const processBatch = conductor.createTask(
  { name: "process-batch" },
  { invocable: true },
  async (event, ctx) => {
    for (const orderId of event.payload.orderIds) {
      await processOrder(orderId);
    }
  }
);
```

**What happens:**

- Batch of 100 orders
- Fails on order #50
- Retry processes orders 1-100 again (50 duplicates!)

**Solution:** Wrap each iteration in a step:

```typescript
// ✅ Solution: Each item processes once
const processBatch = conductor.createTask(
  { name: "process-batch" },
  { invocable: true },
  async (event, ctx) => {
    for (const orderId of event.payload.orderIds) {
      await ctx.step(`process-${orderId}`, async () => {
        await processOrder(orderId);
      });
    }
  }
);
```

**What happens now:**

- Orders 1-49 return cached results
- Only order 50+ run (or retry if they failed)

## Dynamic Step Names

Use unique step names for dynamic data:

```typescript
const syncUsers = conductor.createTask(
  { name: "sync-users" },
  { invocable: true },
  async (event, ctx) => {
    const users = await ctx.step("fetch-users", async () => {
      return await api.getUsers();
    });

    // Create step per user
    for (const user of users) {
      await ctx.step(`sync-user-${user.id}`, async () => {
        await syncToDatabase(user);
      });
    }
  }
);
```

Each user gets their own cached step.

## Sleep for Delays

Sleep pauses execution without blocking the worker:

```typescript
const reminderWorkflow = conductor.createTask(
  { name: "send-reminder" },
  { invocable: true },
  async (event, ctx) => {
    // Send initial email
    await ctx.step("send-welcome", async () => {
      await sendEmail(event.payload.email, "Welcome!");
    });

    // Wait 24 hours (worker is freed, not blocked)
    await ctx.sleep("wait-day", 24 * 60 * 60 * 1000);

    // Send reminder after 24 hours
    await ctx.step("send-reminder", async () => {
      await sendEmail(event.payload.email, "Don't forget...");
    });
  }
);
```

**How it works:**

- Task runs `send-welcome` step
- Task pauses at `ctx.sleep()` and releases the worker
- After 24 hours, task resumes
- `send-welcome` returns cached result (doesn't re-send email)
- `send-reminder` runs for first time

## Step Names Must Be Unique

Each step needs a unique name within a task:

```typescript
// ✅ Good - unique names
const task = conductor.createTask(
  { name: "process" },
  { invocable: true },
  async (event, ctx) => {
    const user = await ctx.step("fetch-user", () => getUser());
    const orders = await ctx.step("fetch-orders", () => getOrders());
    return { user, orders };
  }
);

// ❌ Bad - duplicate names
const bad = conductor.createTask(
  { name: "bad-task" },
  { invocable: true },
  async (event, ctx) => {
    const user = await ctx.step("fetch", () => getUser());
    const orders = await ctx.step("fetch", () => getOrders()); // Duplicate!
    return { user, orders };
  }
);
```

Duplicate step names will cause the second step to return the first step's cached result.

## What's Next?

- [Child Invocation](child-invocation.md) - Call other tasks and wait for results
- [Long-Running Tasks](long-running.md) - Handle tasks that run for extended periods
- [Retries & Backoff](../task-execution/retries.md) - Configure retry behavior
