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

Steps save their results. On retry, a step with a saved result returns it without running its callback again:

```typescript
// ✅ Solution: Safe retries with steps
const processOrder = conductor.createTask(
  { name: "process-order" },
  { invocable: true },
  async (event, ctx) => {
    // Charge payment (skipped on retry once its result is saved)
    const charge = await ctx.step("charge-customer", async () => {
      return await chargeCustomer(event.payload.amount, {
        idempotencyKey: `order-${event.payload.id}`,
      });
    });

    // Send email (skipped on retry once its result is saved)
    await ctx.step("send-confirmation", async () => {
      await sendConfirmation(event.payload.email, {
        idempotencyKey: `order-${event.payload.id}`,
      });
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
2. Retry: Steps 1 and 2 return their saved results, only step 3 re-runs

The retry does not charge the customer or send the email again. The idempotency keys cover the case steps cannot, described next.

## What Steps Guarantee

A step saves its result after its callback returns. Once saved, the result is never recomputed: retries and resumes return it without calling the callback.

Until the result is saved, the step has no result, and the next attempt runs the callback again. This happens when:

- The callback throws: the attempt fails and nothing is saved
- The attempt ends between the side effect and the save, for example because the worker crashes, loses its database connection or times out

Steps are therefore at-least-once around their callback. For side effects outside your database, such as charges and emails, pass an idempotency key that stays the same across attempts, derived from the payload or from `ctx.executionId` and the step name, so the external service ignores a repeat.

The handler always runs from the start: on every retry and every resume after `ctx.sleep()` or a wait, code outside steps runs again.

Steps can run concurrently, for example with `Promise.all()`. When one of them throws or the handler hangs up in another (`ctx.sleep()`, a wait, or `orchestrator.stop()`), the attempt ends only after the callbacks still running return and save their results, or once `timeoutMs` elapses. A timeout or cancellation ends the attempt at once.

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
// ✅ Solution: Each item is skipped once its step is saved
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

- Orders 1-49 return saved results
- Only orders 50+ run. Order 50 runs again from the start, so `processOrder()` must tolerate a repeat

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

Each user gets their own step.

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
- The handler runs again from the start: `send-welcome` returns its saved result (doesn't re-send email)
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

Duplicate step names will cause the second step to return the first step's saved result.

## What's Next?

- [Child Invocation](child-invocation.md) - Call other tasks and wait for results
- [Long-Running Tasks](long-running.md) - Handle tasks that run for extended periods
- [Retries & Backoff](../task-execution/retries.md) - Configure retry behavior
