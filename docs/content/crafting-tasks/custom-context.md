# Custom Context

Extend the task context with your own utilities, services, or configuration.

## Adding Custom Context

Pass custom properties when creating the Conductor:

```typescript
import { EmailService } from "./services/email";
import { S3Client } from "./services/storage";

const conductor = Conductor.create({
  sql,
  tasks: TaskSchemas.fromSchema([myTask]),
  context: {
    email: new EmailService(),
    storage: new S3Client(),
    apiKey: process.env.API_KEY,
  },
});
```

## Using Custom Context

Access custom context in your task handlers:

```typescript
const sendWelcomeEmail = conductor.createTask(
  { name: "send-welcome" },
  { invocable: true },
  async (event, ctx) => {
    const { email, name } = event.payload;

    // Use custom email service
    await ctx.email.send({
      to: email,
      subject: "Welcome!",
      body: `Hello, ${name}!`,
    });

    // Use custom storage
    const template = await ctx.storage.get("templates/welcome.html");

    ctx.logger.info(`Sent welcome email to ${email}`);
  }
);
```

TypeScript infers the context type automatically.

## Middleware

`context` is created once and shared by every execution. When a handler needs objects built per execution, such as a client for the current tenant, add middleware. Middleware runs before the handler on every attempt, including each resume after `ctx.sleep()`, `ctx.waitForEvent()` and `ctx.invoke()`:

```typescript
import { AsyncLocalStorage } from "node:async_hooks";

const requests = new AsyncLocalStorage<{ executionId: string }>();

const conductor = Conductor.create({
  sql,
  tasks: TaskSchemas.fromSchema([myTask]),
  context: { db },
  middleware: [
    async ({ execution, ctx }, next) => {
      const tenant = await ctx.db.tenantFor(execution.id);
      return requests.run({ executionId: execution.id }, () => next({ ...ctx, tenant }));
    },
  ],
});

conductor.createTask({ name: "my-task" }, { invocable: true }, async (event, ctx) => {
  ctx.tenant; // typed
});
```

Each middleware receives the execution and the task context, and returns the result of `next`. The object passed to `next` is merged into the task context, and its keys are added to the handler's `ctx` type. Middleware runs in array order, so later middleware sees what earlier middleware added. Its `ctx` parameter is typed with the base context only.

`execution` contains:

- `id`, `task_key`, `queue`, `parent_execution_id`
- `metadata`: the [execution metadata](execution-metadata.md), typed by the metadata schema and `undefined` when the execution has none
- `attempt`: the retry attempt, starting at 1
- `resumed`: `true` when earlier attempts left durable progress: a persisted step (including sleeps, received events and child results), a pending event wait, or a pending child invocation. A retry after a failure is `resumed` only if the failed attempt persisted a step.

`next` settles when the attempt ends. It resolves when the handler returns, suspends (sleep, event wait, child invocation), is cancelled or times out, and rejects when the handler throws, so `try`/`finally` cleanup runs for every attempt. Use `ctx.signal.aborted` to tell these apart from a completed handler. An abort always wins: whatever middleware returns or throws after the attempt was suspended, cancelled or timed out is ignored.

An error thrown by middleware fails the attempt like a handler error, and the execution is retried.

Middleware does not run for batch tasks, which receive neither custom context nor middleware, or for the internal maintenance and event dispatch tasks.

## What's Next?

- [Testing](testing.md) - Test tasks with custom context
- [API Reference: Conductor](../api/conductor.md) - Full Conductor API
