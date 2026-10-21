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

`context` is created once and shared by every execution. When a handler needs objects built per execution, such as a client for the current tenant, add middleware. Middleware runs before the handler on every attempt, including each resume after `ctx.sleep()`, `ctx.waitForEvent()` and `ctx.invokeAndWait()`:

```typescript
const conductor = Conductor.create({
  sql,
  tasks: TaskSchemas.fromSchema([myTask]),
  context: { db },
  middleware: [
    async ({ execution, ctx }, next) => {
      const tenant = await ctx.db.tenantFor(execution.id);
      return next({ ...ctx, tenant });
    },
  ],
});

conductor.createTask({ name: "my-task" }, { invocable: true }, async (event, ctx) => {
  ctx.tenant; // typed
});
```

Each middleware receives the execution and the task context, and returns the result of `next`. The object passed to `next` is merged into the task context, and its keys are added to the handler's `ctx` type. Middleware runs in array order, so later middleware sees what earlier middleware added. Its `ctx` parameter is typed with the base context only.

`execution` contains:

- `id`, `task_key`, `queue`
- `metadata`: the [execution metadata](execution-metadata.md), typed by the metadata schema and `undefined` when the execution has none

`next` settles when the attempt ends. It resolves when the handler returns, suspends (sleep, event wait, child invocation), is cancelled or times out, and rejects when the handler throws, so `try`/`finally` cleanup runs for every attempt. Use `ctx.signal.aborted` to tell these apart from a completed handler. An abort always wins: whatever middleware returns or throws after the attempt was suspended, cancelled or timed out is ignored.

An error thrown by middleware fails the attempt like a handler error, and the execution is retried.

Middleware does not run for batch tasks, which receive neither custom context nor middleware, or for the internal maintenance and event dispatch tasks.

## Wrapping Context Methods

Middleware can replace context methods by passing wrappers to `next`. A wrapper must keep the method's signature, so TypeScript types its parameters from the original, and handlers see the method's normal type. Use it to enforce an operation policy:

```typescript
const reserved = "internal.";

function reserveKey(key: string) {
  if (key.startsWith(reserved)) throw new Error(`Step key "${key}" is reserved`);
}

const conductor = Conductor.create({
  sql,
  tasks,
  events,
  metadata,
  context: {},
  middleware: [
    async ({ ctx }, next) => {
      // Capture the originals before wrapping them
      const step = ctx.step.bind(ctx);
      const invoke = ctx.invoke.bind(ctx);
      const emit = ctx.emit.bind(ctx);

      return next({
        // Reserved key prefix
        step: async (key, fn) => {
          reserveKey(key);
          return step(key, fn);
        },
        // Metadata hygiene: merge into the metadata the call would set
        invoke: async (key, task, payload, options = {}) => {
          reserveKey(key);
          return invoke(key, task, payload, {
            ...options,
            metadata: (current) => ({
              ...(typeof options.metadata === "function"
                ? options.metadata(current)
                : options.metadata || current),
              traceId: currentTraceId(),
            }),
          });
        },
        // Reserved destination
        emit: async (event, payload, options) => {
          if (event === "audit.recorded") throw new Error(`"${event}" is reserved`);
          return emit(event, payload, options);
        },
        // Internal helpers call the originals, so the policy does not apply to them
        audit: (action: string) =>
          invoke(`${reserved}audit.${action}`, { name: "record-audit" }, { action }),
      });
    },
  ],
});
```

The contract:

- Wrappers apply to every call made after `next`: by the handler and by later middleware, which receive the same context. Middleware runs on every attempt, so wrappers run again on every retry and resume. Memoized methods still return their stored result, so a wrapper that transforms arguments should do so deterministically.
- Each wrapper runs once per call. Context methods never call each other through `ctx`: `ctx.step()` does not call `ctx.checkpoint()`, `ctx.waitForResult()` does not call `ctx.waitForAny()`, and the `wait()` of a `ctx.subscribe()` subscription does not call `ctx.waitForEvent()`. Wrap each method you want to cover.
- A wrapper that throws rejects the call before anything is stored. Make wrappers `async`, so a rejection is a rejected promise like the original's.
- Capture originals with `bind(ctx)`: methods read `this`. Helpers that use the captured originals bypass the policy.
- `ctx.invoke()`, `ctx.invokeAndWait()`, `ctx.signalWithStart()`, `ctx.emit()` and `ctx.schedule()` take `metadata`, which a wrapper can transform. `ctx.sendSignal()` carries none, but a wrapper can reject its destination or signal name.
- Batch tasks and the internal maintenance and event dispatch tasks skip middleware, so nothing is wrapped for them.

Keyed methods are `step`, `sleep`, `invoke`, `invokeAndWait`, `waitForResult`, `waitForEvent`, `subscribe`, `waitForAny` and `signalWithStart`. To reject a prefix for all of them, a generic helper saves repeating the wrapper. Pass the method type explicitly: TypeScript would otherwise instantiate the generic methods' type parameters:

```typescript
const keyed = <T extends (key: string, ...args: never[]) => unknown>(method: T): T =>
  (async (key: string, ...args: never[]) => {
    reserveKey(key);
    return method(key, ...args);
  }) as T;

const sleep = ctx.sleep.bind(ctx);
const waitForResult = ctx.waitForResult.bind(ctx);
return next({
  sleep: keyed<typeof sleep>(sleep),
  waitForResult: keyed<typeof waitForResult>(waitForResult),
});
```

### Reserving Step Keys

Postgres Conductor stores steps under the keys you pass, except for these derived keys:

- `<stepKey>:<branch>` for the execution and signal branches of `ctx.waitForAny()`, so `<stepKey>:result` for `ctx.waitForResult()`
- `pgconductor.next-occurrence` for the next occurrence of a cron schedule

Every derived key starts with a key a method received or with `pgconductor.`. A reserved prefix that contains no `:` and does not start with `pgconductor.` is therefore yours alone once the keyed methods reject it for user code.

## What's Next?

- [Testing](testing.md) - Test tasks with custom context
- [API Reference: Conductor](../api/conductor.md) - Full Conductor API
