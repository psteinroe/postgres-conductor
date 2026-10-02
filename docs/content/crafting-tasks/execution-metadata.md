# Execution Metadata

Metadata is a small JSON object attached to an execution. Executions created from a run inherit
it, so a request ID, tenant or reply address set at the entry point reaches every task in the
workflow without being threaded through each payload.

## Typing Metadata

Pass a [Standard Schema](https://standardschema.dev) to type and validate metadata:

```typescript
import { z } from "zod";

const conductor = Conductor.create({
  sql,
  tasks: TaskSchemas.fromSchema([handleMention, postReply]),
  events: EventSchemas.fromSchema([mentioned]),
  metadata: z.object({ tenant: z.string(), replyTo: z.string().optional() }),
  context: {},
});
```

Without a schema, metadata is any JSON object.

## Setting Metadata

Set metadata when invoking a task or emitting an event:

```typescript
await conductor.invoke({ name: "handle-mention" }, payload, {
  metadata: { tenant: "acme", replyTo: "thread-1" },
});

await conductor.emit("slack.mentioned", payload, {
  metadata: { tenant: "acme", replyTo: "thread-1" },
});
```

The schema validates metadata on `conductor.invoke()`, `conductor.emit()` and `ctx.invoke()`
overrides, like event payloads.

## Reading Metadata

`ctx.metadata` is readonly and `undefined` when the execution has none:

```typescript
const handleMention = conductor.createTask(
  { name: "handle-mention" },
  { invocable: true },
  async (event, ctx) => {
    ctx.logger.info(`Handling mention for ${ctx.metadata?.tenant}`);
  },
);
```

## Inheritance

These executions inherit the metadata of the execution or event that created them:

- Executions triggered by an event
- Children created with `ctx.invoke()`
- Events emitted with `ctx.emit()`, and the executions they trigger
- Cron executions scheduled with `ctx.schedule()`
- Dead-letter deliveries

Retries and resumes after `ctx.sleep()`, `ctx.invoke()` or `ctx.waitForEvent()` run the same
execution and keep its metadata. Cron schedules declared as task triggers have no metadata.

## Overriding Metadata for a Child

Pass `metadata` to `ctx.invoke()` to replace the metadata of that child only. A function
receives the current metadata:

```typescript
await ctx.invoke("reply", { name: "post-reply" }, payload, {
  metadata: (metadata) => ({ ...metadata, tenant: "acme", replyTo: "thread-2" }),
});
```

The parent and later children keep the original metadata.

## Size Limit

Metadata is limited to 8 KB (8192 bytes of its JSON text in Postgres). Larger metadata is
rejected with `Execution metadata must not exceed 8192 bytes`. An oversized `ctx.invoke()`
override fails the invoking attempt. Store references in metadata, not payloads.

## What's Next?

- [Child Invocation](child-invocation.md) - Invoke tasks from tasks
- [Task Triggers](triggers.md) - Trigger tasks with events
- [Task Context API](../api/task-context.md) - Full context reference
