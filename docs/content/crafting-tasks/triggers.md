# Task Triggers

Tasks can be triggered in multiple ways:

- **Invocable** - Triggered manually via `conductor.invoke()`
- **Cron** - Triggered on a schedule
- **Custom Events** - Triggered when you emit custom application events

## Invocable Tasks

Invocable tasks are triggered manually via `conductor.invoke()`:

```typescript
const processOrder = conductor.createTask(
  { name: "process-order" },
  { invocable: true },
  async (event, ctx) => {
    ctx.logger.info("Processing order:", event.payload);
  }
);
```

Invoke it:

```typescript
await conductor.invoke(
  { name: "process-order" },
  { orderId: "123", items: ["widget"] }
);
```

## Cron Tasks

Cron tasks run on a schedule defined with cron syntax:

```typescript
const dailyReport = conductor.createTask(
  { name: "daily-report" },
  { cron: "0 9 * * *" }, // Every day at 9 AM
  async (event, ctx) => {
    ctx.logger.info("Generating daily report");
    // Generate and send report
  }
);
```

## Custom Event Triggers

React to custom application events with type-safe payloads.

### Defining Events

First, define your custom events:

```typescript
import { defineEvent } from "pgconductor-js";
import { z } from "zod";

export const userCreated = defineEvent({
  name: "user.created",
  payload: z.object({
    userId: z.string(),
    email: z.string(),
    name: z.string(),
  }),
});
```

### Creating Event Handlers

Register the event in your conductor and create a task to handle it:

```typescript
import { EventSchemas } from "pgconductor-js";

const conductor = Conductor.create({
  connectionString: "postgres://localhost/mydb",
  tasks: TaskSchemas.fromSchema([taskDef]),
  events: EventSchemas.fromSchema([userCreated]),
  context: {},
});

const onUserCreated = conductor.createTask(
  { name: "send-welcome-email" },
  { event: "user.created" },
  async (event, ctx) => {
    // event.name === "user.created"
    // event.payload is fully typed!
    const { userId, email, name } = event.payload;

    await sendEmail({
      to: email,
      subject: `Welcome ${name}!`,
    });
  }
);
```

### Emitting Events

Emit events from anywhere in your application:

```typescript
await conductor.emit("user.created", {
  userId: "user-123",
  email: "alice@example.com",
  name: "Alice",
});
```

Each event is stored as a short-lived internal dispatch execution. Destination fan-out and a reserved completion marker commit in one database transaction, so failures retry without duplicating or changing the matched destination set.

### Emitting from Database Triggers

Postgres Conductor does not create or manage triggers on application tables. If a database change should emit an event, define the trigger in your own migrations and call `pgconductor.emit_event()`:

```sql
create function app.emit_user_created()
returns trigger
language plpgsql
as $$
begin
  perform pgconductor.emit_event('user.created', to_jsonb(new));
  return new;
end;
$$;

create trigger emit_user_created
after insert on app.users
for each row execute function app.emit_user_created();
```

Because `emit_event()` inserts the dispatch execution in the current transaction, emission is committed or rolled back with the database change.

### Event Filters

Declare top-level scalar fields as filterable, then register typed predicates on handlers:

```typescript
const orderChanged = defineEvent({
  name: "order.changed",
  payload: z.object({
    status: z.string(),
    region: z.string(),
    amount: z.number(),
    note: z.string().optional(),
  }),
  filterable: ["status", "region", "amount", "note"],
});

conductor.createTask(
  { name: "handle-paid-us-orders" },
  {
    event: "order.changed",
    filter: {
      status: ["paid", "trial"],
      region: [{ prefix: "us-" }],
      amount: [{ numeric: [">=", 10, "<", 100] }],
      note: [{ exists: false }],
    },
  },
  async (event, ctx) => {
    // Both fields matched; values within one field are alternatives.
  }
);
```

Fields are combined with AND and alternatives within a field with OR. Supported alternatives are:

- a string, number, boolean, or `null` for type-sensitive equality;
- `{ prefix: "literal" }` for a literal string prefix (`%`, `_`, and `\\` have no special meaning);
- `{ numeric: [">=", 10, "<", 100] }` for a one- or two-bound numeric range;
- `{ exists: true }` or `{ exists: false }` for field presence;
- `{ "anything-but": value }` for one atomic scalar exclusion.

Missing fields differ from JSON `null`. An `anything-but` predicate must be the field's only alternative. Filters are stored as normalized, typed predicates, narrowed through exact/prefix/range indexes or an explicit route-local fallback, and then completely verified. A filter may contain up to 8 fields and 4 alternatives per field; literal prefixes are limited to 64 characters.

### Field Selection

For large events, you can select only specific fields to reduce payload size:

```typescript
conductor.createTask(
  { name: "log-user-id" },
  { event: "user.created", fields: "userId" },
  async (event, ctx) => {
    // event.payload only contains { userId: string }
    ctx.logger.info("User created:", event.payload.userId);
  }
);
```

## Multiple Triggers

Tasks can respond to multiple trigger types:

```typescript
const flexibleTask = conductor.createTask(
  { name: "flexible-task" },
  [
    { invocable: true },
    { cron: "0 * * * *", name: "hourly" },
    { event: "user.created" },
  ],
  async (event, ctx) => {
    // Discriminate based on event.name
    if (event.name === "pgconductor.invoke") {
      // Manually invoked
      const payload = event.payload;
    } else if (event.name === "hourly") {
      // Cron-triggered by "hourly" schedule
    } else if (event.name === "user.created") {
      // Custom event
      const { userId, email, name } = event.payload;
    }
  }
);
```

## What's Next?

- [Retries and Steps](retries-and-steps.md) - Handle retries safely with steps
- [Child Invocation](child-invocation.md) - Invoke other tasks from within a task
- [Cron Scheduling](../task-execution/cron.md) - Learn more about cron syntax and dynamic schedules
