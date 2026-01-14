# Task Triggers

Tasks can be triggered in multiple ways:

- **Invocable** - Triggered manually via `conductor.invoke()`
- **Cron** - Triggered on a schedule
- **Custom Events** - Triggered when you emit custom application events
- **Database Triggers** - Triggered automatically by Postgres triggers on INSERT/UPDATE/DELETE

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

## Database Triggers

React to database changes automatically using Postgres triggers. When a row is inserted, updated, or deleted, Postgres Conductor creates a task execution with the row data.

### Setup

Provide your database schema types to enable type-safe database triggers:

```typescript
import { DatabaseSchema } from "pgconductor-js";
import type { Database } from "./database.types"; // Generated types

const conductor = Conductor.create({
  connectionString: "postgres://localhost/mydb",
  tasks: TaskSchemas.fromSchema([taskDef]),
  database: DatabaseSchema.fromGeneratedTypes<Database>(),
  context: {},
});
```

### Creating Database Triggers

```typescript
const onContactInsert = conductor.createTask(
  { name: "on-contact-insert" },
  {
    schema: "public",
    table: "contact",
    operation: "insert",
    columns: "id,email,first_name", // Required
  },
  async (event, ctx) => {
    // event.name === "public.contact.insert"
    // event.payload.tg_op === "INSERT"
    // event.payload.old === null (no old row on insert)
    // event.payload.new contains selected columns

    const { id, email, first_name } = event.payload.new;
    ctx.logger.info(`New contact: ${first_name} (${email})`);
  }
);
```

The orchestrator automatically creates Postgres triggers on your tables when it starts.

### Operations

Support for INSERT, UPDATE, and DELETE:

**INSERT** - Only `new` row available:
```typescript
{
  schema: "public",
  table: "contact",
  operation: "insert",
  columns: "id,email"
}
// event.payload.old === null
// event.payload.new === { id: string, email: string | null }
```

**UPDATE** - Both `old` and `new` rows available:
```typescript
{
  schema: "public",
  table: "contact",
  operation: "update",
  columns: "id,email,name"
}
// event.payload.old === { id: string, email: string | null, name: string }
// event.payload.new === { id: string, email: string | null, name: string }
```

**DELETE** - Only `old` row available:
```typescript
{
  schema: "public",
  table: "contact",
  operation: "delete",
  columns: "id,email"
}
// event.payload.old === { id: string, email: string | null }
// event.payload.new === null
```

### Conditional Triggers

Use `when` clause to filter which rows trigger task execution:

```typescript
conductor.createTask(
  { name: "on-active-user" },
  {
    schema: "public",
    table: "users",
    operation: "insert",
    columns: "id,email,name",
    when: "NEW.active = true", // Only trigger for active users
  },
  async (event, ctx) => {
    // Only called when active = true
  }
);
```

The `when` clause is evaluated in the Postgres trigger before creating a task execution.

### How It Works

When you start the orchestrator:

1. Postgres triggers are created on your specified tables
2. When a row changes, the trigger captures the row data
3. A task execution is created with the selected columns
4. Your task handler receives the event with typed payload

The triggers persist in the database even after the orchestrator stops.

## Multiple Triggers

Tasks can respond to multiple trigger types:

```typescript
const flexibleTask = conductor.createTask(
  { name: "flexible-task" },
  [
    { invocable: true },
    { cron: "0 * * * *", name: "hourly" },
    { event: "user.created" },
    {
      schema: "public",
      table: "contact",
      operation: "insert",
      columns: "id,email"
    },
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
    } else if (event.name === "public.contact.insert") {
      // Database trigger
      const { id, email } = event.payload.new;
    }
  }
);
```

## What's Next?

- [Retries and Steps](retries-and-steps.md) - Handle retries safely with steps
- [Child Invocation](child-invocation.md) - Invoke other tasks from within a task
- [Cron Scheduling](../task-execution/cron.md) - Learn more about cron syntax and dynamic schedules
