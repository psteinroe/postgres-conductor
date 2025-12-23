# Defining Tasks and Events

Define type-safe tasks, events, and database schemas that work across your entire application.

## Why Define Schemas?

Schema definitions give you:

- **Type safety** - TypeScript infers payload and return types automatically
- **Shared schemas** - One source of truth for worker and client
- **Runtime validation** - Optional validation with StandardSchema (Zod, Valibot, etc.)
- **Better DX** - Autocomplete for payloads and return values

## Task Definitions

### With StandardSchema

Use `defineTask()` with Zod or other StandardSchema-compatible libraries:

```typescript
import { defineTask } from "pgconductor-js";
import { z } from "zod";

export const sendEmailTask = defineTask({
  name: "send-email",
  payload: z.object({
    to: z.string().email(),
    subject: z.string(),
    body: z.string(),
  }),
  returns: z.object({
    messageId: z.string(),
    sent: z.boolean(),
  }),
});
```

### With TypeScript Types

Define tasks using pure TypeScript types:

```typescript
import type { DefineTask } from "pgconductor-js";

export type SendEmailTask = DefineTask<{
  name: "send-email";
  payload: {
    to: string;
    subject: string;
    body: string;
  };
  returns: {
    messageId: string;
    sent: boolean;
  };
}>;
```

### Registering Tasks

Both approaches register similarly:

```typescript
import { Conductor, TaskSchemas } from "pgconductor-js";

// With StandardSchema
const conductor = Conductor.create({
  connectionString: "postgres://localhost/mydb",
  tasks: TaskSchemas.fromSchema([sendEmailTask, processOrderTask]),
  context: {},
});

// With TypeScript types
const conductor = Conductor.create({
  connectionString: "postgres://localhost/mydb",
  tasks: TaskSchemas.fromTypes<[SendEmailTask, ProcessOrderTask]>(),
  context: {},
});
```

## Event Definitions

Events enable reactive, event-driven patterns.

### With StandardSchema

```typescript
import { defineEvent } from "pgconductor-js";
import { z } from "zod";

export const userCreated = defineEvent({
  name: "user.created",
  payload: z.object({
    userId: z.string().uuid(),
    email: z.string().email(),
    name: z.string(),
  }),
});

export const orderPlaced = defineEvent({
  name: "order.placed",
  payload: z.object({
    orderId: z.string(),
    total: z.number().positive(),
  }),
});
```

### With TypeScript Types

```typescript
import type { DefineEvent } from "pgconductor-js";

export type UserCreated = DefineEvent<{
  name: "user.created";
  payload: {
    userId: string;
    email: string;
    name: string;
  };
}>;

export type OrderPlaced = DefineEvent<{
  name: "order.placed";
  payload: {
    orderId: string;
    total: number;
  };
}>;
```

### Registering Events

```typescript
import { EventSchemas } from "pgconductor-js";

// With StandardSchema
const conductor = Conductor.create({
  connectionString: "postgres://localhost/mydb",
  tasks: TaskSchemas.fromSchema([...]),
  events: EventSchemas.fromSchema([userCreated, orderPlaced]),
  context: {},
});

// With TypeScript types
const conductor = Conductor.create({
  connectionString: "postgres://localhost/mydb",
  tasks: TaskSchemas.fromTypes<[...]>(),
  events: EventSchemas.fromTypes<[UserCreated, OrderPlaced]>(),
  context: {},
});
```

## Database Schema

Database schemas enable type-safe database triggers. These are always TypeScript types generated from your database.

### Generating Types

Use your preferred tool to generate types from your database:

**With pgconductor CLI:**

```bash
pnpx pgconductor gen types typescript --db-url "postgres://localhost/mydb" --schemas "public" > database.types.ts
```

**With Supabase CLI:**

```bash
pnpx supabase gen types typescript --linked > database.types.ts
```

### Registering Database Schema

```typescript
import { DatabaseSchema } from "pgconductor-js";
import type { Database } from "./database.types";

const conductor = Conductor.create({
  connectionString: "postgres://localhost/mydb",
  tasks: TaskSchemas.fromSchema([...]),
  database: DatabaseSchema.fromGeneratedTypes<Database>(),
  // or if you are using Supabase CLI
  // database: DatabaseSchema.fromSupabaseTypes<Database>(),
  context: {},
});
```

Now database triggers are fully typed:

```typescript
conductor.createTask(
  { name: "on-user-created" },
  {
    schema: "public",
    table: "users",
    operation: "insert",
    columns: "id,email,name", // TypeScript validates these columns exist!
  },
  async (event, ctx) => {
    // event.payload.new is typed as { id: string, email: string, name: string }
    const { id, email, name } = event.payload.new;
  }
);
```

## Return Types

Tasks can return typed results when invoked from other tasks using `ctx.invoke()`:

```typescript
export const processDataTask = defineTask({
  name: "process-data",
  payload: z.object({
    items: z.array(z.string()),
  }),
  returns: z.object({
    processed: z.number(),
    errors: z.array(z.string()),
  }),
});

// Create handler
const task = conductor.createTask(
  { name: "process-data" },
  { invocable: true },
  async (event, ctx) => {
    // Return value must match schema
    return {
      processed: event.payload.items.length,
      errors: [],
    };
  }
);

// Invoke from another task and get typed result
const parentTask = conductor.createTask(
  { name: "parent-task" },
  { invocable: true },
  async (event, ctx) => {
    // ctx.invoke() waits for completion and returns the result
    const result = await ctx.invoke(
      { name: "process-data" },
      { items: ["a", "b", "c"] }
    );

    // result is typed as { processed: number, errors: string[] }
    console.log(result.processed); // Type-safe!
  }
);
```

**Note**: `conductor.invoke()` (called from outside a task) does not wait for completion and does not return results. Return types are only useful with `ctx.invoke()` (called from within a task).

## Sharing Definitions

Store schemas in a shared package so both worker and client use the same definitions.

### Monorepo Structure

```
packages/
├── schemas/              # Shared schema package
│   ├── package.json
│   ├── tasks.ts          # Task definitions
│   ├── events.ts         # Event definitions
│   └── database.types.ts # Generated database types
├── worker/               # Worker service
│   ├── package.json
│   └── src/
│       └── index.ts
└── api/                  # API/client service
    ├── package.json
    └── src/
        └── index.ts
```

`packages/schemas/tasks.ts`:

```typescript
import { defineTask } from "pgconductor-js";
import { z } from "zod";

export const sendEmailTask = defineTask({
  name: "send-email",
  payload: z.object({
    to: z.string().email(),
    subject: z.string(),
  }),
  returns: z.object({
    sent: z.boolean(),
    messageId: z.string(),
  }),
});

export const processOrderTask = defineTask({
  name: "process-order",
  payload: z.object({
    orderId: z.string().uuid(),
    items: z.array(z.object({
      productId: z.string(),
      quantity: z.number().positive(),
    })),
  }),
  returns: z.object({
    success: z.boolean(),
    total: z.number(),
  }),
});
```

`packages/schemas/events.ts`:

```typescript
import { defineEvent } from "pgconductor-js";
import { z } from "zod";

export const userCreated = defineEvent({
  name: "user.created",
  payload: z.object({
    userId: z.string().uuid(),
    email: z.string().email(),
  }),
});
```

`worker/src/index.ts`:

```typescript
import { Conductor, Orchestrator, TaskSchemas, EventSchemas, DatabaseSchema } from "pgconductor-js";
import { sendEmailTask, processOrderTask } from "@myapp/schemas/tasks";
import { userCreated } from "@myapp/schemas/events";
import type { Database } from "@myapp/schemas/database.types";

const conductor = Conductor.create({
  connectionString: process.env.DATABASE_URL,
  tasks: TaskSchemas.fromSchema([sendEmailTask, processOrderTask]),
  events: EventSchemas.fromSchema([userCreated]),
  database: DatabaseSchema.fromGeneratedTypes<Database>(),
  context: {},
});

const sendEmail = conductor.createTask(
  { name: "send-email" },
  { invocable: true },
  async (event, ctx) => {
    // Implementation...
    return { sent: true, messageId: "msg-123" };
  }
);

const processOrder = conductor.createTask(
  { name: "process-order" },
  { invocable: true },
  async (event, ctx) => {
    // Implementation...
    return { success: true, total: 99.99 };
  }
);

const orchestrator = Orchestrator.create({
  conductor,
  tasks: [sendEmail, processOrder],
});

await orchestrator.start();
```

`api/src/index.ts`:

```typescript
import { Conductor, TaskSchemas, EventSchemas } from "pgconductor-js";
import { sendEmailTask, processOrderTask } from "@myapp/schemas/tasks";
import { userCreated } from "@myapp/schemas/events";

const conductor = Conductor.create({
  connectionString: process.env.DATABASE_URL,
  tasks: TaskSchemas.fromSchema([sendEmailTask, processOrderTask]),
  events: EventSchemas.fromSchema([userCreated]),
  context: {},
});

// Invoke tasks with full type safety
await conductor.invoke(
  { name: "send-email" },
  { to: "user@example.com", subject: "Welcome" }
);

await conductor.invoke(
  { name: "process-order" },
  {
    orderId: "123e4567-e89b-12d3-a456-426614174000",
    items: [
      { productId: "prod-1", quantity: 2 },
      { productId: "prod-2", quantity: 1 },
    ],
  }
);

// Emit events with full type safety
await conductor.emit("user.created", {
  userId: "123e4567-e89b-12d3-a456-426614174000",
  email: "alice@example.com",
});
```

## What's Next?

- [Task Triggers](triggers.md) - Configure invocable, cron, event, and database triggers
- [Testing](testing.md) - Unit test tasks with type-safe mocks
- [Conductor API](../api/conductor.md) - Full API documentation
