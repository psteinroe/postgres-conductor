# Your First Task

This guide walks you through creating, running, and invoking your first Postgres Conductor task.

## Quick Start

Here's a minimal example to get you started:

## Define a Task

Tasks are defined with a name and optional payload schema using [Standard Schema](https://standardschema.dev/):

```typescript
import { z } from "zod";
import { defineTask } from "pgconductor-js";

const greetTask = defineTask({
  name: "greet",
  payload: z.object({ name: z.string() }),
});
```

## Create the Conductor

The Conductor manages task definitions and the database connection:

```typescript
import { Conductor } from "pgconductor-js";
import { TaskSchemas } from "pgconductor-js";

const sql = postgres();

const conductor = Conductor.create({
  connectionString: "postgres://localhost:5432/mydb",
  tasks: TaskSchemas.fromSchema([greetTask])
});
```


## Implement the Task Handler

Create a task handler that defines what happens when the task runs:

```typescript
const greet = conductor.createTask(
  { name: "greet" },
  { invocable: true }, // Allow invocation
  async (event, ctx) => {
    const { name } = event.payload;
    ctx.logger.info(`Hello, ${name}!`);
  }
);
```

## Set Up the Orchestrator

The Orchestrator runs workers that execute tasks:

```typescript
import { Orchestrator } from "pgconductor-js";

const orchestrator = Orchestrator.create({
  conductor,
  tasks: [greet],
});

await orchestrator.start();
console.log(" Worker running. Press Ctrl+C to stop.");
await orchestrator.stopped;
```

## Invoke the Task

In a separate process, invoke the task via an instance of the `Conductor`:

```typescript
await conductor.invoke(
  { name: "greet" },
  { name: "World" }
);
```

## Complete Example

Here's a full working example split into three files:

**tasks.ts** - Contains the task definition:

```typescript
import { z } from "zod";
import { defineTask } from "pgconductor-js";

export const greetTask = defineTask({
  name: "greet",
  payload: z.object({ name: z.string() }),
});
```

>![INFO]
> Store all task schemas in a shared package so both worker and client can use them.

**worker.ts** - Runs continuously to process tasks:

```typescript
import { Conductor, Orchestrator, TaskSchemas } from "pgconductor-js";

import { greetTask } from './tasks';

const conductor = Conductor.create({
  connectionString: "postgres://localhost:5432/mydb",
  tasks: TaskSchemas.fromSchema([greetTask])
});

const greet = conductor.createTask(
  { name: "greet" },
  { invocable: true },
  async (event, ctx) => {
    ctx.logger.info(`Hello, ${event.payload.name}!`);
  }
);

const orchestrator = Orchestrator.create({
  conductor,
  tasks: [greet],
});

await orchestrator.start();
await orchestrator.stopped; // Wait for shutdown signal
```

**invoke.ts** - Triggers tasks:

```typescript
import { Conductor, TaskSchemas } from "pgconductor-js";

import { greetTask } from './tasks';

const conductor = Conductor.create({
  connectionString: "postgres://localhost:5432/mydb",
  tasks: TaskSchemas.fromSchema([greetTask])
});

await conductor.invoke({ name: "greet" }, { name: "World" });
```

## What's Next?

- Learn about [task triggers](../crafting-tasks/triggers.md) (cron vs invocable)
- Understand [retries and steps](../crafting-tasks/retries-and-steps.md) for durable execution
- Explore [child invocation](../crafting-tasks/child-invocation.md) for workflows
