<div align="center">
  <img src="docs/content/images/logo.png" alt="Postgres Conductor" width="300">

  # Postgres Conductor
  Durable execution using only Postgres

  [Documentation](https://github.com/psteinroe/pgconductor)
</div>

## Installation

Postgres Conductor is available on npm:

```bash
pnpm install pgconductor-js
```

## Quick Start

Here's a minimal example to get you started:

### Define a Task

Tasks are defined with a name and optional payload schema using [Standard Schema](https://standardschema.dev/):

```typescript
import { z } from "zod";
import { defineTask } from "pgconductor-js";

const greetTask = defineTask({
  name: "greet",
  payload: z.object({ name: z.string() }),
});
```

### Create the Conductor

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


### Implement the Task Handler

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

### Set Up the Orchestrator

The Orchestrator runs workers that execute tasks:

```typescript
import { Orchestrator } from "pgconductor-js";

const orchestrator = Orchestrator.create({
  conductor,
  tasks: [greet],
});

await orchestrator.start();
console.log("Worker running. Press Ctrl+C to stop.");
await orchestrator.stopped;
```

### Invoke the Task

In a separate process, invoke the task via an instance of the `Conductor`:

```typescript
await conductor.invoke(
  { name: "greet" },
  { name: "World" }
);
```
