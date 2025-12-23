# Testing

Test your tasks end-to-end using `drain()` mode. It processes all queued tasks then stops automatically.

## Basic Example

```typescript
import { test, expect } from "vitest";
import { z } from "zod";
import { defineTask, Conductor, Orchestrator, TaskSchemas } from "pgconductor-js";

const testTaskDef = defineTask({
  name: "test-task",
  payload: z.object({
    value: z.number(),
  }),
});

test("processes tasks end-to-end", async () => {
  const conductor = Conductor.create({
    connectionString: process.env.TEST_DATABASE_URL,
    tasks: TaskSchemas.fromSchema([testTaskDef]),
    context: {},
  });

  const results = [];

  const task = conductor.createTask(
    { name: "test-task" },
    { invocable: true },
    async (event, ctx) => {
      results.push(event.payload.value);
    }
  );

  // Queue some tasks
  await conductor.invoke({ name: "test-task" }, { value: 1 });
  await conductor.invoke({ name: "test-task" }, { value: 2 });
  await conductor.invoke({ name: "test-task" }, { value: 3 });

  // Process and stop
  const orchestrator = Orchestrator.create({
    conductor,
    tasks: [task],
  });

  await orchestrator.drain(); // Runs until queue is empty

  expect(results).toEqual([1, 2, 3]);
});
```

## How It Works

1. **`drain()` starts the orchestrator** - Workers begin polling for tasks
2. **Processes all queued tasks** - Executes each task in order
3. **Stops automatically** - Once the queue is empty, the orchestrator shuts down
4. **Returns a promise** - Resolves when all work is complete

## Testing with Custom Context

Mock external services by passing them in the context:

```typescript
import { test, expect, vi } from "vitest";

const sendEmailTaskDef = defineTask({
  name: "send-email",
  payload: z.object({
    to: z.string(),
  }),
});

test("sends email via mocked service", async () => {
  const mockEmail = {
    send: vi.fn().mockResolvedValue(true),
  };

  const conductor = Conductor.create({
    connectionString: process.env.TEST_DATABASE_URL,
    tasks: TaskSchemas.fromSchema([sendEmailTaskDef]),
    context: {
      email: mockEmail, // Mock external service
    },
  });

  const task = conductor.createTask(
    { name: "send-email" },
    { invocable: true },
    async (event, ctx) => {
      await ctx.email.send(event.payload);
    }
  );

  await conductor.invoke({ name: "send-email" }, { to: "user@example.com" });

  const orchestrator = Orchestrator.create({
    conductor,
    tasks: [task],
  });

  await orchestrator.drain();

  expect(mockEmail.send).toHaveBeenCalledWith({ to: "user@example.com" });
});
```

## What's Next?

- [Custom Context](custom-context.md) - Inject services and configuration
- [Long-Running Tasks](long-running.md) - Test tasks with checkpoints
- [API Reference: Orchestrator](../api/orchestrator.md) - Full Orchestrator API
