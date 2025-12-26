# Logging

Control how tasks log information during execution.

## Built-in Logging

Postgres Conductor a logger in the task context:

```typescript
const task = conductor.createTask(
  { name: "monitored-task" },
  { invocable: true },
  async (event, ctx) => {
    ctx.logger.info("Task started");
    ctx.logger.debug("Processing payload:", event.payload);

    try {
      const result = await processData(event.payload);
      ctx.logger.info("Task completed successfully");
      return result;
    } catch (error) {
      ctx.logger.error("Task failed:", error);
      throw error;
    }
  }
);
```

All log messages automatically include:

- Task name
- Execution ID
- Queue name
- Timestamp

## Log Levels

The logger provides four levels:

```typescript
ctx.logger.debug("Detailed diagnostic information");
ctx.logger.info("General informational messages");
ctx.logger.warn("Warning messages for potentially harmful situations");
ctx.logger.error("Error messages for failures");
```

## Custom Logger

Provide your own logger implementation to integrate with your logging service:

```typescript
import { Logger } from "pgconductor-js";

class CustomLogger implements Logger {
  info(message: string, ...args: unknown[]) {
    // Send to your logging service
    logService.log("info", message, { args, service: "pgconductor" });
  }

  error(message: string, ...args: unknown[]) {
    logService.log("error", message, { args, service: "pgconductor" });
  }

  debug(message: string, ...args: unknown[]) {
    logService.log("debug", message, { args, service: "pgconductor" });
  }

  warn(message: string, ...args: unknown[]) {
    logService.log("warn", message, { args, service: "pgconductor" });
  }
}

const conductor = Conductor.create({
  connectionString: "postgres://localhost:5432/mydb",
  tasks: TaskSchemas.fromSchema([tasks]),
  context: {},
  logger: new CustomLogger(),
});
```

## What's Next?

- [Retention Policies](retention.md) - Configure automatic cleanup
- [Testing](testing.md) - Unit test tasks
