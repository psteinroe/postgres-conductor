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

## What's Next?

- [Testing](testing.md) - Test tasks with custom context
- [API Reference: Conductor](../api/conductor.md) - Full Conductor API
