# Installation

Postgres Conductor is available on npm:

```bash
pnpm install pgconductor-js
```

## Database Setup

Postgres Conductor manages the `pgconductor` schema itself. All you have to do is provide a connection string. On startup, it will create the schema and run any migrations if necessary.

```typescript
const conductor = Conductor.create({
    connectionString: "postgres://localhost:5432/mydb"
});

const orchestrator = Orchestrator.create({
    conductor,
});

// This will run `ensureInstalled()` during startup
await orchestrator.start();

// Alternatively, call it manually form the conductor
await conductor.ensureInstalled();
```

If this library will ever ship breaking changes to the schema, it will gracefully handle the upgrade via [live migrations]().

## What's Next?

Learn how to [create your first task](first-task.md).
