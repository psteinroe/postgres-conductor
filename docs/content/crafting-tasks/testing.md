# Testing

Test tasks end-to-end against a real Postgres with `pgconductor-js/testing`. Each test gets its own database with the Postgres Conductor schema installed, a clock you control, and `drain()` runs everything that is due.

There is no in-memory mode: tests run the same SQL, locks and sessions as production, on the Postgres version you choose.

## Setup

Point `DATABASE_URL` at a Postgres server the tests may create databases on, or install testcontainers and let the helper start a container:

```bash
pnpm add -D vitest @testcontainers/postgresql
```

`createTestDatabase()` uses `DATABASE_URL` if it is set and otherwise starts a Postgres container that lives until the process exits. Vitest runs test files in several processes, so start one Postgres in a global setup and share it through `DATABASE_URL`:

```typescript
// vitest.global-setup.ts
import { startPostgres } from "pgconductor-js/testing";

export default async function setup() {
  const postgres = await startPostgres({ image: "postgres:17" });
  process.env.DATABASE_URL = postgres.url;
  return postgres.stop;
}
```

```typescript
// vitest.config.ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: { globalSetup: ["vitest.global-setup.ts"], hookTimeout: 60_000 },
});
```

Use the image of the Postgres version you run in production.

## Basic Example

```typescript
import { afterEach, beforeEach, expect, test } from "vitest";
import { Conductor, Orchestrator, TaskSchemas, type DefineTask } from "pgconductor-js";
import { createTestDatabase, type TestDatabase } from "pgconductor-js/testing";

type Tasks = DefineTask<{ name: "remind"; payload: { to: string } }>;

let testDb: TestDatabase;

beforeEach(async () => {
  testDb = await createTestDatabase();
  await testDb.clock.set(new Date("2024-01-01T12:00:00Z"));
});

afterEach(async () => {
  await testDb.destroy();
});

test("sends the reminder an hour later", async () => {
  const conductor = Conductor.create({
    sql: testDb.sql,
    tasks: TaskSchemas.fromUnion<Tasks>(),
    context: {},
  });

  const sent: string[] = [];
  const remind = conductor.createTask({ name: "remind" }, { invocable: true }, async (event, ctx) => {
    await ctx.sleep("wait", 60 * 60 * 1000);
    sent.push(event.payload.to);
  });
  const orchestrator = Orchestrator.create({ conductor, tasks: [remind] });

  await conductor.invoke({ name: "remind" }, { to: "ada@example.com" });
  await orchestrator.drain();
  expect(sent).toEqual([]);

  await testDb.clock.advance("1h");
  await orchestrator.drain();
  expect(sent).toEqual(["ada@example.com"]);
});
```

Pass `testDb.sql` to the conductor. It is a pool with a single connection, which the fake clock needs (see [How Time Works](#how-time-works)).

## Controlling Time

`testDb.clock` moves database time, which decides when sleeps end, retries are due and cron schedules fire:

```typescript
await testDb.clock.set(new Date("2024-01-01T12:00:00Z")); // Freeze time
await testDb.clock.advance("15m");                         // 12:15:00
await testDb.clock.reset();                                // Back to real time
```

Fake time stands still until you move it. Without `set()`, `advance()` starts from the current real time.

**Cron:** a worker schedules the next occurrence when it starts and after each run.

```typescript
const report = conductor.createTask(
  { name: "report" },
  { cron: "*/15 * * * *", name: "quarter-hourly" },
  async () => { /* ... */ },
);

await orchestrator.drain();         // Schedules 12:15
await testDb.clock.advance("15m");
await orchestrator.drain();         // Runs once and schedules 12:30
```

**Retries:** a failed attempt is retried after 15 seconds, then 30, 60 and so on (see [Retries & Backoff](../task-execution/retries.md)).

```typescript
await orchestrator.drain();         // Attempt 1 fails
await testDb.clock.advance("15s");
await orchestrator.drain();         // Attempt 2
```

**Running orchestrators:** `drain()` is the simplest way to run work in a test, but `clock.set()` and `clock.advance()` also reach orchestrators started with `start()` on `testDb.sql`, so time windows and cron schedules follow the new time without a restart.

## How Time Works

Postgres Conductor reads time from `pgconductor._private_current_time()`, which returns the `pgconductor.fake_now` session setting when it is set. Because the setting belongs to one connection, `testDb.sql` has exactly one connection (`max: 1`), and everything that should see fake time must use it: the conductor, the orchestrator and your assertions.

A single connection runs one query at a time. A transaction on `testDb.sql` holds the connection until it ends, so do not wait inside it for a query that runs on `testDb.sql` outside the transaction. If you need a second connection, for example to test concurrent sessions, connect to `testDb.url`; it sees real time.

## Mocking Services

Mock external services by passing them in the context:

```typescript
import { vi } from "vitest";

test("sends email via mocked service", async () => {
  const email = { send: vi.fn().mockResolvedValue(true) };

  const conductor = Conductor.create({
    sql: testDb.sql,
    tasks: TaskSchemas.fromUnion<DefineTask<{ name: "send-email"; payload: { to: string } }>>(),
    context: { email },
  });

  const task = conductor.createTask({ name: "send-email" }, { invocable: true }, async (event, ctx) => {
    await ctx.email.send(event.payload);
  });

  await conductor.invoke({ name: "send-email" }, { to: "user@example.com" });
  await Orchestrator.create({ conductor, tasks: [task] }).drain();

  expect(email.send).toHaveBeenCalledWith({ to: "user@example.com" });
});
```

## How drain() Works

1. **Starts the orchestrator** - Workers begin polling for tasks
2. **Processes all due tasks** - Including retries, sleeps and cron runs that are due at the current database time
3. **Stops automatically** - Once nothing more is due, the orchestrator shuts down

If a task creates work for a queue whose pass already finished, for example through custom-event fan-out, call `drain()` again to run another pass.

## Databases

`createTestDatabase()` migrates a template database once per Postgres server and schema version, then clones it for every call with `create database ... template`, which takes a few milliseconds. Template databases are named `pgconductor_template_<hash>` and are kept for later runs; `testDb.destroy()` drops the test database.

## What's Next?

- [API Reference: Testing](../api/testing.md) - Full `pgconductor-js/testing` API
- [Custom Context](custom-context.md) - Inject services and configuration
- [API Reference: Orchestrator](../api/orchestrator.md) - Full Orchestrator API
