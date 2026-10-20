# Testing

Test tasks end-to-end against a real Postgres: `orchestrator.drain()` runs everything that is due, and `createTestClock()` from `pgconductor-js/testing` controls time, so sleeps, retries and cron schedules don't need real waiting.

## Basic Example

Give each test its own database and connect with a single-connection pool (see [How Time Works](#how-time-works)):

```typescript
import { randomUUID } from "node:crypto";
import postgres, { type Sql } from "postgres";
import { afterEach, beforeEach, expect, test } from "vitest";
import { Conductor, Orchestrator, TaskSchemas, type DefineTask } from "pgconductor-js";
import { createTestClock, type TestClock } from "pgconductor-js/testing";

type Tasks = DefineTask<{ name: "remind"; payload: { to: string } }>;

const server = postgres(process.env.DATABASE_URL, { max: 1 });
let database: string;
let sql: Sql;
let clock: TestClock;

beforeEach(async () => {
  database = `test_${randomUUID().replaceAll("-", "")}`;
  await server.unsafe(`create database ${database}`);
  const url = new URL(process.env.DATABASE_URL);
  url.pathname = `/${database}`;
  sql = postgres(url.toString(), { max: 1 });
  clock = createTestClock(sql);
  await clock.set(new Date("2024-01-01T12:00:00Z"));
});

afterEach(async () => {
  await sql.end();
  await server.unsafe(`drop database ${database}`);
});

test("sends the reminder an hour later", async () => {
  const conductor = Conductor.create({
    sql,
    tasks: TaskSchemas.fromUnion<Tasks>(),
    context: {},
    clock,
  });

  const sent: string[] = [];
  const remind = conductor.createTask({ name: "remind" }, { invocable: true }, async (event, ctx) => {
    await ctx.sleep("wait", 60 * 60 * 1000);
    sent.push(event.payload.to);
  });
  const orchestrator = Orchestrator.create({ conductor, tasks: [remind] });

  await conductor.ensureInstalled();
  await conductor.invoke({ name: "remind" }, { to: "ada@example.com" });
  await orchestrator.drain();
  expect(sent).toEqual([]);

  await clock.advance("1h");
  await orchestrator.drain();
  expect(sent).toEqual(["ada@example.com"]);
});
```

## Controlling Time

The clock moves two things together: database time, which decides when sleeps end and retries are due, and the time workers use for cron schedules and time windows. Pass it to `Conductor.create({ clock })` so workers use it:

```typescript
await clock.set(new Date("2024-01-01T12:00:00Z")); // Freeze time
await clock.advance("15m");                         // 12:15:00
await clock.reset();                                // Back to real time
```

Fake time stands still until you move it. Without `set()`, `advance()` starts from the current real time.

**Cron:** a worker schedules the next occurrence when it starts and after each run.

```typescript
const report = conductor.createTask(
  { name: "report" },
  { cron: "*/15 * * * *", name: "quarter-hourly" },
  async () => { /* ... */ },
);

await orchestrator.drain();   // Schedules 12:15
await clock.advance("15m");
await orchestrator.drain();   // Runs once and schedules 12:30
```

**Retries:** a failed attempt is retried after 15 seconds, then 30, 60 and so on (see [Retries & Backoff](../task-execution/retries.md)).

```typescript
await orchestrator.drain();   // Attempt 1 fails
await clock.advance("15s");
await orchestrator.drain();   // Attempt 2
```

**Running orchestrators:** workers read the time from the clock, so `clock.set()` and `clock.advance()` also reach orchestrators started with `start()`.

## How Time Works

Postgres Conductor reads time from `pgconductor._private_current_time()`, which returns the `pgconductor.fake_now` session setting when it is set. Because the setting belongs to one connection, the `sql` you pass to `createTestClock()` must have exactly one connection (`max: 1`), and everything that should see fake time must use it: the conductor, the orchestrator and your assertions.

A single connection runs one query at a time. A transaction on `sql` holds the connection until it ends, so do not wait inside it for a query that runs on `sql` outside the transaction. A second connection to the same database sees real time.

## Mocking Services

Mock external services by passing them in the context:

```typescript
import { vi } from "vitest";

test("sends email via mocked service", async () => {
  const email = { send: vi.fn().mockResolvedValue(true) };

  const conductor = Conductor.create({
    sql,
    tasks: TaskSchemas.fromUnion<DefineTask<{ name: "send-email"; payload: { to: string } }>>(),
    context: { email },
    clock,
  });

  const task = conductor.createTask({ name: "send-email" }, { invocable: true }, async (event, ctx) => {
    await ctx.email.send(event.payload);
  });

  await conductor.ensureInstalled();
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

## What's Next?

- [API Reference: Testing](../api/testing.md) - The `pgconductor-js/testing` API
- [Custom Context](custom-context.md) - Inject services and configuration
- [API Reference: Orchestrator](../api/orchestrator.md) - Full Orchestrator API
