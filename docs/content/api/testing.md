# Testing API

Helpers for testing tasks against a real Postgres, exported from `pgconductor-js/testing`. See [Testing](../crafting-tasks/testing.md) for a guide.

## createTestDatabase()

Create an isolated database:

```typescript
import { createTestDatabase } from "pgconductor-js/testing";

const testDb = await createTestDatabase({
  url?,      // Postgres server to create the database on
  migrate?,  // Install the Postgres Conductor schema (default: true)
});
```

**Parameters:**

- `url`: (optional) Connection string of the server. Defaults to `DATABASE_URL`, then to a Postgres container started once per process with the optional `@testcontainers/postgresql` peer dependency
- `migrate`: (optional) Clone a migrated template database. With `false`, the database is empty, for tests of `conductor.ensureInstalled()` or `orchestrator.start()` migrations

**Returns:** a `TestDatabase`:

- `sql`: a postgres.js pool with a single connection; pass it to `Conductor.create({ sql })`
- `url`: connection string of the test database
- `name`: database name
- `clock`: the database clock
- `destroy()`: close `sql` and drop the database

## testDb.clock

Database time for everything that uses `testDb.sql`. Changes reach orchestrators that are running on it.

```typescript
await testDb.clock.set(new Date("2024-01-01T12:00:00Z"));
await testDb.clock.advance("15m");
await testDb.clock.reset();
```

- `set(date)`: fake the time; it stands still until it is moved again
- `advance(duration)`: move time forward by a duration in milliseconds or a string such as `"15s"`, `"15m"`, `"1h"` or `"1d"`; starts from real time if time is not faked
- `reset()`: return to real time

## startPostgres()

Get a Postgres server, for example in a Vitest global setup:

```typescript
import { startPostgres } from "pgconductor-js/testing";

const postgres = await startPostgres({ image: "postgres:17" });
process.env.DATABASE_URL = postgres.url;
// ...
await postgres.stop();
```

- Uses `DATABASE_URL` if it is set; `stop()` then does nothing
- Otherwise starts a container from `image` (default: `postgres:17`) with `@testcontainers/postgresql`, which must be installed
