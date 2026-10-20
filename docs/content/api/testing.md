# Testing API

Exported from `pgconductor-js/testing`. See [Testing](../crafting-tasks/testing.md) for a guide.

## createTestClock()

Control database time:

```typescript
import { createTestClock } from "pgconductor-js/testing";

const sql = postgres(testDatabaseUrl, { max: 1 });
const clock = createTestClock(sql);
const conductor = Conductor.create({ sql, tasks, context, clock });
```

**Parameters:**

- `sql`: a postgres.js pool with a single connection (`max: 1`). Fake time is a session setting, so only queries on this connection see it

The clock sets database time for everything that uses `sql`. Passed to `Conductor.create({ clock })`, it is also the time workers use for cron schedules and time windows, including workers that are already running.

## clock.now()

The faked time, or the system time when time is not faked.

## clock.set(date)

Fake the time. It stands still until it is moved again.

```typescript
await clock.set(new Date("2024-01-01T12:00:00Z"));
```

## clock.advance(duration)

Move time forward by a duration in milliseconds or a string such as `"15s"`, `"15m"`, `"1h"` or `"1d"`. Starts from the system time if time is not faked.

```typescript
await clock.advance("15m");
```

## clock.reset()

Return to real time.

```typescript
await clock.reset();
```
