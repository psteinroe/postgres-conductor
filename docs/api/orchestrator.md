# Orchestrator API

The Orchestrator manages worker lifecycle and coordinates task execution.

## Orchestrator.create()

Create an orchestrator:

```typescript
import { Orchestrator } from "pgconductor-js";

const orchestrator = Orchestrator.create({
  conductor,            // Conductor instance
  tasks?,               // Tasks for default worker
  workers?,             // Custom workers
  defaultWorker?        // Default worker config
});
```

**Parameters:**

- `conductor`: Conductor instance
- `tasks`: (optional) Array of tasks using default worker
- `workers`: (optional) Array of custom workers from `conductor.createWorker()`
- `defaultWorker`: (optional) Configuration for default worker

**Default worker config:**

```typescript
{
  concurrency?: number,          // Max concurrent tasks (default: 10)
  pollIntervalMs?: number,       // Poll interval (default: 1000)
  flushIntervalMs?: number,      // Flush interval (default: 2000)
  fetchBatchSize?: number,       // Fetch batch size (default: 10)
}
```

## orchestrator.start()

Start the orchestrator:

```typescript
await orchestrator.start();
```

- Initializes database schema (if needed)
- Starts all workers
- Sets up signal handlers (SIGTERM, SIGINT)
- Begins polling for executions

## orchestrator.stop()

Stop the orchestrator gracefully:

```typescript
await orchestrator.stop();
```

- Stops accepting new work
- Waits for current tasks to reach checkpoint
- Releases locked executions
- Cleans up database records
- Shuts down workers

## orchestrator.drain()

Process all queued tasks then stop:

```typescript
await orchestrator.drain();
```

- Starts the orchestrator
- Processes until queue is empty
- Automatically stops
- Returns when all work is done

**Use case:** Testing

```typescript
test("processes tasks", async () => {
  await orchestrator.drain();
  expect(results).toEqual(expected);
});
```

## orchestrator.stopped

Promise that resolves when orchestrator stops:

```typescript
await orchestrator.start();

// Wait for shutdown signal
await orchestrator.stopped;

await sql.end();
```

## orchestrator.info

Access orchestrator state:

```typescript
orchestrator.info.id              // Orchestrator ID (UUID)
orchestrator.info.isRunning       // boolean
orchestrator.info.isShuttingDown  // boolean
orchestrator.info.isStopped       // boolean
```

## What's Next?

- [Worker Configuration](worker-config.md) - Tune worker performance
- [Conductor API](conductor.md) - Task creation and invocation
- [Horizontal Scaling](../scaling/horizontal.md) - Run multiple workers

