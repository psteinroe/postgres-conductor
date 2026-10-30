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
- `tasks`: (optional) Array of tasks on the `default` queue, run by the default worker. A task on another queue throws; run it on a worker from `conductor.createWorker()`
- `workers`: (optional) Array of custom workers from `conductor.createWorker()`
- `defaultWorker`: (optional) Configuration for default worker

The Orchestrator is the registration authority for its workers. Configure at
most one worker for each queue (including the implicit `default` worker); put
all tasks for a queue on that worker. Multiple workers for one queue are
rejected rather than competing for registrations.

If the conductor has `events` configured, the Orchestrator also runs an internal
worker that dispatches emitted events to their subscribers.

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
- Opens a connection of its own for heartbeats and releases the executions of crashed orchestrators (see [Crash Recovery](../scaling/horizontal.md#crash-recovery))
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
- Gives up saving results or releasing executions when the database stays unreachable for about 10 seconds; `stopped` then rejects, and stale orchestrator recovery releases the executions it still held
- Leaves the database connection open, so the conductor stays usable and the orchestrator can be started again (see [`conductor.close()`](conductor.md#conductorclose))

## orchestrator.drain()

Process all queued tasks then stop:

```typescript
await orchestrator.drain();
```

- Starts the orchestrator
- Gives each worker one run-once pass
- Automatically stops after those passes finish; executions still running receive signals and cancellation until they do
- `stop()` interrupts it like a running orchestrator
- May leave work created after another queue's pass, such as event fan-out; call `drain()` again to process another pass

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

await conductor.close();
```

## orchestrator.info

Access orchestrator state:

```typescript
orchestrator.info.id              // Orchestrator ID (UUID)
orchestrator.info.isRunning       // boolean
orchestrator.info.shutdownSignal  // boolean, true once shutdown started
orchestrator.isShuttingDown       // boolean
orchestrator.isStopped            // boolean
```

## What's Next?

- [Worker Configuration](worker-config.md) - Tune worker performance
- [Conductor API](conductor.md) - Task creation and invocation
- [Horizontal Scaling](../scaling/horizontal.md) - Run multiple workers

