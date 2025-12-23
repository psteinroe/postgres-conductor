# Performance Benchmarks

Benchmark suite for measuring pgconductor performance.

## Running Benchmarks

```bash
# Run all benchmarks
bun perf/run.ts

# Run specific category
bun perf/run.ts throughput
bun perf/run.ts scaling
bun perf/run.ts concurrency

# Run specific scenario
bun perf/run.ts throughput/simple-100

# Output as JSON
bun perf/run.ts --json
```

## Available Benchmarks

### Throughput

- `throughput/simple-100` - 100 simple tasks with no steps
- `throughput/with-steps-100` - 100 tasks with step memoization

### Scaling

- `scaling/1-queue-1000` - 1000 tasks on single queue (measures lock contention)
- `scaling/4-queues-1000` - 1000 tasks distributed across 4 queues (partitioned)

### Concurrency

- `concurrency/no-limit-100` - 100 tasks with no task-level concurrency limit
- `concurrency/limit-10-100` - 100 tasks with concurrency limit of 10

## Adding New Benchmarks

Edit `perf/benchmarks.ts`:

```typescript
export function createBenchmarks(ctx: BenchmarkContext) {
  const tasks = createTasks(ctx);

  return {
    "my-category/my-benchmark": {
      orchestrator: Orchestrator.create({
        conductor: ctx.conductor,
        tasks: [tasks.myTask],
      }),
      invoke: async () => {
        await Promise.all(
          Array.from({ length: 100 }, () =>
            ctx.conductor.invoke({ name: "my-task" }, {})
          )
        );
      },
    },
  };
}
```

## How It Works

1. **Setup** (not measured) - Creates database, installs schema
2. **Invoke** (not measured) - Queues all tasks to process
3. **Drain** (measured) - Processes all queued tasks and measures time
4. **Cleanup** - Tears down database

## CI Integration

```yaml
# .github/workflows/benchmark.yml
- name: Run benchmarks
  run: bun perf/run.ts --json > results.json
```
