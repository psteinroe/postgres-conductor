# Performance Benchmarks

Benchmark suite for measuring pgconductor performance.

## Running Benchmarks

```bash
# Run all benchmarks
pnpm perf

# Run specific category
pnpm perf throughput
pnpm perf scaling
pnpm perf concurrency

# Run specific scenario
pnpm perf throughput/simple-100

# Output as JSON
pnpm perf --json
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
  run: pnpm perf --json > results.json
```
