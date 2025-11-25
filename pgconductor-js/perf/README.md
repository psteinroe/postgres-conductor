# PgConductor Performance Tests

This directory contains two types of performance testing tools:

## Directory Structure

```
perf/
├── query-analysis/      # SQL query optimization and EXPLAIN ANALYZE tools
│   ├── compare-plans.ts # Compare query plans across scenarios
│   ├── scenarios.ts     # Test scenarios with different data distributions
│   ├── extract-queries.ts # Extract queries for manual analysis
│   └── lib/
│       └── fixtures.ts  # Test database fixtures
│
└── throughput/          # End-to-end throughput benchmarking
    ├── run.ts           # Main coordinator (spawns worker processes)
    ├── worker.ts        # Worker process implementation
    ├── init.ts          # Database setup and task queuing
    ├── tasks.ts         # Task definitions and implementations
    ├── scenarios.ts     # Performance test scenarios configuration
    └── setup.ts         # Testcontainer management (PostgreSQL)
```

---

## Throughput Testing

Measures end-to-end throughput using child processes for true parallelism.

### Quick Start

**Zero setup required!** Uses testcontainers to automatically start PostgreSQL in Docker.

```bash
# Run with default scenario (4 workers, 10k noop tasks)
AGENT=1 bun perf/throughput/run.ts default

# Run quick smoke test
AGENT=1 bun perf/throughput/run.ts quick

# Run high-throughput scenario
AGENT=1 bun perf/throughput/run.ts high-throughput

# List all available scenarios
AGENT=1 bun perf/throughput/run.ts invalid-name
```

**First run:** Downloads postgres:16 image and starts container (~30s)
**Subsequent runs:** Reuses existing container (instant startup)

**Using your own PostgreSQL:**
```bash
DATABASE_URL=postgres://localhost/pgconductor_perf AGENT=1 bun perf/throughput/run.ts quick
```

### Available Scenarios

All scenarios are defined in `perf/throughput/scenarios.ts`:

- **default** - Balanced settings (4 workers, 10k noop tasks)
- **quick** - Quick smoke test (2 workers, 100 tasks)
- **high-throughput** - Aggressive batching (8 workers, 100k tasks)
- **low-latency** - Frequent polling/flushing (10ms intervals)
- **cpu-bound** - CPU-intensive fibonacci tasks
- **io-bound** - I/O-bound async tasks (100 concurrency)
- **with-steps** - Step memoization testing
- **scale-1**, **scale-2**, **scale-4**, **scale-8** - Worker scalability comparison

Each scenario configures:
- Number of workers
- Number of tasks
- Task type (noop, cpu, io, steps)
- Worker settings (poll interval, flush interval, concurrency, batch size)

### Requirements

- **Docker** - Required for testcontainers (automatic PostgreSQL)
- **Bun** - Runtime for TypeScript execution

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | (auto-testcontainer) | Optional: Use specific PostgreSQL instance |

### Architecture

The test suite uses child processes to achieve true parallelism:

```
throughput/run.ts (coordinator)
  ↓ spawns N worker processes
  ├─> worker.ts (Process 1)
  ├─> worker.ts (Process 2)
  ├─> worker.ts (Process 3)
  └─> worker.ts (Process 4)
```

Each worker:
1. Creates its own Orchestrator instance
2. Runs `orchestrator.drain()` to process queued tasks
3. Uses `started` and `stopped` promises for accurate timing
4. Reports metrics back to coordinator

### Timing Methodology

#### Startup Time
Measured by running a single worker against an empty queue:
- Schema initialization
- Worker registration
- Pipeline setup

#### Execution Time
Measured using the new `started`/`stopped` APIs:
```typescript
void orchestrator.drain();       // Kick off drain
await orchestrator.started;      // Wait for startup
const execStart = Date.now();    // Start measuring
await orchestrator.stopped;      // Wait for completion
const execTime = Date.now() - execStart;
```

This ensures we only measure actual task processing time, not startup overhead.

### Example Output

```
=== PgConductor Performance Test ===
Workers: 4
Tasks: 10000
Task Type: noop

Setting up database...
✓ Database ready (1234ms)

Queuing 10000 tasks...
✓ Queued in 2342ms
  Rate: 4269.72 tasks/sec

Measuring startup overhead...
✓ Startup: 156ms

Processing 10000 tasks with 4 workers...

=== Results ===
Total time: 3456ms
Tasks processed: 10000/10000

Timing breakdown:
  Avg worker startup: 145ms
  Max execution time: 3298ms

Throughput:
  Overall: 2894.17 tasks/sec
  Per worker: 723.54 tasks/sec

Per-worker breakdown:
  Worker 0: 2534 tasks in 3298ms (768.31 tasks/sec)
  Worker 1: 2489 tasks in 3289ms (756.69 tasks/sec)
  Worker 2: 2501 tasks in 3301ms (757.65 tasks/sec)
  Worker 3: 2476 tasks in 3285ms (753.66 tasks/sec)
```

### Interpreting Results

#### Throughput
- **Overall tasks/sec**: Total throughput across all workers
- **Per-worker tasks/sec**: Average throughput per worker
- Compare different configurations to find optimal worker count

#### Scalability Testing
Test with different worker counts:
```bash
for scenario in scale-1 scale-2 scale-4 scale-8; do
  echo "Running $scenario..."
  AGENT=1 bun perf/throughput/run.ts $scenario
done
```

#### Task Complexity Comparison
Compare different task types:
```bash
for scenario in default cpu-bound io-bound with-steps; do
  echo "Running $scenario..."
  AGENT=1 bun perf/throughput/run.ts $scenario
done
```

### Creating Custom Scenarios

Edit `perf/throughput/scenarios.ts` to add new scenarios:

```typescript
export const scenarios = {
  "my-scenario": {
    workers: 8,
    tasks: 50000,
    taskType: "noop",
    workerSettings: {
      pollIntervalMs: 50,
      flushIntervalMs: 50,
      concurrency: 20,
      fetchBatchSize: 200,
    },
  },
  // ... other scenarios
} as const satisfies Record<string, Scenario>;
```

---

## Query Analysis

Tools for analyzing SQL query performance using EXPLAIN ANALYZE.

### Quick Start

```bash
# Run comparison across all scenarios
bun perf/query-analysis/compare-plans.ts

# Extract queries for manual analysis
bun perf/query-analysis/extract-queries.ts
```

### Scenarios

The query analysis suite tests different data distributions:

- **heavy-single-queue**: 1 queue, 1M pending tasks
- **many-queues-sparse**: 100 queues, 500 tasks total
- **with-steps**: Tasks with step memoization

For each scenario, it tests various operations:
- **batch-N**: Fetching N executions
- **batch-N-filtered**: Fetching with task key filters
- **N-completed**: Returning N completed results
- **mixed-N**: Mixed result types

### Output

Results are saved to `perf/to-analyse/` directory with:
- Query plans in JSON format
- Planning and execution times
- Buffer usage statistics
- Recommendations for optimization

---

## Future Enhancements

- [ ] OpenTelemetry instrumentation package
- [ ] Latency percentiles (p50, p95, p99)
- [ ] JSON output for CI/CD
- [ ] Grafana dashboard
- [ ] Regression detection
- [ ] More task types (invoke chains, sleep)
