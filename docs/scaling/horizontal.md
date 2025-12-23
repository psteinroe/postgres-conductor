# Horizontal Scaling

Scale task processing by running multiple workers and partitioning work across queues.

## Understanding Partitioning

All executions live in a single Postgres table: `pgconductor._private_executions`. The `queue` column logically partitions this table.

**How workers claim executions:**

```sql
-- Simplified version of what each worker does
select * from pgconductor._private_executions
where queue = 'default'           -- Filter to this queue's partition
  and locked_at is null           -- Not already claimed
  and run_at <= now()             -- Ready to run
order by priority desc, run_at
limit 10
for update skip locked;           -- Lock rows, skip if another worker grabbed them
```

**The contention problem:**

When multiple workers process the **same queue**, they all fight over the same rows:

```typescript
// 10 workers all polling WHERE queue = 'default'
// All trying to lock the same subset of rows
// High row-level lock contention in Postgres
const orchestrator = Orchestrator.create({
  conductor,
  tasks: [task1, task2, task3], // All use default queue
});

// Deploy with 10 replicas - workers compete for locks!
```

Each worker's `FOR UPDATE SKIP LOCKED` query blocks on rows that other workers are trying to lock. Postgres has to coordinate who gets which rows. With many workers, this creates significant contention.

**The partitioning solution:**

Once you hit scalability issues with the default queue, you can migrate some tasks to separate queues using the `createWorker` API. Since each queue is a separate partition of the executions table, workers on different queues lock different rows:

```typescript
// Workers on "fast" queue: WHERE queue = 'fast'
const fastWorker = conductor.createWorker({
  queue: "fast",
  tasks: [fastTask],
  config: { concurrency: 10 },
});

// Workers on "slow" queue: WHERE queue = 'slow'
// These workers never see "fast" rows - zero contention
const slowWorker = conductor.createWorker({
  queue: "slow",
  tasks: [slowTask],
  config: { concurrency: 2 },
});

const orchestrator = Orchestrator.create({
  conductor,
  workers: [fastWorker, slowWorker],
  tasks: [task1, task2, task3] // You can use the default queue alongside workers
});
```

**Why this helps:**

- Each queue has its own partition of the executions table
- Workers on different queues don't compete for locks
- You can scale each queue independently based on its workload

## When to Scale Horizontally

**Scale up (more processes) when:**

- Queue depth is consistently high
- Workers are CPU-bound

**Scale out (more queues) when:**

- Lock contention is high (many workers on same queue)
- Different task types have different resource needs
- You want independent scaling per workload type

## What's Next?

- [Worker Configuration](worker-config.md) - Tune worker performance
- [Maintenance Task](maintenance.md) - Database housekeeping
- [Live Migrations](live-migrations.md) - Zero-downtime deployments
