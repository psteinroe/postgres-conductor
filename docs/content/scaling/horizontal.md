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
order by priority, run_at
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

`SKIP LOCKED` keeps workers from waiting on each other's rows, but every worker scans the same head of the queue and has to step over the rows others hold. With many workers, each claim does more work for fewer rows.

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

## One Queue, One Codebase

Every orchestrator that polls a queue must run the same task set. Replicas of one service share its queues, and different versions only overlap briefly during a rolling deploy. Separate services must use separate queues, the same constraint as Temporal task queues.

The rule exists because registration owns the whole queue. When a worker starts, it registers its tasks and replaces the queue's cron schedules and event subscriptions with the ones it declares. Schedules and subscriptions of tasks it does not run are removed, so a task deleted from the codebase is cleaned up by the next deploy. Two services sharing a queue would remove each other's schedules and subscriptions on every start.

**Rolling deploys:** while old and new replicas overlap, an old replica that restarts registers the old task set again. That removes the cron schedules and event subscriptions that only the new version declares, until a replica of the new version starts again. If that happens after the rollout has finished, restart one replica of the new version.

## Crash Recovery

Every orchestrator heartbeats every 5 seconds. An orchestrator without a heartbeat for 30 seconds is stale: the next orchestrator to heartbeat removes it and releases its executions, which other workers then claim and retry. A crashed process's executions run again within about 35 seconds. An orchestrator also recovers stale ones when it starts, so this works when every process crashed too.

Heartbeats run on a connection of the orchestrator's own, so handlers that hold every connection of the pool, such as long transactions, do not delay them. Each orchestrator keeps this one extra connection open.

A heartbeat can still be late when the process is blocked: a handler that keeps the event loop busy for 30 seconds makes its orchestrator look stale. Other orchestrators then recover it, its handlers are aborted, its executions run again elsewhere, and it stops. Keep CPU-heavy work off the event loop, for example in worker threads.

After a database outage, every orchestrator's last heartbeat is old. An orchestrator recovers others only once it has heartbeated without failure for 30 seconds, so the others have that long to reconnect.

## When to Scale Horizontally

**Scale up (more processes) when:**

- Queue depth is consistently high
- Workers are CPU-bound

**Scale out (more queues) when:**

- Lock contention is high (many workers on same queue)
- Different task types have different resource needs
- You want independent scaling per workload type

## What's Next?

- [Worker Configuration](../api/worker-config.md) - Tune worker performance
- [Maintenance Task](maintenance.md) - Database housekeeping
- [Live Migrations](live-migrations.md) - Zero-downtime deployments
