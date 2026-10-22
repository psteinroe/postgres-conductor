# Maintenance & Cleanup

Postgres Conductor runs automatic maintenance on each queue to keep the database clean.

## Automatic Maintenance

Maintenance runs daily on each queue:

- Removes old completed executions (based on task retention config)
- Removes old failed executions (based on task retention config)
- Fails pending executions of tasks that the queue's workers no longer run

## Executions of Removed Tasks

Every worker that starts registers its tasks, and that set is the queue's task set (see [Horizontal Scaling](horizontal.md#one-queue-one-codebase)). When a deploy removes a task, its pending executions, such as retries, sleeping workflows or event deliveries, can no longer run. The same holds for executions of a task that was never registered on the queue, for example one invoked with a typo in its name.

Maintenance fails these executions with the error `task <key> is not registered on queue <queue>`, once the task has been missing from the queue's registrations for a day, or, if it was never registered, once the execution is a day old. A day covers a rolling deploy, during which replicas of the previous version still run the task, and a rollback, which registers the task again and keeps its executions.

They fail like any other execution that fails permanently: the task's dead-letter queue receives them, `ctx.waitForResult()` and `conductor.waitForResult()` see the failure, and workflows waiting on them in `ctx.invokeAndWait()` fail too. Executions still running on a replica of the previous version are left alone.

Maintenance runs only on queues with a running worker, so executions of a queue that no worker polls yet stay pending.

Custom events use ordinary internal executions and therefore do not require a separate event-log cleanup pass.

## What's Next?

- [Worker Configuration](../api/worker-config.md) - Optimize worker performance
- [Horizontal Scaling](horizontal.md) - Add more workers
- [Live Migrations](live-migrations.md) - Zero-downtime deployments
