# Monitoring

## Queue Stats

`pgconductor.queue_stats` reports the executions of every queue:

```sql
select * from pgconductor.queue_stats;
```

| Column | Description |
| --- | --- |
| `queue` | Queue name |
| `ready` | Executions due to run and not claimed by a worker |
| `oldest_ready_age` | How long the oldest ready execution has been due, null when none is ready |
| `running` | Executions claimed by a worker |
| `suspended` | Executions scheduled for later: sleeping, waiting on a child, event, result or signal, backing off before a retry, or delayed |
| `failed_last_hour` | Executions that failed permanently in the last hour, not counting cancellations or executions already removed by their [retention policy](../crafting-tasks/retention.md) |

A growing `ready` count or `oldest_ready_age` means workers do not keep up with the queue: add workers or raise their concurrency (see [Horizontal Scaling](horizontal.md)). Ready executions of a task outside its [time window](../task-execution/time-windows.md) or at its [concurrency](../task-execution/concurrency.md) limit also wait here.

Each count reads a partial index, so the view stays cheap however many completed and failed executions are kept: its cost grows with the executions that have not finished yet and with the failures of the last hour. With a million executions, of which 50,000 are not finished, it reads those 50,000 rows and the last hour's failures, about 25 ms on a laptop.

For a single execution, [`conductor.getExecution()`](../api/conductor.md#conductorgetexecution) reports whether it is waiting and for what.

## What's Next?

- [Horizontal Scaling](horizontal.md) - Add more workers
- [Maintenance Task](maintenance.md) - Database housekeeping
