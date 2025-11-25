- refactor / fix cron scheduling to not rely on dedupe key gymnastics anymore
- add job cancellation
- bring column selection back
- add dynamic scheduling of cron jobs
- make sure database api makes it clear what is public api and what is private api (e.g. add `_private_` prefix)
- migrate database-client to args, signal pattern
- add management ui
- only use custom current_time if running tests
- add throttling (limit, period, key)
- add concurrency (limit, key)
- add rateLimit (limit, period, key)
- add debounce (period, key)
- iterate on batched processing, e.g. allow array payloads?

Links:
- https://planetscale.com/blog/the-slotted-counter-pattern

## Job Cancellation

Add `cancelled` flag to execution.

On cancelled:
- if job is already completed, do nothing
- if job is not locked, set failed_at
- if job is locked, set cancelled to true

Now, how do we cancel a running job?
- add cancel_signal to orchestrator, maybe cancelled queue, maybe cancelled executions (queue + ids)?
- on heartbeat, the orchestrator reads the signal and sets it to null
- it sends cancelled executions to worker
- worker need to track running tasks and abort them (--> refactor required)
- need to distinguish between `HANGUP` and `CANCELLED`

Refactor: for all steps, we currently update the execution already within the task (`saveStep`). This is unnecessary, we can return the info from the task to the worker, and use the `released` type. The same refactor can also be used for handling cancelled tasks, which will be returned as failed. Problem, updating the execution and writing the step must happen at the same time. We need to distinguish between steps that update the execution (`sleep`, `waitFor`, `invoke`), and steps that are just checkpoints (`.step`). The former need to be handled by `returnExecutions`, the latter is handled within the task itself.

With the refactoring complete, we can simply abort the task, and return `cancelled` instead of `hangup` to set it to `failed` instead of `released`.

## Cron Scheduling

Every cron execution has a `cron_schedule` and a dedupe key that has the format `repeated::<user-key>::<next-exec-timestamp>`. When an execution is picked up that has `cron_schedule` set, we upsert the next execution before processing the job.

On `unschedule`, or when a worker registers without the cron trigger:
- delete future executions if not locked
- if there is a locked execution, cancel it

Only insert the next execution if the current one is not cancelled.

