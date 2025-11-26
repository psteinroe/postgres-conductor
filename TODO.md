- refactor / fix cron scheduling to not rely on dedupe key gymnastics anymore
- bring column selection back
- add dynamic scheduling of cron jobs
- make sure database api makes it clear what is public api and what is private api (e.g. add `_private_` prefix)
- add management ui
- only use custom current_time if running tests
- add throttling (limit, period, key)
- add concurrency (limit, key)
- add rateLimit (limit, period, key)
- add debounce (period, key)
- iterate on batched processing, e.g. allow array payloads?

Links:
- https://planetscale.com/blog/the-slotted-counter-pattern

## Cron Scheduling

Every cron execution has a `cron_schedule` and a dedupe key that has the format `repeated::<user-key>::<next-exec-timestamp>`. When an execution is picked up that has `cron_schedule` set, we upsert the next execution before processing the job.

On `unschedule`, or when a worker registers without the cron trigger:
- delete future executions if not locked
- if there is a locked execution, cancel it

Only insert the next execution if the current one is not cancelled.

