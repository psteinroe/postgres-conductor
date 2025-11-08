1. Build simple Postgres queue with workflows and steps support.

Goals:
- make sure it scales well. use table-per-queue and time-based partitioning (like pgmq), or partition just by queue (like pg-boss).
- inspiration: pg-boss, pgmq, DBOS
- add benchmarks
- support features like group id (check graphile worker for reference)
- add adapter for both server-only and serverless
- shared functions schema still cool?

the core issue is that for `invoke()`, we want to have a typed function with typed response, but for event-based triggers we want to get the types from the event, and have an optional common type for the function that they trigger...

API:

```ts
const client = createClient({
    db: {},
    schemas: {}
});


// option 1:
// pro: super nice dx
// con: payload will be union type
client.createFunction('my-workflow', {
    [
        { cron: '' },
        { event: '' },
    ],
}, async (payload, ctx) => {
    await ctx.step('my-step', async () => {})
});

// option 2:
// pro: better type-safety
// con: case where we have one function triggered by trigger and manually will end up in bad dx (and not-so-great observability
client.createFunction('my-workflow', { cron: '' }, async (payload, ctx) => {
    await ctx.step('my-step', async () => {})
});

// idea:
client.createFunction('my-workflow', {
    [
        // convert payload to common schema? feels weird.
        { cron: '', convert: () => p },
        { event: '', convert: (p) => p },
    ],
}, async (payload, ctx) => {
    await ctx.step('my-step', async () => {})
});
```
- simple Postgres queue with partitioning and all the features we need (priority, grouping, fifo, job id). Check pg boss for inspo. Check if we can use pgmq / partman
- Test performance, maybe add local queues like graphile worker has them
- Check if we can make it work serverless  (fetch api?)
- add events feature using cdc. Check chatgpt chat for design. Single rust binary.
- We can use the rust binary to ping server less if required
