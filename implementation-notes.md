# Implementation Notes

Running notes on how the event-pipeline rewrite interprets the agreed design.

## Design decisions

- Custom events are immutable rows in `_private_custom_events`. The row ID is also the ID of a short-lived execution on the reserved `pgconductor.internal` queue for task `pgconductor.event-dispatch`.
- The event row is the durable event body and replay source. The internal execution provides scheduling, claim fencing, retry, settlement, drain, and shutdown behavior only; deleting that execution does not delete the event.
- A hidden worker uses the ordinary execution lifecycle. User workers register first so the first successful dispatch sees a complete committed subscription snapshot.
- Emission performs only the event-log and dispatch-execution inserts. It does not take a per-key advisory lock, so unrelated application transactions and hot event keys are not serialized for a downstream feature that is not implemented in this PR.
- Fan-out is claim-fenced and atomically inserts destinations and sets `dispatched_at`. A retry of a committed dispatch returns the already-dispatched event, including zero-match events.
- Event destinations use dedicated `source_event_id` and `event_subscription_id` columns. They do not use `parent_execution_id`, so workflow child/orphan semantics remain separate.
- Persistent task subscriptions use two tables: one canonical filter row and exact scalar predicate rows for every field alternative. Dispatch expands scalar event fields, probes the exact-predicate index, and keeps subscriptions whose distinct matched fields equal their stored field count. The normalized predicates are authoritative; dispatch does not re-evaluate the canonical JSON filter.
- Filter semantics are AND across fields and OR across alternatives. Equality is scalar and type-sensitive; a missing field differs from JSON `null`; `{}` is unconditional.
- TypeScript performs catalog and policy validation, canonical field ordering, and type-sensitive alternative deduplication. PostgreSQL derives `field_count` from the stored filter and retains row-shape, scalar, byte-size, and transactional integrity checks.
- Registrations lock worker and dead-letter queues in one global order, then call a dedicated queue-scoped subscription replacement function. Replacement and exact-predicate compilation are set-based; route locks, route-wide subscription counts, and procedural group/clause compilation were removed.
- Managed database-trigger subscriptions remain out of scope. Applications own PostgreSQL triggers and call `pgconductor.emit_event()` transactionally.
- Dispatched events have a seven-day default retention policy. The always-present internal worker owns global cleanup, including named-queue-only deployments. Cleanup is bounded and skips any event whose internal dispatch execution is still active, preventing cleanup from racing recovery after fan-out committed but source settlement did not.

## Benchmark evidence

Throwaway PostgreSQL benchmarks compared the previous normalized four-table matcher with two simpler designs at 10,000 subscriptions per event key, batches of 1 and 10, selective, broad, and unfiltered workloads, plus selective 100,000-subscription evidence.

- A single subscription row plus residual JSONB matching was rejected: selective batch-10 matching regressed from roughly 6 ms to roughly 937 ms.
- An anchor row plus residual JSONB matching improved registration but remained about twice as slow in matching and was rejected.
- The selected flattened exact matcher reduced registration to roughly 43–68% of normalized time. Selective batch-10 matching was roughly 15–19 ms versus roughly 12 ms normalized; broad batch-10 matching was roughly twice as fast, and unfiltered matching roughly four to six times faster.
- At 100,000 subscriptions, flattened selective matching remained bounded rather than exhibiting the full-scan behavior of the residual-only model.

These measurements are architecture evidence, not throughput or capacity claims. All benchmark code and generated artifacts were removed.

## Deviations and tradeoffs

- Migrations are rewritten in place under the repository's active-development policy. Databases created from earlier unreleased migration snapshots must be recreated; this PR intentionally does not add a compatibility migration.
- The schema supports only the scalar equality semantics exposed by the typed API. Prefix, range, negative, and fallback operators were intentionally not added.
- `LISTEN/NOTIFY` was not added. Persisted executions and polling remain authoritative.
- Dispatcher batches remain 10. Fan-out is one atomic transaction per batch; this bounds ordinary retry amplification without introducing chunk-progress state.
- Indexed filter values are limited to 1,024 UTF-8 bytes, event keys to 255 bytes, and field names to 128 bytes so the composite B-tree index cannot exceed PostgreSQL tuple limits.
- Event positions may contain gaps and do not imply commit order. This PR intentionally does not impose a `waitForEvent` linearization protocol on every emitter.

## Downstream `waitForEvent`

Reusable primitives are the append-only event log, canonical filter representation, immutable matcher, and exact-predicate indexing pattern. One-shot waits still require a separate subscription table, timeout/cancellation lifecycle, and an explicit no-miss registration boundary; persistent task subscriptions and their queue-snapshot replacement function are not overloaded for that purpose.
