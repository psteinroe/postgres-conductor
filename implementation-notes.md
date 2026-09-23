# Implementation Notes

Running notes for the custom-event pipeline implementation.

## Design decisions

- An emitted event is a short-lived execution on `pgconductor.internal` for `pgconductor.event-dispatch`. Its execution ID is the public event ID and its payload is `{ eventKey, payload }`; there is no separate event-log table.
- The hidden dispatcher uses the ordinary execution lifecycle for claiming, retries, recovery, settlement, drain, and shutdown. `event-dispatch-task.ts` owns its task definition and normal batch handler, and the orchestrator registers the shared task instance through the normal worker task list. Batch events retain `name` and `payload` and add read-only execution identity, allowing the handler to pass source IDs to SQL without special-casing Worker. User workers register before dispatcher fetching begins, so dispatch never sees a partially registered local worker set.
- Emission inserts only the dispatch execution and takes no event-key advisory lock. Application-owned PostgreSQL triggers can call `pgconductor.emit_event()` transactionally with their source change.
- Dispatch is one SQL statement that matches subscriptions and inserts destinations atomically. It filters by the current claim owner but takes no second source lock and writes no fan-out marker. Retries re-evaluate current subscriptions; the destination unique index deduplicates an existing `(source, subscription, queue)` delivery.
- Source completion removes the immediate-retention dispatch execution. Destinations remain because they have no foreign key to the source.
- Destination identity is `(parent_execution_id, subscription_id)`. `parent_execution_id` records source-event lineage, while non-null `subscription_id` excludes the delivery from workflow child settlement, orphaning, wake-up, and failure propagation.
- Each subscription is one AND-group. `_private_event_filter_terms` stores every typed atomic alternative; rows with the same field are OR predicates and distinct matched fields are ANDed. Multiple subscriptions preserve the outer OR without separate group or clause tables.
- The subscription stores only its required distinct-field count, not filter JSON or a dynamic match count. Empty filters have count zero and no terms.
- Dispatch uses one statement snapshot, probes operator-specific indexes, deduplicates matched fields, and retains only subscriptions whose matched-field count equals their required-field count. That statement projects payloads and atomically inserts destinations without materializing fan-out through TypeScript.
- Zod validates, sorts, and deduplicates each event trigger once when its `Task` is constructed. The task caches the flat typed term transport, and worker registration persists it without recompilation. Private SQL trusts this compiled policy and converts range bounds into indexed `numrange` values.
- Cascading foreign keys maintain the cold subscription → term relationship. Hot execution and destination lineage remain independent of subscription metadata.
- The in-memory client evaluates the same compiled typed terms to preserve test-double behavior; PostgreSQL is authoritative in production.
- Registration remains queue-scoped and atomic under the existing queue-row lock. Dispatch sees either the old or new committed subscription snapshot.

## Lifecycle invariants

- A matching or insertion error rolls back all destination inserts in the statement; normal dispatch retry applies.
- After fan-out commits but before the source settles, a crash can cause a retry against current subscriptions. Existing destination identities are deduplicated while new subscriptions may receive additional deliveries; an earlier zero-match pass may also deliver on retry. This is an intentional at-least-once fan-out contract.
- Destination completion or failure cannot complete, fail, cancel, or retain the source dispatch execution. A destination may still invoke a normal workflow child; that child has `subscription_id = null` and uses the existing workflow lifecycle.
- The source execution may be deleted immediately after successful settlement. Destination identity remains valid without source or subscription rows.
- Notifications are optional wake-up hints. Persisted executions and polling remain authoritative.

## Matching semantics

- Fields are AND clauses; scalar alternatives within a field are OR predicates.
- Scalar equality and `anything-but` are type-sensitive.
- Prefixes are literal, bounded to 64 characters, and never use `LIKE` semantics.
- Numeric range boundaries preserve strict versus inclusive comparisons.
- Missing values differ from JSON null; `exists` tests presence directly.
- `{}` is unconditional and is represented by `required_field_count = 0` with no term rows.
- Event names are limited to 255 UTF-8 bytes, field names to 128 bytes, and indexed scalar JSON text to 1,024 bytes.
- Event payloads and private filter transport values must be JSON objects at their database boundaries. Numeric predicates and SQL-emitted numeric payload fields are compared as PostgreSQL `numeric`, avoiding JavaScript precision loss during matching.

## Deviations

- Migrations are rewritten in place under the active-development policy. Databases created from earlier unreleased snapshots must be recreated.
- There is no independent event replay or custom-event retention layer. A future one-shot wait feature must define its own no-miss registration/replay protocol rather than inferring commit order from event IDs.

## Tradeoffs

- Exact, prefix, numeric-range, `exists: true`, and field-scoped `anything-but` probes use typed indexes. `exists: false` is inherently broad and scans the event key's absence terms because a missing field provides no positive lookup key.
- Full inverted matching writes more cold metadata rows and performs a matched-field aggregation, in exchange for removing the single-anchor selectivity heuristic and avoiding false-positive candidate filters crossing into TypeScript. Separate operator branches preserve typed index probes rather than shortening the query into a generic `OR` join. The one-use matching CTEs remain materialized because inlining slowed selective and ten-event shared-field probes in a Postgres 15 benchmark with 100,000 subscriptions.
- Queue registration keeps a delete-and-insert subscription snapshot rather than `merge`: Postgres 15 cannot delete rows missing from the source or return generated subscription IDs from `merge`, and incoming subscriptions have no stable identity. The SQL directly expands terms from one materialized prepared set.
- Ordinary claim ownership and settlement fencing protect the internal execution; fan-out reads only sources still owned by that orchestrator. As with other tasks, ownership is fenced by orchestrator ID rather than a claim-attempt token; same-owner reclaims remain a general execution-fencing edge case.
- Dispatcher batches remain 10. Fan-out is one atomic insert statement per batch and follows the ordinary Worker batch path.

## Open questions

- None.
