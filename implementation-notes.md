# Implementation Notes

Running notes for the custom-event pipeline implementation.

## Design decisions

- An emitted event is a short-lived execution on `pgconductor.internal` for `pgconductor.event-dispatch`. Its execution ID is the public event ID and its payload is `{ eventKey, payload }`; there is no separate event-log table.
- The hidden dispatcher uses the ordinary execution lifecycle for claiming, retries, recovery, settlement, drain, and shutdown. User workers register before dispatcher fetching begins, so dispatch never sees a partially registered local worker set.
- Emission inserts only the dispatch execution and takes no event-key advisory lock. Application-owned PostgreSQL triggers can call `pgconductor.emit_event()` transactionally with their source change.
- A reserved `_private_steps` row, `pgconductor.internal.event-fanout.v1`, is the atomic fan-out commit marker. TypeScript holds the source locks while destination and marker insertion commit in the same transaction. A retry that sees the marker completes without re-reading subscriptions, including zero-destination events.
- Source completion removes the immediate-retention dispatch execution and cascades marker removal. Destinations remain because they have no foreign key to the source.
- Destination identity is `(parent_execution_id, subscription_id)`. `parent_execution_id` records source-event lineage, while non-null `subscription_id` excludes the delivery from workflow child settlement, orphaning, wake-up, and failure propagation.
- Persistent filters are normalized as subscription OR-groups, group AND-clauses, and clause OR-predicates. The public API compiles one group per filter object and one clause per field. Predicates support exact scalars, literal prefixes, numeric ranges, presence, and atomic `anything-but`.
- Registration deterministically chooses the safest complete anchor clause by operator rank and field name. Every alternative in that clause must support exact, prefix, or numeric-range candidate lookup. Groups with only presence/negative clauses and empty filters are explicit fallback groups.
- Predicates store an operator and scalar discriminator plus typed text, numeric, boolean, prefix-length, or `numrange` operands. JSON null has its own discriminator and no SQL operand. Missing fields therefore remain distinct from JSON null.
- Dispatch uses SQL for operator-specific indexed anchor probes, including bounded literal event-prefix expansion, then verifies the returned normalized candidate predicates in TypeScript. Destination and marker writes remain atomic in the same transaction. No JSONB predicate values, stored match counts, or hashes are used.
- Zod validates event catalog policy, limits, operators, and scalar values before TypeScript canonicalizes field ordering and type-sensitive scalar alternatives. The private registration SQL converts that transport value into normalized relational rows set-wise.
- Cascading foreign keys maintain the cold subscription → group → clause → predicate hierarchy. Hot execution and destination lineage remain independent of that hierarchy.
- Registration remains queue-scoped and atomic under the existing queue-row lock. Dispatch sees either the old or new committed subscription snapshot.

## Lifecycle invariants

- Before fan-out commits, any matching or insertion error rolls back destinations and the marker; normal dispatch retry applies.
- After fan-out commits, the marker freezes the matched subscription set. Recovery retries complete from the marker without duplicating or changing destinations.
- Destination completion or failure cannot complete, fail, cancel, or retain the source dispatch execution. A destination may still invoke a normal workflow child; that child has `subscription_id = null` and uses the existing workflow lifecycle.
- The source execution may be deleted immediately after successful settlement. Destination identity remains valid without source or subscription rows.
- Notifications are optional wake-up hints. Persisted executions and polling remain authoritative.

## Matching semantics

- Fields are AND clauses; scalar alternatives within a field are OR predicates.
- Scalar equality and `anything-but` are type-sensitive.
- Prefixes are literal, bounded to 64 characters, and never use `LIKE` semantics.
- Numeric range boundaries preserve strict versus inclusive comparisons.
- Missing values differ from JSON null; `exists` tests presence directly.
- `{}` is unconditional and uses the explicit fallback path.
- Event names are limited to 255 UTF-8 bytes, field names to 128 bytes, and indexed scalar JSON text to 1,024 bytes.
- Event payloads and private filter transport values must be JSON objects at their database boundaries.

## Deviations and tradeoffs

- Migrations are rewritten in place under the active-development policy. Databases created from earlier unreleased snapshots must be recreated.
- Presence and `anything-but` are residual-only operators. A group that has no complete exact, prefix, or numeric-range anchor clause is intentionally routed through the event-key-local fallback index.
- There is no independent event replay or custom-event retention layer. A future one-shot wait feature must define its own no-miss registration/replay protocol rather than inferring commit order from event IDs.
- Dispatcher batches remain 10. Fan-out is one atomic transaction per batch and reuses the existing hidden Worker path.
