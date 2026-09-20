# Implementation Notes

Running notes for the custom-event pipeline implementation.

## Design decisions

- An emitted event is a short-lived execution on `pgconductor.internal` for `pgconductor.event-dispatch`. Its execution ID is the public event ID and its payload is `{ eventKey, payload }`; there is no separate event-log table.
- The hidden dispatcher uses the ordinary execution lifecycle for claiming, retries, recovery, settlement, drain, and shutdown. User workers register before dispatcher fetching begins, so dispatch never sees a partially registered local worker set.
- Emission inserts only the dispatch execution and takes no event-key advisory lock. Application-owned PostgreSQL triggers can call `pgconductor.emit_event()` transactionally with their source change.
- A reserved `_private_steps` row, `pgconductor.internal.event-fanout.v1`, is the atomic fan-out commit marker. TypeScript holds the source locks while destination and marker insertion commit in the same transaction. A retry that sees the marker completes without re-reading subscriptions, including zero-destination events.
- Source completion removes the immediate-retention dispatch execution and cascades marker removal. Destinations remain because they have no foreign key to the source.
- Destination identity is `(parent_execution_id, subscription_id)`. `parent_execution_id` records source-event lineage, while non-null `subscription_id` excludes the delivery from workflow child settlement, orphaning, wake-up, and failure propagation.
- Each subscription stores one canonical filter object directly. Fields are AND clauses and each field's alternatives are OR predicates supporting exact scalars, literal prefixes, numeric ranges, presence, and atomic `anything-but`.
- Registration deterministically chooses one complete candidate-safe field by operator rank and field name. Only that field's exact, prefix, or numeric-range alternatives are stored in `_private_event_filter_anchors`; filters without a safe field store one event-key fallback anchor.
- Dispatch uses one SQL statement to lock/fence sources, inspect markers, and perform typed indexed anchor probes. It returns one row per candidate subscription rather than one row per predicate. TypeScript evaluates the already-canonical filter directly, then one SQL statement atomically inserts destinations and markers.
- Zod validates and canonicalizes each event trigger once when its `Task` is constructed. The compiled subscription and anchors are cached on the task and worker registration persists them without recompilation. The private SQL trusts this compiled policy and only converts anchor transport values into indexed typed columns.
- A cascading foreign key maintains the cold subscription → anchor relationship. Hot execution and destination lineage remain independent of subscription metadata.
- The in-memory client reuses the production filter matcher instead of maintaining a second implementation.
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
- Event payloads and private filter transport values must be JSON objects at their database boundaries. Because final matching uses JavaScript numbers, top-level numeric fields emitted directly from SQL must round-trip through PostgreSQL `double precision` without changing their decimal value.

## Deviations

- Migrations are rewritten in place under the active-development policy. Databases created from earlier unreleased snapshots must be recreated.
- There is no independent event replay or custom-event retention layer. A future one-shot wait feature must define its own no-miss registration/replay protocol rather than inferring commit order from event IDs.

## Tradeoffs

- Presence and `anything-but` are residual-only operators. A filter without a complete exact, prefix, or numeric-range candidate field intentionally uses the event-key-local fallback index.
- Canonical filters remain JSONB because TypeScript owns final matching. Only candidate anchors are relational and typed; fully normalizing residual predicates would duplicate the matcher and increase query results.
- Combining source locking and candidate lookup into one SQL statement can use the statement's pre-wait snapshot during a concurrent dispatch. The commit statement rechecks the marker before inserting destinations, so a dispatcher that waited for an already-committed fan-out cannot extend its frozen destination set.
- Dispatcher batches remain 10. Fan-out is one atomic transaction per batch and reuses the existing hidden Worker path.

## Open questions

- None.
