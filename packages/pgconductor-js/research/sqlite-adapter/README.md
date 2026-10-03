# SQLite research prototype for Postgres Conductor

Throwaway evidence only: no public DatabaseClient integration, production guarantees,
commits, or changes outside this directory. Bun 1.4.2 reports SQLite **3.53.2**.

## Reproduce (from repository root)

```sh
AGENT=1 bun test packages/pgconductor-js/research/sqlite-adapter/store.test.ts
AGENT=1 bun node_modules/.bin/tsc -p packages/pgconductor-js/research/sqlite-adapter/tsconfig.json
AGENT=1 bun run oxlint --type-aware --deny-warnings packages/pgconductor-js/research/sqlite-adapter
AGENT=1 bun packages/pgconductor-js/research/sqlite-adapter/features.ts
AGENT=1 mkdir -p packages/pgconductor-js/research/sqlite-adapter/.runs
AGENT=1 bwrap --ro-bind / / --bind "$PWD/packages/pgconductor-js/research/sqlite-adapter/.runs" /tmp --dev /dev --proc /proc --setenv AGENT 1 --setenv SQLITE_STRESS_DIR /tmp -- bun "$PWD/packages/pgconductor-js/research/sqlite-adapter/stress.ts"
```

The private bind mount puts the WAL file at `/tmp/<P>-<mode>/stress.sqlite` for every
process, with physical backing files exclusively inside the declared write scope.
Without bubblewrap, the harness defaults to the scoped `.runs` directory. It removes
only each run's own database, WAL, and per-worker logs after verification. Mounting
`/dev` and `/proc` is needed: the first attempt without these crashed Bun before
executing the benchmark. That failed launch is not included in the measurements.

## Results and statement counts

`test-results.txt`: 16 pass, 0 fail, 124 assertions. Scoped typecheck and type-aware
lint pass (0 warnings/errors). `feature-results.json` and `stress-results.json`
contain the raw evidence. Stress takes 11.53 seconds including seeding/verification:
20,000 executions/configuration, 10% concurrency-limited to 5, claim batch 50.
Timing includes process startup, logging, invariant checks and yielding between
claim/settle; excludes seeding and final verification. No simulated task work.
Each run has zero duplicate/missing claims, 20,000 completions with attempts = 1,
and maximum limited-task occupancy = 5. The invariant check executes inside every
claim transaction, while holding the writer lock, rather than sampling externally.

| P | synchronous | executions/s | seconds | SQLITE_BUSY |
|---|---|---:|---:|---:|
| 1 | normal | 14268 | 1.402 | 0 |
| 2 | normal | 12944 | 1.545 | 1 |
| 4 | normal | 10716 | 1.866 | 0 |
| 8 | normal | 8959 | 2.232 | 0 |
| 1 | full | 5616 | 3.561 | 0 |

Counts below exclude `begin immediate` / `commit` (add 2), connection pragmas and
schema setup. `lastStatementCount` measures application statements, including reads.

- Claim without configured limits: **2** (read task configuration, claim update).
- Limited claim: **K + 3** for a nonempty claim, where K is the number of requested,
  registered tasks with free task capacity (configuration, active counts, K candidate
  reads, update). Empty claims omit the update. Stress K=2 gives **5**, plus the
  invariant checker = **6**. Candidates follow the same bounded-batch/group-ranking
  underfill behavior as Postgres, not an exhaustive fill-to-batch algorithm.
- Settle ordinary completions: **3**; completion waking a parent with a step: **5**.
- Retained permanent failure plus any-depth retained ancestor cascade: **3**;
  mixed removed/retained failures: **4**. Recursive ancestor reads are one statement.
- Worst mixed settlement: **15**, asserted in the tests: validation 1, completions
  including wake/steps/orphan/delete/update 6, cascade read/delete/update 3, retry 1,
  release step/update 2, child insert/parent update 2. Including boundaries: **17**.
- Stale-only settlement: **1**, no mutations; empty input: **0**.

## SQLite findings

`features.ts:24-51` empirically demonstrates:

- DML inside a CTE is rejected (`near "update": syntax error`). Read CTEs driving
  top-level DML work. Settlement must become sequential SQL inside one transaction.
- The exact partial expression unique-index conflict target in the question works;
  a conflicting throttle insert returns no rows. Debounce upsert is also tested.
- `update ... from` and `insert ... on conflict ... do update ... returning` work.
- `returning` inside update/insert trigger bodies is rejected. No trigger-based
  pipeline or Bun user-defined time function is needed.
- `json_type` returns `integer`/`real` for numbers, `true`/`false` for booleans,
  `text` for strings; Postgres `jsonb_typeof` uses `number`, `boolean`, `string`.
  Both use `object`, `array`, `null` for those JSON values. SQL NULL is distinct.

## Semantic limits / differences (not product decisions)

References below use `Q = packages/pgconductor-js/src/query-builder.ts` and
`M = migrations/0000000001_setup.sql`, relative to repository root.

- Whole-file single-writer serialization via `store.ts:138` replaces row locks /
  skip-locked claims (`Q:288,347`); concurrency is strict rather than Postgres's
  intentionally soft coordination (`M:157`). There is no horizontal writer scaling.
  Fencing is checked before all side effects (`store.ts:364`, `Q:434`), safe because
  the writer lock spans the whole synchronous transaction.
- Epoch milliseconds and finite `Number.MAX_SAFE_INTEGER` infinity (`store.ts:6,417`)
  replace timestamptz and real infinity (`M:100`, `Q:726`); sub-ms precision is lost.
  Time is explicit input, not a database clock (`M:5`, `Q:277`). UUIDv7 uses Bun's
  actual clock (`store.ts:215`), not Postgres's pre-18 fake-clock helper (`M:40`).
- JSON is text checked by SQLite, not canonical jsonb (`schema.sql:26`, `M:96`).
  Metadata is copied but the Postgres 8 KB normalized-json size guard is omitted
  (`store.ts:220`, `M:346,573`). The API is local/camelCase and claims return encoded
  JSON strings, unlike DatabaseClient's decoded payload (`store.ts:25`, `Q:396`).
- Scope omits dead-letter delivery, event-subscription cleanup, cron scheduling,
  retention sweeps, cancellation graph/signals, and migration/version shutdown
  signaling (`schema.sql:2,83`; `Q:609,665,739,127`; `M:755`). Immediate retention,
  step-delete cascading, cancelled-result failure and child-orphan failure are covered.
- Duplicate results for one execution in a settlement are rejected deliberately
  (`store.ts:361`); Postgres has no explicit rejection (`Q:426`). No ambiguous
  multiple updates to one row are attempted.
- The existing keyed-singleton caveat is retained, not fixed: with a non-null dedupe
  key, a different slot can conflict with the regular unique key even though the
  singleton index would allow it (`schema.sql:42,47`; `M:120,644,681`). A terminal
  unlocked dedupe upsert also does not resurrect the row (`store.ts:194`, `M:722`).
