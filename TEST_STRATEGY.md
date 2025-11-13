# PgConductor Test Strategy

## Architecture Overview

**Core Components:**
- `DatabaseClient` - All DB interactions (30+ error codes handled, retry logic)
- `Conductor` - Task creation API surface
- `Orchestrator` - Worker lifecycle, heartbeats, migrations
- `Worker` - Async pipeline (fetch → execute → flush)
- `SchemaManager` - Live migrations with breaking change support
- `MigrationStore` - Migration version management

**Key Behaviors to Test:**
- Task execution lifecycle (invoke → lock → execute → complete/fail/retry)
- Live migration coordination across orchestrators
- Heartbeat & stale orchestrator recovery
- Retry logic (exponential backoff, max attempts)
- Execution windows, priority, partitioning
- Network/DB failure resilience

---

## Proposed Test Strategy

### 1. Unit Tests (Fast, No DB)

**Location:** `packages/pgconductor-js/tests/unit/`

**Approach:** Mock `DatabaseClient` at the boundary

**Coverage:**
```
✓ lib/ utilities (pure functions)
  - wait-for.ts, async-queue.ts, deferred.ts, map-concurrent.ts

✓ MigrationStore (no DB)
  - Loading migrations from generated/sql.ts
  - Version parsing, ordering
  - Breaking change detection

✓ Worker (mocked DatabaseClient)
  - Pipeline stages (fetch/execute/flush)
  - Backpressure, concurrency limits
  - Abort signal handling
  - Error handling (task throws, abort during execution)

✓ Conductor (mocked DatabaseClient)
  - Task creation with type safety
  - Context merging (ExtraContext)

✓ SchemaManager (mocked DatabaseClient)
  - Migration sequencing logic
  - Breaking migration coordination
```

**Mock Interface:**
```typescript
// tests/mocks/database-client.mock.ts
class MockDatabaseClient {
  // Track calls for assertions
  calls: { method: string; args: any[] }[] = [];

  // Configurable responses
  mockResponses: Map<string, any> = new Map();

  // Simulate failures
  simulateError(method: string, error: Error): void;
}
```

---

### 2. Integration Tests (Live DB)

**Location:** `packages/pgconductor-js/tests/integration/`

**Setup:** Dedicated test DB per test suite (parallel safe)

**Coverage:**
```
✓ DatabaseClient (all methods with real Postgres)
  - Query retry logic
  - Transaction handling
  - Error code detection
  - Abort signal propagation

✓ Schema installation & migrations
  - Fresh install
  - Sequential migrations
  - Breaking migration blocking
  - Concurrent migration attempts

✓ Task execution flow
  - Invoke → get → execute → return
  - Priority ordering
  - Execution windows (time-based blocking)
  - Visibility timeout & re-locking
  - Partitioned vs default executions

✓ Orchestrator coordination
  - Heartbeat upsert
  - Stale orchestrator recovery
  - Shutdown signal propagation
```

**Test DB Management:**
```typescript
// tests/fixtures/test-db.ts
export async function createTestDb(): Promise<Sql> {
  const dbName = `pgconductor_test_${randomUUID()}`;
  // Create DB, install extensions, return connection
}

export async function destroyTestDb(sql: Sql): Promise<void> {
  // Drop DB, close connection
}
```

---

### 3. E2E Tests (Language-Agnostic Tower Defense)

**Location:** `tests/e2e/` (root level, shared across SDKs)

**Concept:** Tower defense / chaos testing
- **Towers** = Workers (multiple orchestrators with varying concurrency)
- **Waves** = Job batches (varying sizes, priorities, timing)
- **Victory** = All jobs complete successfully within constraints

**Format:** Bash scripts for portability

**Structure:**
```bash
tests/e2e/
├── lib/
│   ├── db.sh           # Database helpers (psql wrappers)
│   ├── waves.sh        # Job wave generation
│   ├── towers.sh       # Worker management
│   └── assertions.sh   # Verification helpers
├── scenarios/
│   ├── 01-basic-wave.sh         # Simple job completion
│   ├── 02-priority-ordering.sh  # Priority queue behavior
│   ├── 03-retry-storm.sh        # Failing jobs with retries
│   ├── 04-tower-crash.sh        # Worker failure mid-execution
│   ├── 05-partition-assault.sh  # Partitioned task handling
│   └── 06-concurrent-waves.sh   # Multiple orchestrators
└── run-scenario.sh     # Main runner
```

**Runner Usage:**
```bash
# Run with specific SDK
./tests/e2e/run-scenario.sh 01-basic-wave.sh --sdk=js
./tests/e2e/run-scenario.sh 02-priority-ordering.sh --sdk=py
./tests/e2e/run-scenario.sh 03-retry-storm.sh --sdk=go

# Run all scenarios
./tests/e2e/run-all.sh --sdk=js

# Chaos mode (random tower crashes, network issues)
./tests/e2e/run-scenario.sh 04-tower-crash.sh --chaos
```

**Example Scenario:**
```bash
#!/usr/bin/env bash
# tests/e2e/scenarios/01-basic-wave.sh

source "$(dirname "$0")/../lib/waves.sh"
source "$(dirname "$0")/../lib/towers.sh"
source "$(dirname "$0")/../lib/assertions.sh"

# Setup
setup_test_db
setup_tower "worker-1" --concurrency=5

# Wave 1: 100 simple jobs
send_wave "hello-task" --count=100 --payload='{"msg":"hello"}'

# Wait for completion
wait_for_wave_completion --timeout=30s

# Assertions
assert_all_completed
assert_no_failures
assert_execution_time_under 30s

# Teardown
teardown_tower "worker-1"
```

**SDK Integration:**
Each SDK provides a CLI wrapper that the bash scripts invoke:
```bash
# pgconductor-js provides:
pgconductor worker start --config=tower.json

# pgconductor-py provides:
pgconductor worker start --config=tower.json

# Same interface, different implementations
```

---

### 4. Chaos/Failure Tests

**Location:** `packages/pgconductor-js/tests/chaos/`

**Tools:** `toxiproxy` or manual connection manipulation

**Scenarios:**
```
✓ Database failures
  - Connection loss during fetch
  - Connection loss during flush
  - Deadlock/serialization errors
  - Timeout during transaction

✓ Network issues
  - Latency spikes (>10s)
  - Packet loss
  - Connection pool exhaustion

✓ Worker failures
  - Task timeout (AbortSignal)
  - Worker crash mid-execution
  - Orchestrator crash (heartbeat expires)

✓ Migration failures
  - Migration SQL syntax error
  - Lock contention between orchestrators
  - Orchestrator version mismatch
```

**Example:**
```typescript
// tests/chaos/network-partition.test.ts
test("worker recovers after DB connection loss", async () => {
  const proxy = new ToxiProxy(/* config */);

  // Start worker
  const worker = new Worker(/* ... */);
  await worker.start();

  // Simulate network partition
  await proxy.cutConnection();
  await sleep(5000);
  await proxy.restoreConnection();

  // Worker should resume processing
  expect(await getExecutionStatus(execId)).toBe("completed");
});
```

---

### 5. Database-Specific Tests (pgTAP)

**Location:** `tests/sql/` (root level, language-agnostic)

**Approach:** pgTAP for database testing

**Setup:**
```bash
# Install pgTAP extension
CREATE EXTENSION pgtap;

# Run tests
pg_prove -d pgconductor_test tests/sql/*.sql
```

**Structure:**
```
tests/sql/
├── setup.sql                    # Test fixtures
├── functions/
│   ├── get_executions.sql      # Locking, ordering, windows
│   ├── return_executions.sql   # Retry logic, partitioning
│   ├── invoke.sql              # Deduplication, superseding
│   └── heartbeat.sql           # Shutdown signaling
├── triggers/
│   └── partitions.sql          # Partition management
└── constraints/
    └── unique_keys.sql         # Constraint validation
```

**Example:**
```sql
-- tests/sql/functions/get_executions.sql
BEGIN;
SELECT plan(5);

-- Setup test data
SET pgconductor.fake_now = '2025-01-15 10:00:00';

INSERT INTO pgconductor.tasks (key, window_start, window_end)
VALUES ('windowed-task', '09:00:00', '17:00:00');

INSERT INTO pgconductor.executions (id, task_key, payload)
VALUES (uuid_generate_v4(), 'windowed-task', '{}'::jsonb);

-- Test: Should return executions within window
SELECT is(
  (SELECT count(*) FROM pgconductor.get_executions('windowed-task', uuid_generate_v4(), 10)),
  1::bigint,
  'Returns executions within time window'
);

-- Test: Should not return executions outside window
SET pgconductor.fake_now = '2025-01-15 20:00:00';
SELECT is(
  (SELECT count(*) FROM pgconductor.get_executions('windowed-task', uuid_generate_v4(), 10)),
  0::bigint,
  'Blocks executions outside time window'
);

-- Test: Respects max_attempts
INSERT INTO pgconductor.executions (id, task_key, payload, attempts)
VALUES (uuid_generate_v4(), 'windowed-task', '{}'::jsonb, 5);

UPDATE pgconductor.tasks SET max_attempts = 3 WHERE key = 'windowed-task';

SELECT is(
  (SELECT count(*) FROM pgconductor.get_executions('windowed-task', uuid_generate_v4(), 10)),
  1::bigint,
  'Excludes executions that exceeded max_attempts'
);

-- Test: Locks executions atomically
-- ... more tests

SELECT * FROM finish();
ROLLBACK;
```

**Running Tests:**
```bash
# Run all pgTAP tests
pg_prove -d $DATABASE_URL tests/sql/**/*.sql

# Run specific test file
psql $DATABASE_URL -f tests/sql/functions/get_executions.sql

# With verbose output
pg_prove -v -d $DATABASE_URL tests/sql/**/*.sql
```

**Coverage:**
```
✓ Stored functions (all pgconductor.* functions)
✓ Triggers (partition management, cascade deletes)
✓ Constraints (unique keys, foreign keys)
✓ Indexes (performance, uniqueness)
✓ Partitioning (creation, deletion, routing)
✓ Time windows (execution blocking)
✓ Retry logic (exponential backoff, max attempts)
✓ Concurrency (locking, SKIP LOCKED behavior)
```

---

## Implementation Plan

### Phase 1: Foundation (Week 1-2)
1. Test infrastructure
   - Mock DatabaseClient
   - Test DB factory
   - Bun test configuration
2. Unit tests for lib/ utilities
3. DatabaseClient integration tests

### Phase 2: Core (Week 3-4)
4. Worker unit + integration tests
5. Orchestrator tests (migration, heartbeat)
6. SQL function tests

### Phase 3: E2E & Database (Week 5-6)
7. E2E bash framework (wave/tower scripts)
8. Basic tower defense scenarios
9. pgTAP database tests

### Phase 4: Hardening (Week 7-8)
10. Chaos mode for E2E scenarios
11. Performance benchmarks
12. Multi-SDK E2E validation

---

## Tooling Recommendations

```json
// package.json additions (JS SDK)
{
  "devDependencies": {
    "@testcontainers/postgresql": "^10.0.0",  // Isolated test DBs
    "bun": "latest"                            // Test runner
  },
  "scripts": {
    "test": "bun test",
    "test:unit": "bun test tests/unit",
    "test:integration": "bun test tests/integration",
    "test:e2e": "./tests/e2e/run-all.sh --sdk=js",
    "test:sql": "pg_prove -d $DATABASE_URL tests/sql/**/*.sql",
    "test:chaos": "./tests/e2e/run-all.sh --sdk=js --chaos",
    "test:ci": "bun test --coverage && npm run test:sql && npm run test:e2e"
  }
}
```

**System Dependencies:**
```bash
# PostgreSQL with pgTAP
brew install pgtap  # macOS
apt-get install pgtap  # Ubuntu

# pg_prove (comes with pgTAP)
pg_prove --version
```

---

## Key Design Decisions

1. **Mock at DatabaseClient boundary** - Allows testing all SDK logic without DB
2. **Test containers for isolation** - Each test suite gets fresh DB (parallel safe)
3. **pgTAP for database tests** - Standard, SQL-native testing framework
4. **Bash-based E2E** - Maximum portability across SDKs and systems
5. **Tower defense metaphor** - Makes chaos testing intuitive and fun
6. **Chaos mode flag** - Same scenarios, optional failure injection

---

## Metrics & Coverage Goals

- **Unit tests:** >90% coverage (fast feedback)
- **Integration tests:** All DatabaseClient methods + critical paths
- **E2E tests:** 10-15 core user journeys
- **SQL tests:** All custom functions, triggers, constraints
- **Chaos tests:** 5-10 failure scenarios
