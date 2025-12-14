# Group-Based Concurrency Design

## Current System

### Schema
```
_private_tasks:
  - concurrency_limit (integer, nullable)

_private_concurrency_slots:
  - task_key + slot_group_number (PK)
  - capacity (always 1)
  - used (0 or 1)
```

### Characteristics
- **Single dimension**: One global concurrency limit per task (e.g., `concurrency: 5`)
- **Pre-allocated slots**: Slots created at task registration (5 rows for limit=5)
- **Batch processing**: Query pairs up to 100 executions with available slots using ROW_NUMBER
- **Fast path**: When no tasks have concurrency, uses simpler query (no slot overhead)

### Limitations
Cannot express multi-dimensional rate limits like:
- "Max 20 executions per tenant"
- "Max 1 execution per message"
- Both constraints applied simultaneously to same task

## Problem Statement

### Use Case
```
Task: "process-message"
Requirements:
  - Max 20 concurrent executions per tenant (tenant-level rate limit)
  - Max 1 concurrent execution per message (message-level deduplication)
  - Constraints apply independently and simultaneously

Examples:
  { tenant: "acme", message: "msg-1" } → needs slots from tenant=acme AND message=msg-1
  { tenant: "acme", message: "msg-2" } → needs slots from tenant=acme AND message=msg-2
  { tenant: "beta", message: "msg-1" } → needs slots from tenant=beta AND message=msg-1
```

### Core Challenges
1. **Dynamic slot space**: Cannot pre-create slots for unknown tenant/message IDs
2. **Multi-dimensional claiming**: Each execution needs slots from multiple independent groups
3. **All-or-nothing atomicity**: Must claim ALL required group slots or none (avoid deadlocks)
4. **Optional groups**: Some executions may skip certain groups (no key provided)
5. **Query complexity**: Current batch approach breaks down - each execution has unique group keys

## Design Options

### Option 1: Fully Independent Groups (Recommended)

#### Schema Changes
```
_private_task_concurrency_groups:
  - task_key + group_name (PK)
  - concurrency_limit (integer)

_private_concurrency_slots:
  - task_key + group_name + group_key + slot_number (PK)
  - used_by_execution_id (nullable UUID)

_private_execution_group_keys:
  - execution_id + group_name (PK)
  - group_key (text)
```

#### API Surface
```typescript
// Task definition
defineTask({
  name: "process-message",
  concurrency: {
    tenant: 20,   // max 20 per tenant
    message: 1,   // max 1 per message
    // groups are optional per execution
  }
})

// Invocation
invoke({ name: "process-message" }, {
  payload: { ... },
  concurrencyGroups: {
    tenant: "acme",
    message: "msg-123"
  }
})
```

#### Semantics
- **Independent constraints**: Each group is an independent concurrency limit
- **All must pass**: Execution must satisfy ALL provided group constraints
- **Optional participation**: If execution doesn't provide key for a group, that group's constraint is skipped
- **Example**: `{ tenant: 20, message: 1 }` means "max 20 per tenant" AND "max 1 per message"

#### Slot Lifecycle
1. **Creation**: On-demand when first execution with that (task, group, key) arrives
2. **Claiming**: Execution atomically claims one slot from EACH provided group
3. **Release**: All slots released when execution completes/fails/released
4. **Garbage collection**: Delete unused slots after TTL (e.g., 24 hours of inactivity)

#### Query Strategy (Conceptual)
```
for each execution candidate:
  1. extract concurrencyGroups from payload/metadata
  2. for each group defined on task:
     - if execution provides key for group:
       → check if slot available for (task, group, key)
       → if no slot available → skip this execution
     - if execution doesn't provide key:
       → skip this group check (no constraint)
  3. if ALL checks pass:
     → claim all required slots atomically (for update)
     → return execution for processing
```

#### Pros
- ✅ Flexible: supports any number of groups
- ✅ Optional: groups can be skipped per execution
- ✅ No deadlocks: all-or-nothing claiming
- ✅ Intuitive semantics
- ✅ Matches stated use case exactly

#### Cons
- ❌ Query complexity: O(executions × groups) checks needed
- ❌ Hard to batch: each execution has unique group keys
- ❌ Slot table growth: one row per unique (task, group, key, slot) combination
- ❌ Performance unknown: needs prototyping

### Option 2: Single Primary Group + Global Limit

#### Simplified Model
```typescript
defineTask({
  name: "process-message",
  concurrency: 100,  // global task limit (existing system)
  concurrencyGroup: {  // ONE group dimension only
    name: "tenant",
    limit: 20
  }
})
```

#### Semantics
- Global limit (100) applied first using existing system
- Group limit (20 per tenant) applied as secondary filter
- Only one group dimension supported

#### Pros
- ✅ Simpler to implement
- ✅ Covers common case (rate limiting per tenant)
- ✅ Can still use batch processing somewhat
- ✅ Lower query complexity

#### Cons
- ❌ Only one group (not extensible to multi-group case)
- ❌ Doesn't solve stated use case (tenant + message simultaneously)
- ❌ Coupling between global and group limits may be confusing

### Option 3: Composite Keys (Pre-computed)

#### Idea
User declares all possible group key combinations upfront

```typescript
defineTask({
  name: "process-message",
  concurrency: {
    tenant: ["acme", "beta", "gamma"],  // fixed list of keys
    message: 1  // per-key limit
  }
})
```

#### Pros
- ✅ Can pre-create slots (known space)
- ✅ Query similar to current system

#### Cons
- ❌ Not dynamic (can't handle new tenants at runtime)
- ❌ Combinatorial explosion with multiple groups (tenant × message slots)
- ❌ Doesn't match stated use case (unknown keys upfront)

## Open Design Questions

### 1. Query Performance Strategy

**Problem:** Current query efficiently batches 100 executions. With groups, need per-execution evaluation.

**Options:**
- **A) PostgreSQL function with loop**: Procedural approach iterating through candidates
- **B) CTE-based with LATERAL joins**: Declarative query checking all groups per execution
- **C) Two-phase approach**: Batch-lock likely slots, then check per execution

**Decision needed:** Which approach balances performance vs complexity?

### 2. Slot Creation & Lifecycle

**When to create slots?**
- Lazy: on first execution with that group key
- Eager: during registration if keys are known upfront
- Explicit: via separate admin API

**Garbage collection:**
- Delete slots unused for X hours/days?
- Keep forever (infinite growth)?
- Manual cleanup API?

**Decision needed:** What's the lifecycle management strategy?

### 3. Atomicity & Locking Model

**Scenario:** Execution needs [tenant=acme slot, message=msg-1 slot]

**Option A - Optimistic:**
- Check both available
- Attempt to claim both
- If second claim fails → rollback first

**Option B - Pessimistic:**
- Lock all required slots with for update
- Then assign to execution

**Option C - Hierarchical:**
- Order groups (alphabetically?) to prevent deadlocks
- Always lock in same order

**Decision needed:** Which locking strategy avoids deadlocks and contention best?

### 4. Backwards Compatibility

**Current API:**
```typescript
concurrency: 5
```

**Proposed API:**
```typescript
concurrency: { tenant: 20 }
```

**Migration Options:**
- **A) Support both**: `number | Record<string, number>` (number maps to global limit)
- **B) Explicit migration**: `concurrency: 5` becomes `concurrency: { default: 5 }`
- **C) Deprecate number**: Breaking change, require object format

**Decision needed:** Migration path for existing tasks?

### 5. Fast Path Optimization

**Current:** When no tasks have concurrency, uses optimized query (no slot logic)

**With groups:** Need to:
1. Check if task defines groups
2. Check if execution provides group keys
3. Mix grouped and ungrouped tasks in same queue

**Options:**
- **A) Multiple fast paths**: ungrouped, single-group, multi-group
- **B) Unified query**: Always handle groups (slower but simpler)
- **C) Separate queues**: Group-enabled tasks use different queue

**Decision needed:** Can we preserve fast path optimization?

### 6. Payload vs Metadata

**Where do group keys live?**

**Option A - In payload:**
```typescript
invoke({ name: "task" }, {
  tenant: "acme",  // part of payload
  message: "msg-1"
})
```
- ✅ Simple
- ❌ Mixes business data with infrastructure concerns

**Option B - Separate metadata:**
```typescript
invoke({ name: "task" }, {
  payload: { /* business data */ },
  concurrencyGroups: {
    tenant: "acme",
    message: "msg-1"
  }
})
```
- ✅ Clean separation
- ❌ More verbose
- ❌ Need to persist metadata separately

**Decision needed:** API ergonomics and data modeling?

## Recommendation

### Start with Option 1 (Fully Independent Groups)

**Rationale:**
1. Matches stated use case exactly (tenant + message simultaneously)
2. Extensible to N groups without redesign
3. Optional groups handle partial constraints elegantly
4. Most flexible for future requirements

### But Prototype Query Performance First

Before full implementation, validate performance with prototype:

**Test scenario:**
- 1000 pending executions
- 10 concurrency groups defined
- 100 unique keys per group
- Measure query time for group-based slot matching
- Target: maintain ~300 tasks/sec throughput (current optimized baseline)

**Critical query sections to prototype:**
1. Extract concurrencyGroups from execution metadata (JSONB operations)
2. Check slot availability across N groups (LATERAL joins? function?)
3. Atomic all-or-nothing claiming (locking strategy)
4. Mixing grouped and ungrouped tasks (fast path preservation)

**Success criteria:**
- Throughput degradation < 50% for grouped tasks
- Ungrouped tasks maintain fast path performance
- No deadlocks under concurrent worker load

## Next Steps

1. **Design review**: Validate Option 1 approach and semantics
2. **Decide on open questions**: Particularly #1 (query strategy) and #3 (locking)
3. **Query prototype**: Implement conceptual query logic in SQL
4. **Performance benchmark**: Compare against current system (diagnose-variance.ts)
5. **API finalization**: Lock down TypeScript interfaces
6. **Implementation**: Schema migration → query → tests → docs

## Related Documents

- `CONCURRENCY.md` - Current single-dimension concurrency implementation
- `perf/diagnose-variance.ts` - Benchmarking methodology for throughput testing
