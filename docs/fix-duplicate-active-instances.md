# Fix: duplicate active instances under concurrent CreateWorkflowInstance

Status: **Design note** (upstream issue cschleiden/go-workflows#489). Verified against
this fork's code. No code changed yet.

## Problem (confirmed in this fork)

`createInstance` (backend/postgres/instances.go) guards "one active instance per
`instance_id`" with a **check-then-insert**, in two separate statements:

```go
// 1. check
SELECT 1 FROM instances WHERE instance_id = $1 AND state = <Active> LIMIT 1
// ... if found → ErrInstanceAlreadyExists
// 2. insert
INSERT INTO instances (..., state) VALUES (..., <Active>)
```

`CreateWorkflowInstance` runs this in a `READ COMMITTED` transaction. Under
concurrency, two transactions creating the **same `instance_id`** can both execute
step 1 before either commits step 2 — both see "no active instance", both insert.
Result: **two active instances with the same `instance_id`**.

Why the existing index doesn't help: the only unique index is
`(instance_id, execution_id)`, and each create generates a **different random
`execution_id`**, so two active rows with the same `instance_id` do not violate it.

Impact for this fork specifically: our dedup-by-InstanceID pattern (see
architecture.md) relies on "one active instance per id" being an actual guarantee.
Today it is only guaranteed under serial calls, not concurrent ones — so the dedup
key can be defeated by a race (e.g. two REST requests firing the same workflow at
once).

## Fix (Postgres-native, atomic)

Let the database enforce the invariant atomically with a **unique partial index**,
and make the insert conflict-aware.

### 1. New migration `000002_unique_active_instance.up.sql`

```sql
-- At most one ACTIVE instance (state = 0) per instance_id.
-- Partial index: completed/continued/finished rows are exempt, so an id can be
-- reused after its previous run is no longer active (matches existing semantics).
CREATE UNIQUE INDEX idx_instances_active_instance_id
  ON instances (instance_id)
  WHERE state = 0;   -- core.WorkflowInstanceStateActive
```

down:
```sql
DROP INDEX IF EXISTS idx_instances_active_instance_id;
```

Note: the literal `0` must match `core.WorkflowInstanceStateActive`. Add a comment
in the migration pinning this, since the enum value is positional.

### 2. Make `createInstance` rely on the constraint

Replace the check-then-insert race with an insert that detects the unique
violation. Two options:

- **Option A — catch the unique violation** (keeps the explicit existence check for
  the friendly path, but the index is the real guard):
  ```go
  _, err = tx.ExecContext(ctx, "INSERT INTO instances (...) VALUES (...)", ...)
  if err != nil {
      if isUniqueViolation(err, "idx_instances_active_instance_id") {
          return backend.ErrInstanceAlreadyExists
      }
      return fmt.Errorf("inserting workflow instance: %w", err)
  }
  ```
  `isUniqueViolation` inspects the pgx error (SQLSTATE `23505`).

- **Option B — `INSERT ... ON CONFLICT DO NOTHING`** on the partial index, then
  check `RowsAffected() == 0` → `ErrInstanceAlreadyExists`. Clean, but the partial
  index as a conflict target needs the matching `WHERE` predicate in the statement.

Recommended: **Option A** — simplest, keeps behavior identical, lets the DB be the
source of truth. The existing SELECT check can stay as a fast-path, but correctness
now comes from the index, not the check.

### 3. Determinism / sub-workflow note

`createInstance` is also used when creating **sub-workflow** instances in
`CompleteWorkflowTask`. That path already handles `ErrInstanceAlreadyExists` (emits
a `SubWorkflowFailed` event). So returning `ErrInstanceAlreadyExists` from the
unique-violation branch keeps that behavior correct — no extra handling needed.

## Test scope to add

In `backend/postgres/postgres_test.go` (integration, skipped under `-short`):

- **Concurrent create test:** fire N (e.g. 20) parallel `CreateWorkflowInstance`
  calls with the **same `InstanceID`**; assert **exactly one** succeeds and the rest
  return `ErrInstanceAlreadyExists`; assert exactly one active row exists in
  `instances` for that id.
- **Reuse-after-completion test:** create, finish/remove, then create again with the
  same id → succeeds (confirms the partial index doesn't block legitimate reuse).

## Verification plan

1. `go build ./...`
2. `docker compose up -d` (Postgres), then
   `go test -run TestPostgres -race -count 1 ./backend/postgres` including the new
   concurrency test — must pass, and must fail WITHOUT the migration (prove the test
   catches the race).
3. Confirm the migration applies cleanly on a fresh DB and is idempotent
   (`ErrNoChange` on re-run).

## Why this fits the fork

Postgres-specialized, correctness-first, uses a native partial unique index
(exactly the kind of Postgres feature the fork leans into), and hardens the
dedup-by-InstanceID guarantee the application layer depends on.
