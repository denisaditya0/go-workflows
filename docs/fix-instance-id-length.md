# Fix: instance_id length limit (dedup IDs overflow varchar(128))

Status: **Design note** → implementing. Verified against the schema.

## Problem

`instance_id` is `varchar(128)` in every table (instances, pending_events, history,
activities, attributes), and `parent_instance_id` is `varchar(128)` in instances.
`instance_id` is **user-supplied** (`WorkflowInstanceOptions.InstanceID`,
`SpawnOptions.InstanceID`, sub-workflow options). The dedup-by-InstanceID pattern
builds composite IDs from business data (e.g. `order-<id>-child-<key>`), which can
exceed 128 chars. Today an oversize ID fails at the DB with a raw
`value too long for type character varying(128)` error — after dedup logic already
produced it, and surfaced as an opaque SQL error.

## Fix (two parts)

### 1. Widen the user-controlled columns to varchar(256)

New migration `000003_widen_instance_id`. Only the **user-controlled** id columns
change; UUID columns (`execution_id`, `event_id`, `activity_id`) stay 128.

```sql
ALTER TABLE instances      ALTER COLUMN instance_id        TYPE varchar(256);
ALTER TABLE instances      ALTER COLUMN parent_instance_id TYPE varchar(256);
ALTER TABLE pending_events ALTER COLUMN instance_id        TYPE varchar(256);
ALTER TABLE history        ALTER COLUMN instance_id        TYPE varchar(256);
ALTER TABLE activities     ALTER COLUMN instance_id        TYPE varchar(256);
ALTER TABLE attributes     ALTER COLUMN instance_id        TYPE varchar(256);
```
down: `ALTER ... TYPE varchar(128)` (will fail if data exceeds 128; acceptable for a
down-migration). `ALTER COLUMN ... TYPE` with a widening varchar is a metadata-only
change in Postgres (no table rewrite), so it is fast.

256 is a deliberate **bound** (not `text`): it keeps intent clear, stays well under
the btree index key-size limit (`instance_id` is indexed), and 256 comfortably fits
realistic composite dedup keys.

### 2. Validate length in Go (fail fast, friendly error)

Add a cheap guard so an oversize ID returns a clear error *before* touching the DB,
instead of a raw SQL failure. Define the bound as a constant matching the column:

- `const MaxInstanceIDLength = 256` (exported from `core`, next to `Queue`/validation).
- Validate in `client.CreateWorkflowInstance` and `workflow.SpawnWorkflow` (and the
  sub-workflow path) — return a clear error like
  `instance ID exceeds max length (256)` when `len(InstanceID) > MaxInstanceIDLength`.

Keep the DB column and the Go constant in sync (both 256).

## Test

- Unit: `CreateWorkflowInstance` with a 257-char InstanceID → friendly error, no DB hit.
- Integration (local Postgres): create with a ~200-char InstanceID → succeeds (proves
  the widened column accepts it).

## Verify

`go build ./...`, `go vet`, `go test -short`; run the postgres integration test with
the portable local Postgres.
