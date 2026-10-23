# Decisions & notes

Running record of design decisions and findings for the Postgres-only fork.

## Serialization: converter, jsonb, gob

**Current state.** The only converter is JSON: `converter.DefaultConverter = &jsonConverter{}`
(`backend/converter/json.go`), which is `encoding/json` producing `[]byte`. All
workflow/activity inputs, results, and event attributes are JSON bytes stored in
`bytea` columns (`attributes.data`, `instances.metadata`).

**Why `bytea` upstream.** `bytea` stores arbitrary bytes, so the storage layer is
**serializer-agnostic** — the converter could be JSON, protobuf, or gob. The engine
never queries inside payloads, so it never needed structured storage.

**What protobuf / gob are.** Alternative converter formats (neither implemented
here):
- **gob** — Go's built-in binary format. Fast, Go-only, no cross-language reads.
- **protobuf** — cross-language binary format (what Temporal uses). Compact, fast.
- Both produce **binary that is not valid JSON**, so a `jsonb` column would reject
  them. **jsonb and gob/protobuf are mutually exclusive.**

### Decision: DEFER jsonb and gob (revisit with performance data)

- **bytea → jsonb** — would enable querying *inside* payloads (GIN indexes, reports /
  searchable attributes, roadmap #26). Trade: couples storage to JSON (loses
  serializer-agnosticism). Safe *for this fork* because only the JSON converter
  exists. **Not done yet** — add only when there is a concrete need to query payloads.
  - If/when done: likely **only `instances.metadata`** (small, filterable for
    reports) + a **GIN index**, kept **flat** (River-style — a shallow search-attribute
    object, not deeply nested), and **leave `attributes.data` as bytea** (large event
    payloads, rarely queried inside). Use a `USING convert_from(data,'UTF8')::jsonb`
    cast in a new migration; verify Go read/write (pgx accepts `[]byte` for jsonb).
- **JSON → gob** — faster/smaller binary, Go-only. Mutually exclusive with jsonb.
  **Not done** — decide only after measuring (JSON vs gob) with the `bench/` utility.

**How to get the data:** `bench/` runs real workloads against Postgres and reports
timings; use it to compare JSON vs gob and bytea vs jsonb before committing to either.

## Schema column limits (varchar) and validation

The initial schema uses fixed-width `varchar`:

| Column | Limit | Source | Overflow risk |
|---|---|---|---|
| `instance_id` | varchar(128) | **user-supplied** (`WorkflowInstanceOptions.InstanceID`) or UUID | **Yes** — long dedup-derived IDs could exceed 128 |
| `queue` | varchar(128) | **user-supplied** queue name | Low (see below) |
| `execution_id`, `event_id`, `activity_id` | varchar(128) | internally generated UUIDs (36) | No |
| `worker` | varchar(64) | `"worker-"+UUID` (43) | No |
| `parent_instance_id`/`parent_execution_id` | varchar(128) | derived from parent | mirrors instance_id |

**Does the code enforce these?** Finding (verified by grep):
- **`instance_id`: NOT validated in Go.** No length check in `client.CreateWorkflowInstance`
  or `createInstance`. An ID longer than 128 chars fails at the **database** with a raw
  `value too long for type character varying(128)` error, not a clean validation error.
  This matters for the dedup-by-InstanceID pattern (IDs built from business data, e.g.
  `order-<id>-child-<key>`, can grow long).
- **`queue`: `core.ValidQueue` exists** (regex `^[a-zA-Z0-9_-]{4,31}$`, caps length at
  31) but appears **not called** on the create path — so queue names also rely on the
  DB's 128 limit in practice.

**Potential follow-up (not done):** add a clean length check on `InstanceID` (and wire
`ValidQueue`) in `client.CreateWorkflowInstance` so oversize values return a friendly
error instead of a raw SQL failure. Low effort; decide if worth it given your ID
scheme. Alternatively, widen the columns (e.g. `varchar(256)` or `text`) via a
migration if long composite IDs are expected.
