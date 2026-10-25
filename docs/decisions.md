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

### Fix: int64 precision loss in `any` decode (#471)

`jsonConverter.From` now uses `json.Decoder` with `UseNumber()`. Reproduced bug
(see `backend/converter/json_test.go`): decoding a large int64 (> 2^53) into an
`interface{}` target with plain `json.Unmarshal` coerced the number to `float64`,
losing precision (`9007199254740993` → `9007199254740992`). This affected any
`any`-typed decode: `GetWorkflowResult[any]`, `ExecuteActivity[any]`, `any` signal
channels, workflow inputs.

With `UseNumber()`, numbers decoded into `interface{}` become `json.Number` (a
string) instead of `float64`, preserving full precision. Decoding into a concrete
numeric type (e.g. `int64`) was always lossless and is unaffected.

**Behavior change / caveat:** code that type-asserts an `any`-decoded number as
`x.(float64)` will now get a `json.Number` and panic. Callers who need the numeric
value from an `any` result should use `v.(json.Number).Int64()` / `.Float64()`, or —
preferred — use the **typed** API (`GetWorkflowResult[int64]`,
`NewSignalChannel[int64]`), which decodes losslessly and avoids `json.Number`
entirely. This is why upstream has not merged the change; for this JSON-only fork the
correctness win is worth it.

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

**InstanceID follow-up: DONE.** `instance_id`/`parent_instance_id` widened to
varchar(256) (migration 000003) and `core.ValidateInstanceID` (max 256) is called in
`client.CreateWorkflowInstance` and `workflow.SpawnWorkflow`. See
`fix-instance-id-length.md`.

## Deferred schema findings (known, harmless, not yet fixed)

Audit of all column types. None of these are correctness/overflow bugs today; listed
so they are not lost.

- **`pending_events.sequence_id` is unused (confirmed).** It is written as a
  placeholder (pending events have no assigned sequence ID) and read into
  `event.SequenceID`, which the executor then overwrites via `nextSequenceID()`. Only
  `history.sequence_id` is load-bearing (ordering + `MAX(sequence_id)` replay
  checkpoint). The column exists only because `insertEvents`/scan SQL is shared between
  `pending_events` and `history`. Removing it means un-sharing that hot insert/read
  path — deferred (low value, determinism-adjacent, needs careful testing).
- **`sequence_id` / `schedule_event_id` are `bigserial`** but hold explicitly-set
  values, not DB-generated ones. They should be `bigint`; `bigserial` creates unused
  sequence objects. Cosmetic type smell — deferred.
- **`parent_schedule_event_id` is `numeric`** while sibling event-id columns are
  `bigint`/`bigserial`. Inconsistent and heavier than needed. Cosmetic — deferred.
- **`queue` varchar(128): `core.ValidQueue` NOW WIRED.** Validation (regex
  `^[a-zA-Z0-9_-]{4,31}$`) is called in `client.CreateWorkflowInstance` (after the
  empty→default substitution), on `worker` `WorkflowQueues`/`ActivityQueues` at
  construction (panics, matching existing registration-error behavior), and on
  non-empty `SpawnWorkflow`/`CreateSubWorkflowInstance` queues. NOTE: this is a
  **behavior change** — queue names outside 4–31 chars or with illegal characters are
  now rejected (previously they reached the DB). `default`/`_system_` and the existing
  sample/test queues all pass.
- **`worker` varchar(64): user-settable via `WithWorkerName`, unvalidated.** Default
  `"worker-"+UUID` (43) fits; a custom name > 64 would overflow at the DB. Low risk.
- **`metadata` / `data` are `bytea`** (unbounded) — no width risk; see the jsonb/gob
  section above.
- **`event_type` is `int`** holding a positional enum (1–19); not a width risk but the
  values must not be reordered (persisted wire format).


## Investigation: #464 "sporadic workflow/activity not found" (Postgres)

**Reproduced** (two workers polling the same queue with separate registries; the
workflow registered on only one): 20/30 instances failed with "workflow not found" —
whichever worker won the SKIP LOCKED race ran the task, and the one without the
registration failed it.

**Root cause: worker↔queue↔registry inconsistency, NOT the Postgres backend.** The
queue is shared in the backend; the registry is per-worker. The engine doesn't enforce
that a worker polling a queue has registered what's routed there. Postgres (real
multi-poller concurrency) exposes the inconsistency SQLite serialized away.

**Done:** documented the contract in `developer-guide.md` (worker must register
everything for the queues it polls; see `samples/queues`), which already demonstrates
the correct per-queue pattern.

**Deferred (the real enforcement):** bind registration to queues so the engine can
validate coverage at `Start` / warn at dispatch. Lives at the registration layer.
Until then, "not found" is a `PermanentError` — a transient misroute permanently fails
the workflow; making it retryable (with a cap) is a possible robustness follow-up.

## Roadmap: deferred improvements (captured, not started)

- **Workflow determinism analysis.** Revive the `analyzer/` package (custom Go
  analyzer for workflow non-determinism: `time.Now`, `rand`, `map` iteration,
  goroutines, `select` in workflow code). Per AGENTS.md it is currently
  disabled/version-incompatible with golangci-lint. Motivation: non-determinism in a
  workflow makes replay fail, and the symptom is nasty — the worker process stays up
  and keeps polling, but the affected workflow never advances (task fails to complete
  on every attempt), so the app silently stalls while looking healthy.
- **Stuck-workflow visibility (#381).** Add max-workflow-age / stuck detection to stats
  so a workflow stalled in a replay/non-determinism failure loop is observable
  (alertable) instead of silently halting.
- **diag state correctness (#469).** `diag` lists a Failed workflow as "Completed":
  `GetWorkflowInstances` derives `state=Finished` from `completed_at != nil` without
  distinguishing failed/canceled. Fix the state derivation in `diag/`.

## Tester issues #481 and #436

- **#481 (FIXED): race → TM_WallClock for instant mocked activities.** The activity
  goroutine decremented `runningActivities` in a `defer`, which ran after it had
  already pushed its completion callback. `newTimerMode` could then read
  `runningActivities > 0` for an already-finished activity and wrongly pick wall-clock
  mode, stalling timers until the test timeout ("workflow blocked?"). Fix: decrement
  `runningActivities` inside the completion callback (which runs on the main loop,
  synchronized with `newTimerMode`), not in the goroutine defer. Matches the
  reporter's suggested fix.
- **#436 (DEFERRED, documented limitation): tester doesn't detect pending ACTIVITY
  futures on completion.** Reproduced: a workflow that returns while an activity future
  is pending does NOT surface "workflow completed, but there are still pending
  futures". Root cause (per maintainer): `ExecuteActivity` spawns an internal retry
  coroutine that outlives the workflow function's return, so the executor doesn't mark
  the workflow completed-with-pending-futures until the activity resolves. The
  maintainer's candidate fix (mark completion on workflow-function exit) breaks several
  existing tester tests and touches executor completion/determinism semantics — too
  risky to force. Left as a documented limitation with a skipped test
  (`Test_PendingActivityFutureOnCompletion`). Note: production behavior is correct (the
  workflow does eventually complete); this is a test-fidelity gap only.

## Roadmap: rewrite diag as Go html/template (replace embedded React)

Replace the current `diag` UI (a prebuilt React SPA embedded via `//go:embed
app/build`, served by `diag.NewServeMux` with a JSON `/api/`) with a **server-rendered
`html/template`** UI. Rationale: we own the fork; Go templates need no npm/React
toolchain or embedded build step, are easy to extend, and can use richer Postgres
queries directly.

Scope / notes for later:
- Keep `diag.Backend` (GetWorkflowInstances / GetWorkflowInstance / GetWorkflowTree /
  GetStats) as the data source, or extend it with Postgres-native queries for new
  views (filter by state/queue/age, search metadata once jsonb lands, etc.).
- Server-render pages (instance list, instance detail + history, sub-workflow tree,
  stats) with `html/template`; drop `diag/app/` (React source) and the embedded build.
- **Folds in #469** (Failed workflow shown as "Completed"): the current bug is that
  `GetWorkflowInstances` derives `state=Finished` from `completed_at != nil` without
  distinguishing failed/canceled. A rewrite should derive the real outcome (failed vs
  succeeded vs canceled) — either from the final history event or a dedicated outcome
  column — so the list shows the correct status from the start.
- Candidate new features: stuck-workflow view (ties to #381), per-queue dashboards,
  retention/cleanup controls, re-run/terminate actions (ties to #196).

## Roadmap: built-in schedules (durable timer-based cron/interval)

Goal: replace the external cron package in the app worker with scheduling built into
the engine, using the durable timer (`ScheduleTimer`/`Sleep` → persisted `visible_at`)
so recurrence survives restarts — no separate cron library.

### Reference designs (how the big engines do it)

**River (closest model — Postgres queue):**
- `PeriodicJob` with a `schedule`: either `PeriodicInterval(d)` or a cron schedule.
- Cron is **not built-in** — River reuses `robfig/cron/v3`'s `Schedule` interface
  (its `PeriodicSchedule` == cron's `Schedule`). So "cron" = plug in `robfig/cron`.
- `RunOnStart: true` → also enqueue once when a node becomes leader.
- Periodic insertion is done by the **cluster leader**, which holds the schedule
  **in memory** → the schedule **resets across restarts/elections** (free tier).
- **Durable periodic jobs** (run times persisted, guaranteed across restart/election)
  are **River Pro (paid)**. So DB-backed durability is the paid differentiator.
- Overlap: combine periodic + **unique jobs** (e.g. a 24h unique interval) so a
  duplicate isn't enqueued while one is pending — same idea as our SingletonCron
  dedup.

**Temporal / Cadence (schedules as first-class objects):**
- A Schedule is a durable server-side object (created once with a cron/interval +
  target workflow); Cadence runs "a durable internal scheduler workflow that fires
  your target on each tick, surviving restarts" — i.e. a self-driving workflow, the
  same pattern available to us.
- **Overlap policy** (what to do if the previous run is still going): `Skip` (default),
  `BufferOne`, `BufferAll`, `CancelOther`, `TerminateOther`, `AllowAll`.
- **Catchup window**: after downtime, which missed ticks to run (bounded) vs skip.
- Lifecycle: pause/unpause, update, backfill, inspect — without touching workflow code.

### How it maps to this fork (compose existing primitives)

The engine already has the pieces: durable timer (`workflow.Sleep`), `ContinueAsNew`,
`SpawnWorkflow`, and SingletonCron-style dedup. A scheduler is a **self-rescheduling
workflow**:

```
Scheduler(ctx, spec):
    next := nextFire(spec, workflow.Now(ctx))   // DETERMINISTIC (replay-safe clock!)
    workflow.Sleep(ctx, next.Sub(workflow.Now(ctx)))
    spawn/trigger the target workflow (deterministic instance id per tick)
    workflow.ContinueAsNew(ctx, spec)            // reschedule
```

Design decisions to make (borrowed from above):
1. **Schedule type:** start with **interval** (trivial, fully deterministic, no deps);
   add **cron** via `robfig/cron/v3` (River's exact approach) as step 2.
2. **Determinism:** `nextFire` must use `workflow.Now(ctx)`, never wall clock. Cron
   parsing is deterministic; the *next-time* calc must be pinned to the replay clock.
3. **Overlap policy:** at minimum `Skip` (our SingletonCron dedup already does this via
   deterministic instance id → duplicate rejected). Consider `BufferOne`/`AllowAll`
   later.
4. **Catchup:** decide run-missed-ticks vs skip-to-next after downtime (bounded
   window), like Temporal's catchupWindow.
5. **Multi-replica:** needs **leader election** (roadmap prerequisite) so only one
   scheduler drives recurrence. Single-replica (current) doesn't.
6. **Durability tier:** our timer is already durable (persisted `visible_at`), so we
   get "durable periodic" for free — the thing River charges for. Good fork win.

### Plan
- **Phase 1 (app-layer, no engine change):** a generic `Scheduler` workflow + helper
  (interval first, then `robfig/cron`), using Sleep + ContinueAsNew + dedup. Replaces
  the external cron package.
- **Phase 2:** overlap policies + catchup window; pause/update as signals.
- **Phase 3:** leader election for multi-replica (shared with other maintenance
  services).

## Roadmap: denormalize initial input + final result onto `instances`

**Goal:** make a workflow's **initial input** and **final result** readable without
loading its full event history and decoding every attribute blob. Today a report or
the diag list must fetch all `history`/`attributes` rows for an instance and decode
each blob just to find the `WorkflowExecutionStarted` inputs and the
`WorkflowExecutionFinished` result. Denormalize those two (head + tail) onto the
`instances` row.

**Scope — common fields only, NOT a full attribute spread.** Do not explode the
polymorphic per-event attributes into columns (19 event types, sparse/ugly, and the
payloads are opaque base64-of-JSON anyway — see the payload-nesting note). Only
promote the small, useful head/tail values.

**Proposed columns on `instances`:**
- `input bytea NULL` — the start event's `Inputs` (the initial workflow input).
- `result bytea NULL` — the final result payload (NULL until Finished).
- `error ...` (optional) — the failure, so failed vs succeeded is distinguishable.
  This also **fixes #469** (diag showing Failed as "Completed") and lets the UI show
  the outcome without reading history.

**Write points (already have the values, just copy the payload bytes):**
- `input`: set in `createInstance` from `ExecutionStartedAttributes.Inputs` (known at
  create time).
- `result`/`error`: set in `CompleteWorkflowTask` when state → Finished (from the
  complete-workflow command / `WorkflowExecutionFinished`).
- Values are already serialized `payload.Payload` ([]byte), so storing them is a copy,
  no new encoding. (If jsonb lands later, these could be jsonb for queryability — but
  note the payload is base64-of-JSON today; see payload-nesting note.)

**Nuances / decisions:**
- **ContinueAsNew:** each execution gets its own input; a new execution row carries its
  own `input` — correct per-execution semantics.
- **Failed workflows:** "result" for a failure is really the error — store `error`
  separately rather than cramming it into `result`.
- **Storage cost:** inputs/results are duplicated (also in `attributes`). Deliberate
  denormalization for read speed; usually fine, note for large payloads.
- **Core change:** migration (new nullable columns; backfill optional) + write-path
  edits in `createInstance` and `CompleteWorkflowTask` + a query/diag surface to expose
  them. Bounded but touches the engine — do with a design pass + tests, and as a new
  migration (append-only).

**Reference:** matches how Temporal/River surface a small amount of structured
head/tail info without forcing a full-history read; payloads themselves stay opaque.

### Phased plan (agreed)

- **Phase 1 — promote + KEEP (duplicate):** add `input`/`result` (+ optional `error`)
  columns on `instances`, populated at `createInstance` / `CompleteWorkflowTask`. The
  history blobs are unchanged; the engine keeps replaying from history exactly as
  today. These columns are a pure fast read path for reports/diag. Low risk, additive,
  test first. Storage duplication (the two payloads) is the accepted cost.
- **Phase 2 — remove the duplication (single source of truth):** only after Phase 1 is
  solid. Strip inputs/result from the history blobs and make all readers source them
  from the new columns. **CAUTION — this touches replay:**
  `handleWorkflowExecutionStarted` reads `ExecutionStartedAttributes.Inputs` on every
  replay to re-invoke the workflow; the completed result flows to parent sub-workflow
  awaits and `GetWorkflowResult`. Removing them from history means replay/result
  sourcing must read the columns instead, history is no longer self-contained, and the
  shared insert/scan path needs event-type-specific handling. Determinism-critical;
  do with a careful design pass + full replay tests, and keep Phase 1 as the fallback.
- **Alternative to Phase 2 (no duplication, no redesign):** instead of de-duplicating,
  make diag/report queries fetch only the start + finish events (by event type) rather
  than the full history — a query optimization that keeps a single source of truth
  (the blobs) without new columns. Consider this before committing to Phase 2's risk.

### Phase 2 — IMPLEMENTED for inputs (prove-first, incremental)

Done on `feature-postgres`:
- **Step 1 — source inputs from the column (blob still written).** `GetWorkflowInstanceHistory`
  and `GetWorkflowTask` overwrite the `WorkflowExecutionStarted` event's `Inputs` from the
  `instances.input` column after the result set is consumed (`injectPromotedInput`). The
  executor is unchanged — it receives a fully-populated `ExecutionStartedAttributes` and
  cannot tell the source. Proven by a **divergence test** (`Test_Promote_InputSourcedFromColumn`):
  blob=21, column overwritten to 5, workflow returns 10 — the column is genuinely the source,
  not a no-op. (An earlier in-loop query on the same tx caused `driver: bad connection`; fixed
  by injecting after the rows are closed.)
- **Step 2 — stop writing inputs into the blob (column is the sole copy).** `stripStartInputs`
  returns a shallow copy of the start event with `Inputs=nil` for serialization only (the
  caller's event is never mutated). Applied at both write sites: `CreateWorkflowInstance` and
  the sub-workflow/continuation path in `CompleteWorkflowTask`. Proven by
  `Test_Promote_InputStrippedFromBlob`: the persisted start-event blob has empty `Inputs`, the
  column has them, the result is still correct (replay from column), other blob fields (Name)
  survive, and `RemoveWorkflowInstance` still works. Conformance suite + e2e simple/sub-workflow
  all green.

**Scope + caveats:**
- **Terminal result + error de-duplicated (DONE).** The `WorkflowExecutionFinished` event's
  `Result`/`Error` are sourced from `instances.result`/`instances.error` on read
  (`injectPromotedResult` in `GetWorkflowInstanceHistory`/`GetWorkflowTask`) and stripped from
  the blob on write (`stripFinishedResult` before `insertHistoryEvents`, after the column
  UPDATE). Replay-safe because the executor **ignores** `WorkflowExecutionFinished`
  (`executor.go` — no handler), so only reader paths (`GetWorkflowResult`, diag) consume it, and
  they go through the injecting read path. Proven: result divergence (column=99 vs blob=42 →
  `GetWorkflowResult` returns 99), result/error blob-stripped tests, failed-workflow error
  round-trip (via `workflowerrors.Error` JSON), and sub-workflow result still reaches the parent.
- **`SubWorkflowCompleted`/`SubWorkflowFailed` are intentionally OUT OF SCOPE.** Those are
  inter-workflow *delivery messages* to the parent (read by the parent's replay via
  `handleSubWorkflowCompleted`), not duplicates of the child's terminal event. De-duplicating
  them would cross instance boundaries and touch determinism-critical parent replay — not worth
  the risk. The child's result bytes live in the child's `instances.result` column AND in the
  parent-bound message; that is correct, not redundant.
- **History is no longer self-contained for inputs or terminal result/error.** A start or
  finished event loaded *without* going through `GetWorkflowInstanceHistory`/`GetWorkflowTask`
  (a future direct reader, external tooling, or an upstream merge that bypasses the injection)
  will see empty inputs/result/error. The `instances` columns are now authoritative; any new
  reader of those two events must source from the columns.
- **Partial irreversibility.** Rows written after this change cannot recover inputs/result/error
  from the blob (empty there) — only from the columns. The divergence tests establish the columns
  are authoritative, so this is acceptable, but there is no blob fallback for new rows.

## ContinueAsNew: history scoping, cleaning, and lineage (findings)

How it works today (verified):
- Each ContinueAsNew creates a **new execution row** (same `instance_id`, new
  `execution_id`, fresh history). The old execution gets `state=ContinuedAsNew` and
  `completed_at=now`.
- `GetWorkflowInstanceHistory` is scoped to **one `(instance_id, execution_id)`** — it
  does NOT merge history across continuations. There is no "full lineage history" call.
- **Forward link only:** the OLD execution's `WorkflowExecutionContinuedAsNew` event
  carries `ContinuedExecutionID` → the next execution. The NEW execution's
  `WorkflowExecutionStarted` has **no backward pointer** to its predecessor.
- **Promotion (Phase 1):** `input` + `workflow_name` are populated per execution (each
  continued execution gets its own input). `result`/`error` are only on the execution
  that truly `Finished` — ContinuedAsNew executions have `result=null` (they handed off,
  didn't return). This is correct: a continued execution produced no final result.

With vs. without cleaning (`WithRemoveContinuedAsNewInstances`):
- **Without (default):** all execution rows + histories retained. Lineage is walkable
  **forward** via `ContinuedExecutionID` (start at the head, follow to the finished
  one). diag list shows N rows per continued instance. Storage grows per continuation
  (until normal retention sweeps by `completed_at`).
- **With cleaning:** the previous execution (row + history + attributes) is DELETED on
  each continuation. Only the current/terminal execution survives (keeps its own input,
  and result if finished). **Lineage is unrecoverable** — the forward links lived in the
  now-deleted old executions, and the survivor has no backward pointer. Proper for
  **scheduler/cron-style** workflows (only "now" matters); NOT for cases needing an
  audit trail.

## Candidate feature: durable backward lineage pointer

**Problem:** continuation lineage is forward-only and stored in the ephemeral old
executions, so cleaning destroys it and the surviving row can't tell you what it
continued from.

**Proposal:** record a durable **backward** reference on each continued execution:
- Add `previous_execution_id varchar NULL` (and/or `original_execution_id`) — set when
  `createInstance` creates a continued execution (the ContinueAsNew path knows the old
  execution id). Also carry it on `ExecutionStartedAttributes` so it's in history too.
- Then, even **with cleaning**, the current row knows its predecessor (or the original
  head), so lineage/"this is run #k of instance W1" is answerable from the `instances`
  row alone — no forward-walk, no dependence on retained old executions.

**Pairs with:**
- The input/result promotion (another promoted column on `instances`).
- The diag rewrite (render the continuation chain; show "run N of M").
- Per-state retention (decide lineage retention independently).

**Note:** small, additive (nullable column + one write-path assignment on the
ContinueAsNew create path + optionally the start attributes). Low risk; a new
append-only migration.

## Pre-release: rename the module path (deferred)

The module is still `github.com/cschleiden/go-workflows` (kept for drop-in compatibility
during development). Before tagging a release, rename it to the fork's own path
(`github.com/denisaditya0/go-workflows`) so it can be consumed by `go get <fork-url>`
without a `replace` directive and so the module owns its identity/provenance.

**Why defer until after LISTEN/NOTIFY:** the rename rewrites imports across ~150 files;
doing it while feature branches are in flight would force every branch to rebase across
the rename. Do it as the *last* step before the release tag.

**Mechanics when we do it:**
- Change the `module` line in `go.mod`.
- Scripted rewrite of every `github.com/cschleiden/go-workflows/...` import across the repo.
- Update the companion app (`apimicro-testing-worker`) imports.
- Verify: `go build ./...`, `go vet ./...`, full `backend/postgres` suite.

**Tradeoff being accepted:** renaming breaks the drop-in property (consumers importing the
old path must update), which is fine for an independently published, tagged module.
