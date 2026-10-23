# Developer Guide: structure & where to add things

This guide maps the codebase and tells you **where new code goes**. It reflects the
Postgres-only fork's layout. For *how execution works* (the flow/replay model), see
`architecture.md`; this doc is about *where things live* and *how to extend*.

## The layering rule (one direction)

```
public API        workflow/  activity/  client/  worker/  (+ workflow/executor contract)
   │ depends on
internal engine   internal/{workflowengine, activityengine, workerengine, systemworkflows,
   │                        command, workflowstate, sync, tracing, propagators, contextvalue,
   │                        continueasnew, signals, args, metrics, workflowerrors}
   │ depends on
shared leaf       internal/common/{fn, log, metrickeys}   and   core/   (domain types)
```

Rules:
- **Everything depends downward.** public → internal → common/core. Never the reverse.
- **Internal must not import the public `workflow`/`activity`/`worker` packages** — use
  `core` for shared types (`core.Queue`, `core.WorkflowInstance`). Exceptions that *are*
  workflow-level code may import `workflow` (e.g. `internal/systemworkflows`,
  `internal/propagators`).
- **`internal/` is private** to this module; public packages are the supported API.

## Package map (by concern)

| Package | Role |
|---|---|
| `backend` | The `Backend` interface (contract). |
| `backend/postgres` | The only backend impl. Split: `postgres.go` (ctor/migrate), `instances.go`, `workflow_tasks.go`, `activity_tasks.go`, `events.go`, `diagnostics.go`, `stats.go`. |
| `client` | Control plane: create/signal/cancel/query workflows. |
| `worker` | Public worker + options; assembles the internal workers. |
| `workflow` | Workflow authoring API (deterministic primitives). |
| `activity` | Activity authoring helpers (`Logger`, `Attempt`). |
| `workflow/executor` | **Public contract** for the executor: `WorkflowExecutor`, `ExecutionResult`, `Cache`, `WorkflowHistoryProvider`. |
| `workflow/executor/cache` | Default LRU implementation of `executor.Cache`. |
| `registry` | Name → function lookup. |
| `core` | Shared domain types: `WorkflowInstance`, `Queue`, state. |
| `internal/workflowengine` | The workflow replay engine (`NewExecutor`, `ExecuteTask`, event handlers, coroutine wrapper). |
| `internal/activityengine` | Activity executor + activity state. |
| `internal/workerengine` | The poll/dispatch loop (`poller`, `dispatcher`, `handle`) + the per-type workflow/activity task workers. |
| `internal/systemworkflows` | Library-shipped system/maintenance workflows (e.g. expiration). |
| `internal/command` | Command state machine: one file per command type (`ScheduleActivityCommand`, …). |
| `internal/workflowstate` | In-memory workflow execution state (`WfState`): futures, pending commands, replay flag, signal channels. |
| `internal/sync` | Deterministic coroutine/channel/future/selector runtime. The determinism core. |
| `internal/tracing`, `internal/propagators` | OTel span plumbing + context propagators. |
| `internal/contextvalue` | Typed context keys (converter, registry, propagators). |
| `internal/workflowerrors` | Serializable error envelope (`FromError`/`ToError`, retryability). |
| `internal/args`, `internal/common/fn` | Reflection helpers for function args/names. |
| `internal/common/log`, `internal/common/metrickeys`, `internal/metrics` | Logging keys, metric names, metrics client. |
| `diag` | Diagnostics/introspection (+ web UI). |

## Where to add X

**A new workflow primitive** (like `workflow.Sleep`): add the public API in
`workflow/` (deterministic — no wall clock/rand/IO). If it needs to emit an event,
add a `command` in `internal/command` (new file, embed `command`, add
`var _ Command` assertion, implement `Execute`), add the `EventType` in
`backend/history/history.go` (append at the end — values are positional/persisted),
add attributes + a case in `backend/history/serialization.go`, and a handler in
`internal/workflowengine` (`executeEvent` switch).

**A new activity helper** (like `activity.Attempt`): add to `activity/`; read from
`internal/activityengine.ActivityState`. Activities are plain Go — no determinism
rules.

**A new client operation**: add to `client/`; implement by calling `backend` methods.
If it needs a new backend capability, add it to the `Backend` interface
(`backend/backend.go`) and implement in `backend/postgres` (+ mock).

**A new backend method**: add to the `Backend` interface, implement in
`backend/postgres` (pick the right file: instances/workflow_tasks/activity_tasks),
update `backend/mock_Backend.go`, and add a conformance test in `backend/test`.

**A schema change**: add a NEW migration file under
`backend/postgres/db/migrations` (never edit an existing one). See
`fix-duplicate-active-instances.md` for an example (partial unique index).

**A new worker tuning option**: add to `worker/options.go`
(`WorkflowWorkerOptions`/`ActivityWorkerOptions`), thread it into
`internal/workerengine` via `WorkerOptions`.

**Metrics / log fields**: add constants in `internal/common/metrickeys` /
`internal/common/log`.

## Conventions established in this fork

- **Engine packages are suffixed `...engine`** (`workflowengine`, `activityengine`,
  `workerengine`) so they never collide with the public `workflow`/`activity`/`worker`
  packages. New internal engine-level packages should follow this.
- **Interface → implementation links are explicit**: add
  `var _ Iface = (*impl)(nil)` next to each implementation (grep `var _ ` to find seams).
- **Public contract vs internal engine**: when an internal engine must expose a type
  to users (like `executor.Cache`), keep the *contract* in a public package and the
  *implementation* in `internal/...engine` (see `workflow/executor` ↔
  `internal/workflowengine`).
- **Determinism**: anything that runs inside a workflow (including interceptors, once
  added) must be deterministic. External/non-deterministic work goes in an activity.
- **Migrations are append-only**; event-type enum values are positional and persisted —
  never reorder them.
- **Verify every change**: `go build ./...`, `go vet ./...`, `go test -short ./...`;
  run `backend/postgres` tests with Postgres up (`docker compose up -d`).
