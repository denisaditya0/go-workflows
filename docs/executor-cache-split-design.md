# Design note: workflow executor & the Cache public/internal split

Status: **Design note.** No code changed. Decide the approach before implementing.

## Context

For symmetry we moved the internal engines to `internal/activityengine` and
`internal/workerengine`. The **workflow executor**, by contrast, lives in the
**public** package `workflow/executor` (+ `workflow/executor/cache`). This note
asks: should the executor move to `internal/workflowengine` for consistency, and
what must stay public?

## What `workflow/executor` exposes today (the real surface)

Verified external references:

| Symbol | Used by | Is it a real extension point? |
|---|---|---|
| `executor.Cache` (interface) | `worker/options.go` (the `WorkflowExecutorCache` option), a sample, backend/test, workerengine | **Yes** — users plug in a custom cache. A sample implements a no-op cache. |
| `executor.WorkflowExecutor` (interface) | needed to implement `Cache` (its methods take/return it), tester | **Yes** — transitively public via `Cache`. |
| `executor.ExecutionResult` | workerengine (internal) | No — internal plumbing. |
| `executor.NewExecutor` | tester, cache tests | Mostly internal; tester uses it. |
| `cache.NewWorkflowExecutorLRUCache` | worker default | Public default impl. |

So the **genuinely public** part is `Cache` + `WorkflowExecutor` (the pluggable
cache contract). The replay engine (`NewExecutor`, `ExecuteTask`,
`ExecutionResult`, the event handlers) is **internal in spirit** — it only leaked
public because it shares a package with `Cache`.

## The purpose of the Cache (for the record)

The executor rebuilds a workflow's in-memory state by **replaying its history**.
Replaying the full history on every task is O(n) per task, O(n^2) over a
workflow's life. The `Cache` keeps the **live, warm executor** in memory between
tasks (keyed by instance): a cache hit feeds only the *new* events to the existing
coroutine instead of replaying everything. Combined with sticky affinity
(`StickyTimeout`), the same worker keeps serving an instance so its cache stays
warm. Default: LRU, size 128, TTL 10s. Eviction closes the executor (frees the
coroutine goroutine). Users can supply their own `Cache` via
`worker.Options.WorkflowExecutorCache`.

## Options

### Option A — leave as-is
`workflow/executor` stays public. Accept the asymmetry (activity/worker engines
internal, workflow executor public). Zero churn. The asymmetry mirrors a real
fact: users can plug a custom workflow-executor cache, but not a custom
activity/worker loop.

### Option B — split: engine internal, contract public (the consistent shape)
- Move the **replay engine** (`NewExecutor`, `ExecuteTask`, `ExecutionResult`,
  event handlers, `workflow.go`) → `internal/workflowengine`.
- Keep the **public contract** (`Cache`, `WorkflowExecutor`) where users can still
  reach it for `worker.Options`. Candidates for its home:
  - a small public package `workflow/executorcache`, or
  - fields on `worker` options typed against an interface defined in `worker`.
- The LRU default (`cache` package) depends on the public contract, not the engine.

Churn: touches **public API** (import paths for `Cache`/`WorkflowExecutor` change
for the sample, backend/test, tester, worker options). It is **more than a move** —
it reshapes a public package. Needs each external user updated + verified.

### Option C — half-measure (rejected)
Move the engine but keep `Cache` in `workflow/executor` → leaves a public package
that's just the interface, with the impl internal. Confusing; no better than A.

## Recommendation

**Option A for now, revisit with interceptors.** Honest reasoning:
- The executor being public is *justified* (the `Cache` extension point), not an
  accident. It's a mild inconsistency, not a bug.
- Option B reshapes public API (breaks import paths for anyone implementing
  `Cache`, including a shipped sample) for a mostly-cosmetic symmetry gain.
- The interceptor work will touch this same seam (interceptors wrap execution).
  Better to make the executor's public/internal boundary decisions **once**, as
  part of the interceptor design, than to churn it twice.

If we do pursue Option B, do it as its own change with a baseline-verified,
incremental migration, and treat the `Cache`/`WorkflowExecutor` relocation as a
deliberate public-API revision (note it for the rebase bridge).

## If Option B is chosen — change list

1. Create `internal/workflowengine`; move executor.go, workflow.go, cache/ engine
   bits there (package `workflowengine`).
2. Define `Cache` + `WorkflowExecutor` in a public home (`workflow/executorcache`
   or on `worker`), re-export or alias so `worker.Options.WorkflowExecutorCache`
   keeps its type.
3. Update external impls: `samples/tracing`, `backend/test/e2e.go`, `tester`.
4. `go build ./...` + `go vet` + full short tests vs baseline after each step.
