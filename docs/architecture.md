# Architecture & Control Flow

This document explains **how the pieces fit together and who calls whom**, so you
can follow the flow of control the way you would in an MVC app. The short version:
this is not a request/response system — it is a **durable, queue-driven state
machine**. There are two entry points (starting a workflow, and a worker
processing tasks), and all control flows *downward* into the backend.

---

## Components (the main nouns)

| Component | Package | Role |
|---|---|---|
| **Backend** | `backend` (impl: `backend/postgres`) | Persistence **and** task queues. The single source of truth. Everything else talks to it. |
| **Client** | `client` | Control plane. External entry point: create/signal/cancel workflows, read results. Only reads/writes via the backend. |
| **Worker** | `worker` (loop in `internal/worker`) | Execution plane. Polls backend queues and runs code. Internally = a *workflow worker* + an *activity worker*. |
| **Registry** | `registry` | Name → Go function lookup, so the worker knows which function a task refers to. |
| **Workflow executor** | `workflow/executor` | The engine that replays history and advances a workflow function deterministically. |
| **Orchestrator** | `worker.WorkflowOrchestrator` | Convenience combo of Worker + Client + shared Registry for single-process use. |
| **Authoring: workflow** | `workflow` | Deterministic primitives for *workflow* code (channels, futures, timers, select, …). |
| **Authoring: activity** | `activity` | Minimal helpers (`Logger`, `Attempt`) for *activity* code, which is ordinary Go. |

Supporting packages: `core` (shared domain types: `WorkflowInstance`, `Queue`,
state), `backend/history` / `backend/metadata` / `backend/converter` (data
types), `diag` (introspection UI), everything under `internal/` (private engine).

**Where is the "queue"?** It is *inside the backend*. There is no separate queue
component. In Postgres the queues are tables (`pending_events` for workflow tasks,
`activities` for activity tasks) and "dequeue" is a `SELECT ... FOR UPDATE SKIP
LOCKED`. `workflow.Queue` is just a *name/label* used to partition tasks.

---

## The one rule that removes most confusion: call direction

Control always flows **one way — downward into the backend**:

```
client ─┐
        ├─► backend   (client and worker both call the backend; never the reverse)
worker ─┘
   │
   └─► workflow/executor ─► internal/sync (coroutine) + internal/command
```

- `client` and `worker` **never call each other.** They coordinate *through the
  backend* (Postgres). That is why the client can run in one process and workers
  in others.
- The backend **never calls up** into client/worker/executor. It only stores and
  serves tasks.
- The executor calls *down* into the coroutine engine (`internal/sync`) and emits
  `internal/command`s; those packages never call back up.

If you remember only one thing: **everything calls the backend; the backend calls
nobody.**

---

## Flow A — Start a workflow (the write path; like an HTTP POST)

```
your code / REST API
  └─> client.CreateWorkflowInstance(ctx, opts, Workflow, args...)
        └─> backend.CreateWorkflowInstance(...)
              └─> INSERT instance row
              └─> INSERT WorkflowExecutionStarted event into pending_events
  ── returns immediately; NOTHING has executed yet ──
```

Starting a workflow only **records intent** in the backend. No workflow code runs
on this path. (To trigger work "from outside," this is the call you make — e.g.
from your REST API.)

---

## Flow B — A worker processes tasks (the execution loop; no MVC analogy)

A worker runs two independent pollers against the backend.

### B1 — Workflow task
```
worker.Start()
  └─> (internal/worker) poll loop
        └─> backend.GetWorkflowTask(queues)          // pull instance + new events
              └─> workflow/executor.ExecuteTask(task)
                    ├─ catchupOnHistory / replayHistory   // rebuild in-memory state
                    ├─ executeEvent(...) for each new event:
                    │     • resolves the pending *future* the workflow awaited
                    │     • calls workflow.Continue() to advance the coroutine
                    │     • your workflow code runs until it blocks again, emitting
                    │       *commands* (ExecuteActivity, Sleep, …)
                    └─ collect commands → ExecutionResult { events, activityEvents,
                                                            timerEvents, workflowEvents }
        └─> backend.CompleteWorkflowTask(result)     // ONE transaction:
              • append executed events to history
              • enqueue scheduled activities (activities table)
              • schedule timers (future pending_events)
              • create sub-workflow instances / deliver signals
```

### B2 — Activity task
```
worker.Start()
  └─> (internal/worker) poll loop
        └─> backend.GetActivityTask(queues)          // pull one activity
              └─> internal/activity.Executor runs YOUR activity func (plain Go,
                  may be non-deterministic, uses real context/time/network)
        └─> backend.CompleteActivityTask(result)     // write ActivityCompleted/Failed
              event into the workflow instance's pending_events
```

### The loop that connects them

`CompleteActivityTask` writes an `ActivityCompleted` event into the workflow's
`pending_events`. That makes the workflow instance pickable again → **B1 runs
again** → the executor resolves the activity's future → your workflow code
continues past `.Get()`. A workflow may cycle through B1 many times, with long
gaps, until it completes. **That cycle is "the flow."**

---

## How the workflow executor works (the heart)

A workflow function does **not** run start-to-finish in one go. It runs on a
coroutine (`internal/sync`) that **blocks** whenever it awaits something, and is
**driven forward by events**. History is the source of truth; the live coroutine
is reconstructed from it on demand.

Phases inside `executor.ExecuteTask`:

1. **Replay** (`replayHistory`, `Replaying = true`): re-run all prior events
   through `executeEvent` to rebuild in-memory state (pending futures, commands)
   **without repeating side effects**. Replaying the same history must reproduce
   the same state — this is the *determinism contract*.
2. **Execute new events** (`Replaying = false`): run the newly arrived events,
   which resolve futures and let the coroutine make real progress.
3. **Collect commands**: the workflow code emitted `command`s (one per
   `ExecuteActivity`, `Sleep`, `SideEffect`, sub-workflow, …). Each command
   produces events, bucketed into history / activity / timer / workflow events.
4. **Return** `ExecutionResult`; the backend persists it in one transaction.

The recurring triple to keep in mind, matched by `ScheduleEventID`:

```
future   — what the workflow code is awaiting (in memory)
command  — the intent the code emitted      (in memory, becomes an event)
event    — the persisted record             (in the backend)
```

`executeEvent` is a big type switch. `*Scheduled` events **commit** the matching
command (and assert replay scheduled the *same* thing — the determinism check).
`*Completed` / `*Fired` / `*Received` events **resolve the matching future** and
`Continue()` the coroutine.

---

## Determinism: why workflow vs activity is split

- **Workflow code is replayed.** It must be deterministic: no `time.Now()`, no
  `rand`, no raw goroutines, no network. The `workflow` package provides
  deterministic replacements (`workflow.Now`, `workflow.Sleep`,
  `workflow.Channel`, `workflow.Select`, `workflow.SideEffect`, …). This is why
  the `workflow` package is large.
- **Activity code is ordinary Go.** It runs once, can be non-deterministic, uses
  the standard library. The framework only needs to hand it a little metadata,
  which is why the `activity` package is tiny (`Logger`, `Attempt`).

An activity is always **scheduled from within a workflow** via the activity queue;
there is intentionally no "run an activity directly" API, because an activity
result that is not anchored to a workflow's history cannot be made durable or
retryable. To trigger work externally, start a (possibly trivial) workflow.

This determinism boundary is also where future **interceptors/middleware** must be
careful: an activity interceptor is plain Go, but a workflow interceptor runs
inside the replay loop and must not introduce non-determinism.
