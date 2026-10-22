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

### The general loop (one model for everything)

Although the worker runs two pollers, they are the **same loop** over different
task types. A workflow is really a **chain of steps** (activities, timers,
signals, sub-workflows); each step is dispatched, run, and its result written back
as an event that unblocks the next step. Generalized:

```
            ┌───────────────────────── backend (Postgres) ─────────────────────────┐
            │   instances + pending_events (WF tasks)   +   activities (ACT tasks)   │
            └───────────────────────────────────────────────────────────────────────┘
                     ▲  pull task                       │  write result event
                     │  (GetXxxTask)                     ▼  (CompleteXxxTask)
            ┌─────────┴───────────────────────────────────────────────┐
            │                      worker poll loop                     │
            │  1. pull next ready task from a queue                     │
            │  2. run it:                                               │
            │       • WF task  → executor replays history, advances the │
            │                    workflow coroutine until it blocks,    │
            │                    emitting the NEXT steps as commands     │
            │       • ACT task → run the plain-Go activity function      │
            │  3. write resulting event(s) back to the backend          │
            └───────────────────────────────────────────────────────────┘
                     │
                     └── the written event makes some instance "ready" again,
                         so the loop picks it up → next step in the chain
```

So "workflow vs activity worker" is just **which queue the step came from**. The
unifying idea: *every step produces an event; every event (eventually) triggers
the next step; the workflow coroutine is the thing that decides what the next
step is.* The two detailed cases below are specializations of this one loop.

### B1 — Workflow task (decide the next step)
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

### B2 — Activity task (do one step's work)
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

## The real cycle, step by step

To make the cycle concrete, here is the complete lifecycle of a trivial workflow
that runs one activity and returns. Each row is one step in time; the columns
show who acts and what changes in the backend. "WF task queue" = instances with
rows in `pending_events`; "ACT task queue" = the `activities` table.

```go
func Workflow1(ctx workflow.Context, n int) (int, error) {
    r, err := workflow.ExecuteActivity[int](ctx, opts, Add, n, 1).Get(ctx) // (*)
    return r, err
}
func Add(ctx context.Context, a, b int) (int, error) { return a + b, nil }
```

```
# | Actor                 | Action                                          | Backend state after
--+-----------------------+-------------------------------------------------+---------------------------------
1 | client                | CreateWorkflowInstance(Workflow1, 5)            | instances: [W1 active]
  |                        | → backend.CreateWorkflowInstance                | history:   []
  |                        |                                                 | WF queue:  [W1: ExecutionStarted]
  |                        | returns immediately                             | ACT queue: []
--+-----------------------+-------------------------------------------------+---------------------------------
2 | worker (wf poller)    | GetWorkflowTask → W1 (new: ExecutionStarted)    | W1 locked by this worker
--+-----------------------+-------------------------------------------------+---------------------------------
3 | executor.ExecuteTask  | replay: none. execute ExecutionStarted:         | (in memory)
  |                        |   start Workflow1 coroutine; it calls           |   future F1 pending (Add)
  |                        |   ExecuteActivity(Add) → emits command C1;      |   command C1 = ScheduleActivity
  |                        |   coroutine BLOCKS on .Get(ctx) (*)             |
--+-----------------------+-------------------------------------------------+---------------------------------
4 | executor → backend    | CompleteWorkflowTask(result):                   | history:   [ExecutionStarted,
  |                        |   append history, enqueue activity, unlock      |             WFTaskStarted,
  |                        |                                                 |             ActivityScheduled]
  |                        |                                                 | WF queue:  []  (nothing pending)
  |                        |                                                 | ACT queue: [Add(5,1) for W1]
--+-----------------------+-------------------------------------------------+---------------------------------
  | ...workflow is now idle. No WF task is pickable. It waits. ...          |
--+-----------------------+-------------------------------------------------+---------------------------------
5 | worker (act poller)   | GetActivityTask → Add(5,1)                      | activity locked
--+-----------------------+-------------------------------------------------+---------------------------------
6 | internal/activity     | run Add(ctx, 5, 1) → 6  (plain Go)              | (in memory) result = 6
--+-----------------------+-------------------------------------------------+---------------------------------
7 | act exec → backend    | CompleteActivityTask(6):                        | history:   [... ActivityCompleted]
  |                        |   delete from ACT queue, write completed event  | WF queue:  [W1: ActivityCompleted]
  |                        |                                                 | ACT queue: []
--+-----------------------+-------------------------------------------------+---------------------------------
8 | worker (wf poller)    | GetWorkflowTask → W1 (new: ActivityCompleted)   | W1 locked again
--+-----------------------+-------------------------------------------------+---------------------------------
9 | executor.ExecuteTask  | REPLAY ExecutionStarted again (rebuild state:   | (in memory, rebuilt)
  |                        |   F1 pending, C1 committed), Replaying=true.    |   then F1 resolved = 6
  |                        |   Then execute new ActivityCompleted:           |   coroutine runs past .Get(),
  |                        |   resolve future F1=6, Continue() coroutine;    |   returns 6, workflow COMPLETES
  |                        |   Workflow1 returns → CompleteWorkflow command  |
--+-----------------------+-------------------------------------------------+---------------------------------
10| executor → backend    | CompleteWorkflowTask(state=Finished):           | instances: [W1 finished]
  |                        |   append WorkflowExecutionFinished, mark done   | history:   [... Finished]
  |                        |                                                 | WF queue:  []   ACT queue: []
--+-----------------------+-------------------------------------------------+---------------------------------
11| client                | GetWorkflowResult(W1) → 6                       | (reads final history)
```

Key things this makes visible:

- **Steps 1 and 11 are the only external touchpoints** (the client). Everything in
  between is the worker + backend cycling on their own.
- **The workflow coroutine is reconstructed twice** (step 3 and step 9). Step 9
  *replays* ExecutionStarted before handling the new event — same code runs again,
  but side effects (scheduling the activity) are **not** repeated because the
  replayed command matches the already-persisted `ActivityScheduled` event. This
  is the determinism contract in action.
- **The activity (steps 5–7) runs completely separately** from the workflow, pulled
  from its own queue, and feeds its result back by writing an event that re-arms
  the workflow (step 7 → 8).
- **Between step 4 and step 5 (and 7 and 8) the workflow is durably paused** in
  Postgres. If every process crashed here, a new worker would resume from history.

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



---

## Launching workflows from a workflow: sub-workflow vs spawn

A running workflow can start other workflows two ways. They look similar but
model different relationships, and picking the wrong one causes real bugs.

### CreateSubWorkflowInstance — parent/child, awaited

- Records a `SubWorkflowScheduled` (7) event on the **parent's** history and emits
  a `WorkflowExecutionStarted` (1) to the child.
- The child is a `core.NewSubWorkflowInstance(id, exec, parent, scheduleEventID)` —
  it carries a **parent link**, and the parent **tracks a future** for it.
- The parent can **await the child's result**; parent completion/cancellation
  propagates to the child.
- If you omit `InstanceID`, the child gets a **random UUID**. If you pass a fixed
  `InstanceID` inside a loop, every iteration collides on the same ID and the
  backend rejects the duplicates (`ErrInstanceAlreadyExists`).

Use it for **fan-out/fan-in**: the parent starts children and waits for their
results.

### SpawnWorkflow — detached, top-level, fire-and-forget

- Emits only a `WorkflowExecutionStarted` (1) for a **new top-level instance**.
  Nothing is recorded on the calling workflow's history, and **no future is
  tracked** (so the caller never trips the pending-futures check).
- No parent/child link; the spawned workflow is **not** canceled when the caller
  completes or is canceled.
- `InstanceID` is **required** and used verbatim, so the spawned instance stays
  addressable: it can be signalled and cancelled by ID via
  `client.SignalWorkflow` / `client.CancelWorkflowInstance`.

Use it when a workflow needs to launch **independent, individually-addressable**
instances — e.g. an orchestrator that loops over items and starts one long-lived,
signalable workflow per item.

### Why spawn exists (the loop-of-signalable-children problem)

With `CreateSubWorkflowInstance` in a loop, you cannot get stable per-instance
addressability: omitting `InstanceID` yields unpredictable UUIDs, and reusing an
`InstanceID` collides so only the first survives. Either way you cannot reliably
signal an individual child. `SpawnWorkflow` fixes this by requiring a
deterministic, caller-chosen `InstanceID` and creating a detached top-level
instance — so `client.SignalWorkflow("order-123", ...)` always reaches exactly
that instance.

**Caveat:** correctness now depends on **your** IDs being unique per spawned
instance. Derive them from something unique per item (e.g. `"order-"+orderID`); a
duplicate ID is rejected and the existing instance keeps running.



### Recommended pattern: generate known IDs upstream

`SpawnWorkflow` (and `client.CreateWorkflowInstance`) deliberately use the
`InstanceID` **you** provide; they do not invent a signalable ID for you. The
recommended pattern is therefore to **generate a known ID in the layer above the
engine** — e.g. your REST/API handler — and pass it down:

```
HTTP request
  └─> your handler: derive a stable, unique ID (your dedup key lives here)
        id := "order-" + orderID
  └─> client.CreateWorkflowInstance(ctx, {InstanceID: id}, Orchestrator, ...)
         │
         └─> Orchestrator workflow spawns children with derived, known IDs:
               for i, item := range items {
                   childID := id + "-child-" + item.Key   // deterministic & known
                   workflow.SpawnWorkflow(ctx, workflow.SpawnOptions{
                       InstanceID: childID,
                   }, Child, item)
               }
  ── later, from anywhere ──
  client.SignalWorkflow(ctx, "order-123-child-abc", "sig", payload)  // reaches it
```

Because the IDs are chosen upstream and derived deterministically, every spawned
instance is **addressable without any lookup**: you can reconstruct the ID from
the same business data and signal/cancel it.

**Where dedup lives.** The engine does *not* provide a dedup key. The registry
only maps workflow/activity *names* to functions — it has no ID or dedup concept.
The backend enforces only "one **active** instance per `InstanceID`"
(`createInstance` returns `ErrInstanceAlreadyExists` for an active duplicate), and
there is no `WorkflowIdReusePolicy` like Temporal/Cadence. So any real
deduplication — especially across already-completed runs — belongs in **your
layer above the engine** (as implemented here: the dedup key is handled in the
API/handler layer before `CreateWorkflowInstance` is called). This matches
Temporal's own guidance that long-term dedup should be kept in a separate
datastore rather than relying on the engine.



### Best practice: a declarative registration layer with schema-derived dedup

A convenient way to structure a service on top of this engine is to add a small
**declarative registration layer** above `worker.RegisterWorkflow` /
`client.CreateWorkflowInstance`. Each workflow is described once by a definition
that carries its input type (with a JSON schema), metadata, and — importantly —
how its **deduplication key** is derived. The engine itself stays unaware of any
of this; it only ever sees a name and an `InstanceID`.

A typical definition looks like:

```go
type WorkflowDef struct {
    Fn          workflow.Workflow        // the registered function
    NewInput    func() any               // input factory → JSON schema + validation
    Name        string                   // logical name / trigger
    // Dedup: how to build the InstanceID from the request body.
    DedupField  []string                 // e.g. ["TestID","Service","Env"] → id = name-<f1>-<f2>-<f3>
    DedupStatic string                   // fixed key → one instance id for all runs (serialize)
    // ... queue, cron, signals, visibility, etc.
}
```

Flow:

```
HTTP request (JSON body)
  └─> registration layer:
        1. look up the WorkflowDef by trigger/name
        2. validate the body against the def's JSON schema
        3. build a deterministic InstanceID from the body using DedupField
           (or DedupStatic) — this IS the dedup key
  └─> client.CreateWorkflowInstance(ctx, {InstanceID: id}, def.Fn, input)
        └─> backend rejects a duplicate ACTIVE instance (ErrInstanceAlreadyExists)
```

Why this works well:

- **Dedup is data-driven and lives above the engine.** The key is computed from
  validated request fields, so the same request always maps to the same
  `InstanceID`. The engine's "one active instance per `InstanceID`" rule then
  provides the actual guard.
- **`DedupField`** gives per-request dedup (one running instance per unique field
  tuple). **`DedupStatic`** collapses all runs onto a single `InstanceID`, which
  serializes them — a second trigger while one is active is rejected. Useful for
  "only one of these may run at a time" workflows.
- **Schema validation happens before anything durable is created**, so malformed
  requests never reach the backend.
- For **cron / singleton** workflows, use the trigger name as a deterministic
  `InstanceID` (rather than a fresh UUID) so overlapping ticks skip instead of
  stacking up.

This keeps the engine thin (name + ID) while all policy — validation, naming,
dedup, visibility — lives in one declarative layer you control. Long-term dedup
(across completed runs) still belongs in your own datastore if you need it, per
the note above.
