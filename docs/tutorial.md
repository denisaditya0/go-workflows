# Tutorial: durable workflows, concepts to code

This tutorial walks from the ideas behind durable workflows to working code, then
lifts the hood to show how execution is paused, persisted, and replayed. It adapts
the background series written for upstream `go-workflows` (Christopher
Schleiden's "Durable Workflows in Go", parts 1 and 2) to this fork: PostgreSQL
only, generic `ExecuteActivity`/`Future`, and the Postgres storage layer.

Reference companions: [`scheduler.md`](scheduler.md),
[`architecture.md`](architecture.md), [`postgres-schema.md`](postgres-schema.md),
[`background.md`](background.md), and the runnable programs under
[`samples/`](../samples).

---

## Part 1 — the concepts

### Why durable workflows

Modern services orchestrate many downstream calls. When everything is fast and
synchronous you can just call functions. Once work becomes long-running, spans
services, or waits on external events, hand-rolled state machines, queues,
retries, and "where were we?" bookkeeping take over and the business logic
disappears under reliability plumbing.

A durable workflow engine restores the simple picture: you write mostly ordinary
sequential Go, and the engine makes it survive crashes, restarts, deployments,
and rebalancing. It does this by separating two kinds of code:

- **Workflows** orchestrate. They are long-running and durable, and must be
  deterministic. They look like normal code but follow a few rules.
- **Activities** do the side effects. They are plain functions, can use any
  library or language feature, and are executed exactly once with their result
  persisted.

This split — workflows as orchestration, activities as effects — is the model
shared by Temporal, Uber's Cadence, and Azure's Durable Task Framework (DTFx),
and the model this engine follows.

### Where the model comes from

- **DTFx** (Azure) calls workflows *orchestrations* and runs them as C#
  `async`/`await` state machines. It is a library with pluggable *providers* for
  the store (Service Bus, Azure Storage, Service Fabric, SQL Server). Clients and
  workers talk directly to the same store — a two-tier architecture.
- **Temporal** (a fork of Cadence) uses the same concepts but ships a server with
  several roles you talk to.
- **This engine** is written from scratch and borrows liberally from both. Like
  DTFx it is two-tier — clients and workers talk directly to one backend — but
  the user-facing API looks more like Temporal's, which works around Go's
  non-deterministic `select`.

What DTFx calls *providers*, this codebase calls *backends*. In this fork there
is exactly one: `backend/postgres`.

### The one rule that explains everything: event sourcing

The engine never captures and restores a goroutine's stack. Instead it records an
**event history** of everything that can happen to a workflow: started, activity
scheduled, activity completed, timer fired, signal received, sub-workflow
started, completed, and so on. To resume, it **replays** that history from the
top, re-running the workflow function; each event advances the replay past the
point that previously blocked. Because the workflow is deterministic, replay
reaches the same state and then continues with anything new.

That is the whole trick. It explains why workflows must be deterministic, why you
never call activities directly, and why the history is the source of truth.

### Inversion of execution, concretely

Consider:

```go
func OrderWorkflow(ctx workflow.Context, item string) error {
	_, err := workflow.ExecuteActivity[any](ctx, workflow.DefaultActivityOptions, Process, item).Get(ctx)
	if err != nil {
		return err
	}

	_, err = workflow.ExecuteActivity[any](ctx, workflow.DefaultActivityOptions, SendConfirmation, item).Get(ctx)
	return err
}
```

`ExecuteActivity` does not call `Process`. It schedules it and returns a
`Future`; `Get(ctx)` blocks (yields) until the result exists. Watch the history
grow as the workflow progresses:

| # | Event | Attributes | Applied during replay |
|---|-------|------------|-----------------------|
| 1 | `WorkflowExecutionStarted` | `OrderWorkflow` | ✓ |
| 2 | `ActivityScheduled` | `Process`, `item` | ✓ |

Now `Process` runs somewhere — same process, another worker, another machine —
and its result is written back:

| # | Event | Attributes | Applied |
|---|-------|------------|---------|
| 1 | `WorkflowExecutionStarted` | `OrderWorkflow` | ✓ |
| 2 | `ActivityScheduled` | `Process`, `item` | ✓ |
| 3 | `ActivityCompleted` | `some-output` | |

The workflow continues: `Get` returns, and it schedules the next activity.

| # | Event | Attributes | Applied |
|---|-------|------------|---------|
| 1 | `WorkflowExecutionStarted` | `OrderWorkflow` | ✓ |
| 2 | `ActivityScheduled` | `Process`, `item` | ✓ |
| 3 | `ActivityCompleted` | `some-output` | ✓ |
| 4 | `ActivityScheduled` | `SendConfirmation`, `item` | |

Eventually the workflow returns and its completion is recorded. Replaying this
history from scratch must reach byte-for-byte the same schedule-event sequence —
which is why determinism is non-negotiable.

### Rules for workflow code

Because the function is re-executed during replay, it must not observe
non-deterministic state directly:

- **Time.** Don't call `time.Now()`. Use `workflow.Now(ctx)`.
- **Iteration and selection.** Don't rely on `map` range order or `select`
  randomness. Use `workflow.NewSelector` to wait on several events, and
  `workflow.SideEffect` to capture a non-deterministic value into the history so
  replay reuses it.
- **Effects.** Don't call services, activities, or sub-workflows directly. Use
  `ExecuteActivity`, `CreateSubWorkflowInstance`, `SpawnWorkflow`, or
  `ScheduleTimer`, then `.Get(ctx)` the returned `Future`.
- **Serialization.** Inputs and outputs must be serializable by the configured
  converter.

Activities have none of these constraints: they run once, outside replay, and
their result is stored.

---

## Part 2 — building it

### Step 1: a workflow and its activities

```go
package main

import (
	"context"

	"github.com/denisaditya0/go-workflows/workflow"
)

func OrderWorkflow(ctx workflow.Context, item string) (string, error) {
	r1, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, Charge, 35, 12).Get(ctx)
	if err != nil {
		return "", err
	}
	_ = r1

	r2, err := workflow.ExecuteActivity[string](ctx, workflow.DefaultActivityOptions, Ship, item).Get(ctx)
	if err != nil {
		return "", err
	}
	return r2, nil
}

func Charge(ctx context.Context, a, b int) (int, error) { return a + b, nil }
func Ship(ctx context.Context, item string) (string, error) {
	return "shipped " + item, nil
}
```

`workflow.ExecuteActivity[TResult]` returns a `Future[TResult]`; `TResult` is the
activity's return type. The engine records a schedule command and blocks the
workflow until the corresponding completion event arrives.

### Step 2: run a worker

```go
import (
	"github.com/denisaditya0/go-workflows/backend"
	"github.com/denisaditya0/go-workflows/worker"
)

func RunWorker(ctx context.Context, b backend.Backend) *worker.Worker {
	w := worker.New(b, nil)

	w.RegisterWorkflow(OrderWorkflow)
	w.RegisterActivity(Charge)
	w.RegisterActivity(Ship)

	if err := w.Start(ctx); err != nil {
		panic("could not start worker: " + err.Error())
	}
	return w
}
```

`worker.New` starts a workflow worker and an activity worker. To run the durable
scheduler alongside them, pass `worker.WithScheduler(...)`; see
[`scheduler.md`](scheduler.md).

### Step 3: connect to PostgreSQL and start a workflow

```go
import (
	"context"
	"log"
	"time"

	"github.com/denisaditya0/go-workflows/backend/postgres"
	"github.com/denisaditya0/go-workflows/client"

	"github.com/google/uuid"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := postgres.NewPostgresBackend("localhost", 5432, "user", "password", "workflows")

	w := RunWorker(ctx, b)
	defer w.WaitForCompletion()

	c := client.New(b)

	wf, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: uuid.NewString(),
	}, OrderWorkflow, "item-42")
	if err != nil {
		panic("could not start workflow: " + err.Error())
	}

	result, err := client.GetWorkflowResult[string](ctx, c, wf, 10*time.Second)
	if err != nil {
		panic("could not get result: " + err.Error())
	}
	log.Println("Result:", result)
}
```

`NewPostgresBackend` applies migrations by default; see
[`postgres-schema.md`](postgres-schema.md) for `WithSchema` and other options.
Client and worker can be in one process (above) or separate processes sharing
the same database.

### Step 4: durable timers

`workflow.Sleep` schedules a timer event whose fire time is persisted; replay does
not re-wait wall-clock time.

```go
func ReminderWorkflow(ctx workflow.Context) error {
	if err := workflow.Sleep(ctx, 24*time.Hour); err != nil {
		return err
	}
	return nil // runs ~24h later, across restarts
}
```

Use `workflow.ScheduleTimer` when you need a `Future` to race against other
events via a selector.

### Step 5: signals

```go
func ApprovalWorkflow(ctx workflow.Context) (string, error) {
	ch := workflow.NewSignalChannel[string](ctx, "approval")
	value, ok := ch.Receive(ctx) // blocks until signalled or cancelled
	if !ok {
		return "", ctx.Err()
	}
	return value, nil
}
```

```go
if err := c.SignalWorkflow(ctx, wf.InstanceID, "approval", "yes"); err != nil {
	panic(err)
}
```

### Step 6: sub-workflows and spawning

- **Sub-workflow** — lifecycle tied to the parent; the parent blocks on its
  result and cancellation propagates.

```go
func Parent(ctx workflow.Context) (int, error) {
	return workflow.CreateSubWorkflowInstance[int](
		ctx, workflow.DefaultSubWorkflowOptions, Child, 41,
	).Get(ctx)
}

func Child(ctx workflow.Context, n int) (int, error) { return n + 1, nil }
```

- **Spawn** — fire-and-forget, addressable by a deterministic `InstanceID`, and
  **not** cancelled with the parent.

```go
inst, err := workflow.SpawnWorkflow(ctx, workflow.SpawnOptions{
	InstanceID: "order-42-followup",
}, FollowupWorkflow, "order-42")
```

Pick a known, derived ID so the spawned instance stays addressable for signals
and cancellation (see [`architecture.md`](architecture.md)).

### Step 7: retries

```go
opts := workflow.ActivityOptions{
	RetryOptions: workflow.RetryOptions{
		MaxAttempts:        5,
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
	},
}
res, err := workflow.ExecuteActivity[int](ctx, opts, Flaky, 1).Get(ctx)
```

### Step 8: scheduling workflows on a cron or interval

The durable scheduler starts instances from cron or fixed intervals, stored in
PostgreSQL, embedded in the worker or standalone. Reference:
[`scheduler.md`](scheduler.md); example: [`samples/scheduler`](../samples/scheduler).

### Step 9: testing workflows

The `tester` package runs a workflow in-process with mocked activities and
virtual time, so timers don't actually wait.

```go
func Test_OrderWorkflow(t *testing.T) {
	wf := tester.NewWorkflowTester[string](OrderWorkflow)

	wf.OnActivity(Charge, mock.Anything, mock.Anything).Return(47, nil)
	wf.OnActivity(Ship, mock.Anything).Return("shipped", nil)

	wf.Execute(context.Background(), "item-42")

	result, err := wf.WorkflowResult()
	// ... assert on result / err
	wf.AssertExpectations(t)
}
```

---

## Part 3 — how it works under the hood

This is the "part 2" material, mapped onto this codebase's internals. The public
`workflow` API is a thin layer over `internal/sync` (coroutines and futures) and
`internal/workflowstate` (per-execution state).

### Futures

```go
// internal/sync/future.go
type Future[T any] interface {
	Get(ctx Context) (T, error)
}

type SettableFuture[T any] interface {
	Future[T]
	Set(v T, err error)
	HasValue() bool
}
```

`ExecuteActivity` creates one with `sync.NewFuture[T]()` and returns it as a
`Future[T]`. The concrete implementation is tiny — a flag, a value, an error:

```go
type future[T any] struct {
	hasValue bool
	v        T
	err      error
}

func (f *future[T]) Get(ctx Context) (T, error) {
	for {
		cr := getCoState(ctx)

		if f.hasValue {
			cr.MadeProgress()
			if f.err != nil {
				return f.v, f.err
			}
			return f.v, nil
		}

		cr.Yield() // not ready: pause the workflow here
	}
}
```

The loop is the crux: a future either has a value (return it) or yields the
coroutine until the engine wakes it.

### Coroutines

Workflows run on **coroutines**: a goroutine plus a `coState` carried in the
context. The interface is small:

```go
type Coroutine interface {
	Execute()          // continue a blocked coroutine until it blocks again or finishes
	Yield()            // pause execution and hand control back
	Exit()             // prevent a blocked coroutine from continuing
	Finished() bool
	Blocked() bool
}
```

Creation mirrors DTFx's orchestrations — the passthrough function is started in a
goroutine that yields once *before* its first line runs, so the engine is always
in control of "continue from here":

```go
func NewCoroutine(ctx Context, fn func(ctx Context) error) Coroutine {
	s := newState()
	ctx = withCoState(ctx, s)

	go func() {
		defer s.finish()
		defer func() { /* convert panics into s.err */ }()

		s.yield(false) // yield before the first execution
		s.err = fn(ctx)
	}()

	return s
}
```

`Execute()` runs the coroutine until it yields or returns, with deadlock
detection. This is **cooperative** multitasking: the workflow decides where to
pause (every `Future.Get`), which is exactly why the deterministic rules matter.

### Workflow state

The `workflow.Context` carries a `WfState` in addition to the coroutine. It is
where futures and pending effects live for the current execution:

```go
// internal/workflowstate/workflowstate.go (fields abbreviated)
type WfState struct {
	instance        *core.WorkflowInstance
	scheduleEventID int64
	commands        []command.Command
	commandIndex    map[int64]command.Command
	pendingFutures  map[int64]*DecodingSettable
	replaying       bool

	pendingSignals map[string][]payload.Payload
	signalChannels map[string]*signalChannel
	// ...
}
```

`scheduleEventID` is a monotonically increasing counter used to correlate a
scheduled effect with the event that later completes it. `GetNextScheduleEventID`
hands one out; `TrackFuture` files the future under that ID; when a completion
event arrives, the engine looks the future up by ID, sets its value, and wakes the
coroutine.

### How `ExecuteActivity` builds an effect

```go
func executeActivity[TResult any](ctx Context, options ActivityOptions, attempt int, activity Activity, args ...any) Future[TResult] {
	f := sync.NewFuture[TResult]()                 // 1. create the future

	// ... resolve the activity name, validate return type and args, convert inputs

	wfState := workflowstate.WorkflowState(ctx)
	scheduleEventID := wfState.GetNextScheduleEventID() // 2. allocate a correlation ID

	cmd := command.NewScheduleActivityCommand(
		scheduleEventID, name, inputs, attempt, metadata, options.Queue,
	)                                                   // 3. build a command (not an event!)
	wfState.AddCommand(cmd)                             // 4. queue the command
	wfState.TrackFuture(scheduleEventID,
		workflowstate.AsDecodingSettable(cv, "activity: "+name, f)) // 5. track the future

	return f                                            // 6. caller blocks on f.Get(ctx)
}
```

Two details worth calling out:

- `DecodingSettable` wraps the typed future so the engine can decode the stored
  payload back into `TResult` when the completion event arrives.
- Cancellation is checked here: if the context is already done and the command
  is still pending, the command is dropped, the future removed, and the future
  is set to `Canceled` — no activity is scheduled.

### Commands vs events

Workflow code never writes history directly. It records **commands** on the
`WfState` (`ScheduleActivity`, `ScheduleTimer`, `ScheduleSubWorkflow`,
`SpawnWorkflow`, `CompleteWorkflow`, and so on). After the coroutine blocks or
returns, the executor turns the still-pending commands into history events and
persists them. During replay, events that already have a matching command cause
that command to be dropped (it already happened), keeping the "what the workflow
thinks it did" and "what the history says happened" consistent.

### Two-pass execution, in full

For `OrderWorkflow` above:

**First execution — no prior history**

1. Create `WfState` and attach it (plus a new coroutine) to the context.
2. Run the workflow. `ExecuteActivity(Process)` creates a future, allocates
   schedule ID `1`, queues a schedule command, and returns.
3. `Get` finds no value and yields. The coroutine is now blocked.
4. No events remain to apply. The executor flushes the pending command:
   `ActivityScheduled(Process)` is persisted.
5. The backend schedules an activity task. An activity worker runs `Process` and
   writes `ActivityCompleted(some-output)`; a workflow task is queued.

**Second execution — replay the two events**

1. New `WfState`, new coroutine; run the workflow from the top.
2. `ExecuteActivity(Process)` creates a future, allocates schedule ID `1`, and
   queues a schedule command again (deterministically, the same ID).
3. `Get` yields.
4. Apply event `ActivityScheduled(Process)`: it matches schedule ID `1`, so the
   pending command is dropped (already persisted).
5. Try to continue; the future still has no value, so the coroutine stays
   blocked.
6. Apply event `ActivityCompleted(some-output)`: look up the future by ID `1`,
   set its value, wake the coroutine.
7. `Get` returns; the workflow proceeds to `ExecuteActivity(SendConfirmation)`,
   which gets the next schedule ID, queues a command, and blocks.
8. Flush the pending command: `ActivityScheduled(SendConfirmation)` is persisted.

The pattern then repeats until the workflow returns and a completion event is
written. Because each pass reproduces the same decisions, the state after replay
equals the state before a crash — provided the code is deterministic.

---

## What is different from upstream

The concepts match upstream, but several upstream capabilities are gone here (see
[`background.md`](background.md)):

- **Only PostgreSQL.** SQLite, MySQL, Redis, and in-memory backends were removed;
  `backend/postgres` is the single backend.
- **No built-in tracing.** The OpenTelemetry integration and tracing API are gone;
  build tracing out of tree as middleware if needed.
- **Different storage.** Instances use denormalized query columns and
  Postgres-specific constraints; see [`postgres-schema.md`](postgres-schema.md).

Treat upstream material as the programming-model reference, and this fork's docs
for anything backend- or feature-specific.
