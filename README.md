# Durable workflows using Go — PostgreSQL-centric fork

> **Fork notice.** This is a PostgreSQL-specialized fork of
> [`cschleiden/go-workflows`](https://github.com/cschleiden/go-workflows). The module path is
> unchanged (`github.com/cschleiden/go-workflows`) and the public workflow/activity API stays
> compatible with upstream, but the backend story is different: **PostgreSQL is the only backend**,
> and the storage layer has been extended with Postgres-specific features. See
> [What's different](#whats-different-from-upstream) below.

Borrows heavily from [Temporal](https://github.com/temporalio/temporal) (and since it's a fork also [Cadence](https://github.com/uber/cadence)) as well as [DTFx](https://github.com/Azure/durabletask).

Upstream background reading:
- https://cschleiden.dev/blog/2022-02-13-go-workflows-part1/
- https://cschleiden.dev/blog/2022-05-02-go-workflows-part2/

## What's different from upstream

This fork trades upstream's multi-backend flexibility for a single, well-understood,
feature-rich PostgreSQL backend.

- **PostgreSQL only.** The SQLite, MySQL, Redis, and in-memory/monoprocess backends have been
  removed. There is one backend: `backend/postgres`. This keeps the storage layer focused and
  lets it use Postgres-specific features without a lowest-common-denominator abstraction.
- **Denormalized query columns on `instances`.** `workflow_name`, `input`, `result`, and `error`
  are promoted onto the `instances` row so common queries (lists, dashboards, result lookups)
  don't have to load and decode the full history blob. Workflow **inputs** are stored *solely* in
  the `input` column — the start-event history blob no longer duplicates them (replay sources
  inputs from the column).
- **ContinueAsNew lineage.** A `previous_execution_id` column records the predecessor execution of
  a continued workflow — a durable backward pointer, distinct from the `parent_*` sub-workflow link.
- **Duplicate-active-instance protection.** A partial unique index plus conflict-aware inserts
  prevent two *active* executions sharing one instance ID, while still allowing an ID to be reused
  once the prior run has finished (upstream #489).
- **Stricter validation.** Queue names and instance IDs are validated on create/register/spawn
  paths; `instance_id` is widened to `varchar(256)`.
- **Schema isolation.** `WithSchema(...)` runs the engine (migrations and all) inside a
  non-`public` Postgres schema.
- **Correctness fixes carried from upstream issues.** int64 precision on `any`-typed decode
  (#471) and tester timer/race fixes (#494, #481).

For the full design rationale, roadmap, and the explicit caveats behind these decisions, see
[`docs/decisions.md`](docs/decisions.md), [`docs/architecture.md`](docs/architecture.md), and
[`docs/developer-guide.md`](docs/developer-guide.md).

## Docs

Upstream documentation (API, guide, patterns) applies to the workflow/activity programming model:
http://cschleiden.github.io/go-workflows. Backend-specific behavior for this fork is documented in
the `docs/` directory of this repository.

## Simple example

### Workflow

Workflows are written in Go code. The only exception is they must not use any of Go's non-deterministic features (`select`, iteration over a `map`, etc.). Inputs and outputs for workflows and activities have to be serializable:

```go
func Workflow1(ctx workflow.Context, input string) error {
	r1, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, Activity1, 35, 12).Get(ctx)
	if err != nil {
		panic("error getting activity 1 result")
	}

	log.Println("A1 result:", r1)

	r2, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, Activity2).Get(ctx)
	if err != nil {
		panic("error getting activity 1 result")
	}

	log.Println("A2 result:", r2)

	return nil
}
```

### Activities

Activities can have side-effects and don't have to be deterministic. They will be executed only once and the result is persisted:

```go
func Activity1(ctx context.Context, a, b int) (int, error) {
	return a + b, nil
}

func Activity2(ctx context.Context) (int, error) {
	return 12, nil
}

```

### Worker

The worker is responsible for executing `Workflows` and `Activities`, both need to be registered with it.

```go
func runWorker(ctx context.Context, mb backend.Backend) {
	w := worker.New(mb, nil)

	w.RegisterWorkflow(Workflow1)

	w.RegisterActivity(Activity1)
	w.RegisterActivity(Activity2)

	if err := w.Start(ctx); err != nil {
		panic("could not start worker")
	}
}
```

### Backend

The backend is responsible for persisting the workflow events. This fork uses a single [PostgreSQL](https://www.postgresql.org) backend.

```go
b := postgres.NewPostgresBackend("localhost", 5432, "user", "password", "workflows")
```

### Putting it all together

We can start workflows from the same process the worker runs in -- or they can be separate. Here we use the PostgreSQL backend, spawn a single worker (which then executes both `Workflows` and `Activities`), and then start a single instance of our workflow

```go
func main() {
	ctx := context.Background()

	b := postgres.NewPostgresBackend("localhost", 5432, "user", "password", "workflows")

	go runWorker(ctx, b)

	c := client.New(b)

	wf, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: uuid.NewString(),
	}, Workflow1, "input-for-workflow")
	if err != nil {
		panic("could not start workflow")
	}

	c2 := make(chan os.Signal, 1)
	signal.Notify(c2, os.Interrupt)
	<-c2
}
```
