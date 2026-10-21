# Backends

This repository provides a single backend implementation backed by **PostgreSQL**. The backend accepts the following common options:

- `WithStickyTimeout(timeout time.Duration)` - Set the timeout for sticky tasks. Defaults to 30 seconds
- `WithWorkflowLockTimeout(timeout time.Duration)` - Set the timeout for workflow task locks. Defaults to 1 minute
- `WithActivityLockTimeout(timeout time.Duration)` - Set the timeout for activity task locks. Defaults to 2 minutes
- `WithLogger(logger *slog.Logger)` - Set the logger implementation
- `WithMetrics(client metrics.Client)` - Set the metrics client
- `WithTracerProvider(tp trace.TracerProvider)` - Set the OpenTelemetry tracer provider
- `WithConverter(converter converter.Converter)` - Provide a custom `Converter` implementation
- `WithContextPropagator(prop workflow.ContextPropagator)` - Adds a custom context propagator
- `WithRemoveContinuedAsNewInstances()` - Immediately removes workflow instances that complete using ContinueAsNew, including their history. ContinueAsNew allows workflows to restart with new parameters while preserving the same instance ID. By default, such instances are retained according to the configured retention policy. Use this option to prevent storage bloat in workflows that frequently use ContinueAsNew.


## PostgreSQL

```go
func NewPostgresBackend(host string, port int, user, password, database string, opts ...option)
```

Create a new PostgreSQL backend instance with `NewPostgresBackend`.

### Using an Existing Connection

```go
func NewPostgresBackendWithDB(db *sql.DB, opts ...option)
```

If you already have a `*sql.DB` connection, you can use `NewPostgresBackendWithDB` to create a backend that uses your existing connection. When using this constructor:

- The backend will **not** close the database connection when `Close()` is called
- Migrations are **disabled by default** - use `WithApplyMigrations(true)` to enable them

```go
db, _ := sql.Open("pgx", "host=localhost port=5432 user=myuser password=mypass dbname=mydb sslmode=disable")

backend := postgres.NewPostgresBackendWithDB(db, postgres.WithApplyMigrations(true))
```

### Options

- `WithPostgresOptions(f func(db *sql.DB))` - Apply custom options to the PostgreSQL database connection
- `WithApplyMigrations(applyMigrations bool)` - Set whether migrations should be applied on startup. Defaults to `true` for `NewPostgresBackend`, `false` for `NewPostgresBackendWithDB`
- `WithBackendOptions(opts ...backend.BackendOption)` - Apply generic backend options


### Schema

See `backend/postgres/db/migrations` for the schema and migrations. Main tables:

- `instances` - Tracks workflow instances. Functions as instance queue joined with `pending_events`
- `pending_events` - Pending events for workflow instances
- `history` - History for workflow instances
- `activities` - Queue of pending activities
- `attributes` - Payloads of events


## Custom implementation

To provide a custom backend, implement the `backend.Backend` interface defined in `backend/backend.go`.
