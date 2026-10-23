// Package postgres provides a PostgreSQL implementation of the backend.Backend
// interface defined in github.com/cschleiden/go-workflows/backend.
//
// The implementation is split across several files within this package:
//   - postgres.go        constructors, lifecycle, migrations, shared helpers
//   - instances.go       workflow instance lifecycle (create/cancel/remove/signal/history/state)
//   - workflow_tasks.go  workflow task queue operations (get/complete/extend)
//   - activity_tasks.go  activity task queue operations (get/complete/extend/schedule)
//   - events.go          pending/history event persistence helpers
//   - diagnostics.go     diag.Backend implementation
//   - stats.go           backend stats
package postgres

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/metrics"
	"github.com/cschleiden/go-workflows/internal/common/metrickeys"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel/trace"
)

//go:embed db/migrations/*.sql
var migrationsFS embed.FS

// postgresBackend implements backend.Backend backed by PostgreSQL.
var _ backend.Backend = (*postgresBackend)(nil)

func NewPostgresBackend(host string, port int, user, password, database string, opts ...option) *postgresBackend {
	options := &options{
		Options:         backend.ApplyOptions(),
		ApplyMigrations: true,
	}

	for _, opt := range opts {
		opt(options)
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, database)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		panic(err)
	}

	if options.PostgresOptions != nil {
		options.PostgresOptions(db)
	}

	b := &postgresBackend{
		dsn:            dsn,
		db:             db,
		workerName:     getWorkerName(options),
		options:        options,
		ownsConnection: true,
	}

	if options.ApplyMigrations {
		if err := b.Migrate(); err != nil {
			panic(err)
		}
	}

	return b
}

// NewPostgresBackendWithDB creates a new Postgres backend using an existing database connection.
// When using this constructor, the backend will not close the database connection when Close() is called.
// Migrations can still be applied using WithApplyMigrations(true) as Postgres does not require
// special connection settings for migrations.
func NewPostgresBackendWithDB(db *sql.DB, opts ...option) *postgresBackend {
	options := &options{
		Options:         backend.ApplyOptions(),
		ApplyMigrations: false,
	}

	for _, opt := range opts {
		opt(options)
	}

	b := &postgresBackend{
		dsn:            "",
		db:             db,
		workerName:     getWorkerName(options),
		options:        options,
		ownsConnection: false,
	}

	if options.ApplyMigrations {
		if err := b.Migrate(); err != nil {
			panic(err)
		}
	}

	return b
}

type postgresBackend struct {
	dsn            string
	db             *sql.DB
	workerName     string
	options        *options
	ownsConnection bool
}

func (pb *postgresBackend) FeatureSupported(feature backend.Feature) bool {
	return true
}

func (pb *postgresBackend) Close() error {
	if !pb.ownsConnection {
		return nil
	}
	return pb.db.Close()
}

// Migrate applies any pending database migrations.
func (pb *postgresBackend) Migrate() error {
	var db *sql.DB
	var needsClose bool

	if pb.dsn != "" {
		var err error
		db, err = sql.Open("pgx", pb.dsn)
		if err != nil {
			return fmt.Errorf("opening schema database: %w", err)
		}
		needsClose = true
	} else {
		db = pb.db
		needsClose = false
	}

	dbi, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("creating migration instance: %w", err)
	}

	migrations, err := iofs.New(migrationsFS, "db/migrations")
	if err != nil {
		return fmt.Errorf("creating migration source: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", migrations, "postgres", dbi)
	if err != nil {
		return fmt.Errorf("creating migration: %w", err)
	}

	if err := m.Up(); err != nil {
		if !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("running migrations: %w", err)
		}
	}

	if needsClose {
		if err := db.Close(); err != nil {
			return fmt.Errorf("closing schema database: %w", err)
		}
	}

	return nil
}

func (pb *postgresBackend) Tracer() trace.Tracer {
	return pb.options.TracerProvider.Tracer(backend.TracerName)
}

func (pb *postgresBackend) Metrics() metrics.Client {
	return pb.options.Metrics.WithTags(metrics.Tags{metrickeys.Backend: "postgres"})
}

func (pb *postgresBackend) Options() *backend.Options {
	return pb.options.Options
}

// getWorkerName returns the worker name from options, or generates a UUID-based name if not set.
func getWorkerName(options *options) string {
	if options.WorkerName != "" {
		return options.WorkerName
	}
	return fmt.Sprintf("worker-%v", uuid.NewString())
}

// pgPlaceholders builds a comma-separated list of pgx-style placeholders starting at a given index.
// Example: pgPlaceholders(3, 4) => "$3,$4,$5,$6"
func pgPlaceholders(start, count int) string {
	if count <= 0 {
		return ""
	}
	ph := make([]string, count)
	for i := 0; i < count; i++ {
		ph[i] = fmt.Sprintf("$%d", start+i)
	}
	return strings.Join(ph, ",")
}
