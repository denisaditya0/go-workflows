package postgres

import (
	"database/sql"
	"fmt"
	"regexp"

	"github.com/cschleiden/go-workflows/backend"
)

type options struct {
	*backend.Options

	PostgresOptions func(db *sql.DB)

	// ApplyMigrations automatically applies database migrations on startup.
	ApplyMigrations bool

	// Schema is the Postgres schema to use for all tables. If empty, the role's
	// default search_path is used (normally "public"). When set, the schema is
	// created if missing and search_path is pinned to it for both migrations and
	// runtime queries.
	Schema string
}

type option func(*options)

// validSchemaName allows standard unquoted Postgres identifiers only. Schema names
// cannot be passed as bind parameters, so they are validated and quoted when used
// in DDL / DSN to avoid injection.
var validSchemaName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// WithSchema targets a specific Postgres schema for all tables (default: the
// role's search_path, normally "public"). The schema name must be a valid,
// unquoted Postgres identifier. The schema is created if it does not exist.
func WithSchema(schema string) option {
	return func(o *options) {
		o.Schema = schema
	}
}

// WithApplyMigrations automatically applies database migrations on startup.
func WithApplyMigrations(applyMigrations bool) option {
	return func(o *options) {
		o.ApplyMigrations = applyMigrations
	}
}

func WithPostgresOptions(f func(db *sql.DB)) option {
	return func(o *options) {
		o.PostgresOptions = f
	}
}

// WithBackendOptions allows to pass generic backend options.
func WithBackendOptions(opts ...backend.BackendOption) option {
	return func(o *options) {
		for _, opt := range opts {
			opt(o.Options)
		}
	}
}

// validateSchema returns an error if the configured schema name is not a valid identifier.
func (o *options) validateSchema() error {
	if o.Schema == "" {
		return nil
	}
	if !validSchemaName.MatchString(o.Schema) {
		return fmt.Errorf("invalid schema name %q: must match %s", o.Schema, validSchemaName.String())
	}
	return nil
}
