# Configurable PostgreSQL Schema — Design & Change List

Status: **Proposal** (no code changed yet)
Branch target: `feature-postgres`

## Problem

The backend currently assumes the default schema resolution of the connecting
user. Concretely:

1. `NewPostgresBackend` builds a fixed DSN with no way to pin a schema:

   ```
   host=%s port=%d user=%s password=%s dbname=%s sslmode=disable
   ```

   There is no `search_path` / `options` parameter, so unqualified table names
   (`instances`, `history`, `pending_events`, `activities`, `attributes`) are
   resolved against whatever the role's default `search_path` is (normally
   `"$user", public`).

2. `golang-migrate`'s postgres driver, by default, runs migrations against the
   first schema in `search_path` and stores its bookkeeping table
   (`schema_migrations`) there. If the role's default schema differs from where
   the application later looks for tables, migrations land in an inconsistent
   place.

3. Net effect for a role whose default schema is **not** `public`: tables may be
   created in one schema while queries read from another, which is why tables had
   to be created manually to get a working state.

## Goal

Allow callers to target a specific, named schema cleanly:

```go
b := postgres.NewPostgresBackend(host, port, user, pass, db,
    postgres.WithSchema("workflows"))
```

- The schema is auto-created if missing (`CREATE SCHEMA IF NOT EXISTS`).
- Migrations run **in that schema**, and `schema_migrations` lives there too.
- All runtime queries resolve to that schema via `search_path`.
- Table SQL stays **unqualified** (no churn across dozens of queries).
- Default behavior is unchanged when `WithSchema` is not used (uses the role
  default / `public`), so this is backward compatible.

## Design decision

**Approach A (chosen): pin `search_path`, keep tables unqualified.**

Set `search_path=<schema>` on the connection and tell golang-migrate to use the
same schema. This is minimal and keeps all existing SQL untouched.

Rejected — Approach B (schema-qualify every table name in Go): large, error-prone
diff with no advantage over A.

## Change list

### 1. `backend/postgres/options.go`
- Add a `Schema string` field to the `options` struct.
- Add `func WithSchema(schema string) option`.
- Validate the schema name (identifier allowlist: `^[A-Za-z_][A-Za-z0-9_]*$`)
  to avoid injection, since schema names cannot be passed as query parameters.

### 2. `backend/postgres/postgres.go` — `NewPostgresBackend`
- When `Schema` is set, append `search_path` to the DSN:
  ```
  ... dbname=%s sslmode=disable search_path=%s
  ```
  (pgx accepts `search_path` as a DSN keyword; it maps to the `options` startup
  parameter `-c search_path=...`.)
- Before running migrations, ensure the schema exists:
  ```sql
  CREATE SCHEMA IF NOT EXISTS <schema>;
  ```
  Executed with the validated, quoted identifier.
- Store `Schema` on the `postgresBackend` struct for reuse by `Migrate()`.

### 3. `backend/postgres/postgres.go` — `NewPostgresBackendWithDB`
- Document that when passing your own `*sql.DB`, you are responsible for setting
  `search_path` on that pool (via DSN or an `AfterConnect` hook), OR
- If `WithSchema` is provided here too, run `CREATE SCHEMA IF NOT EXISTS` and
  `SET search_path` — but note a shared pool may reset it per connection, so the
  DSN-level approach is preferred. Decide and document one behavior.

### 4. `backend/postgres/postgres.go` — `Migrate()`
- Pass schema config to golang-migrate's postgres driver via
  `postgres.Config{ SchemaName: <schema>, MigrationsTable: "schema_migrations" }`
  so both the DDL target and the bookkeeping table live in the chosen schema.
- When opening the dedicated migration `*sql.DB` from the DSN, make sure it also
  carries the `search_path`.

### 5. Migrations — `db/migrations/000001_initial.up.sql`
- No change to table definitions required (they stay unqualified and resolve via
  `search_path`).
- Confirm the `down` migration also works within a non-public schema (it only
  drops unqualified tables, so it is fine).

### 6. Tests — `backend/postgres/postgres_test.go`
- Add a test that creates a database, then a non-`public` schema (e.g. `wf_test`),
  constructs the backend with `WithSchema("wf_test")`, runs migrations, and
  asserts the tables exist **in that schema**:
  ```sql
  SELECT 1 FROM information_schema.tables
  WHERE table_schema = 'wf_test' AND table_name = 'instances'
  ```
- Add a negative test: invalid schema name is rejected by `WithSchema`.

### 7. Docs
- `docs/source/includes/_backends.md`: document `WithSchema` under the Postgres
  options list.
- `README.md`: mention schema support in the backend example if appropriate.

## Edge cases / notes

- **Identifier quoting:** schema name is interpolated into DDL (`CREATE SCHEMA`)
  and the DSN, so it must be validated (allowlist) and double-quoted in DDL.
  It cannot be a bind parameter.
- **Connection pooling:** `search_path` via DSN is applied per physical
  connection by pgx, so it survives pool churn. A one-off `SET search_path` on a
  single `*sql.DB` connection does not — hence prefer the DSN keyword.
- **Backward compatibility:** no `WithSchema` → DSN unchanged → identical to
  today.
- **Case sensitivity:** unquoted Postgres identifiers fold to lowercase. The
  allowlist + explicit quoting keeps behavior predictable; document that the
  schema name is used verbatim (quoted).

## Verification plan

1. `go build ./...`
2. `go test -run TestPostgres -v ./backend/postgres` against a running Postgres
   (docker compose up -d) including the new non-public-schema test.
3. Manually: create a role whose default schema is not `public`, run a sample
   with `WithSchema`, confirm no manual table creation is needed.
