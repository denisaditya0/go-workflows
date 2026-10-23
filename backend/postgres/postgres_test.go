package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/backend/test"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/workflow"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const testUser = "root"
const testPassword = "root"

// Creating and dropping databases is inefficient but gives full isolation for tests.
// For future optimization consider: transactional tests (pgx + rollback),
// or TRUNCATE between tests within a single database

func Test_PostgresBackend(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	var dbName string

	test.BackendTest(t, func(options ...backend.BackendOption) test.TestBackend {
		// Connect to default "postgres" database to create a new one
		adminDB, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=postgres sslmode=disable", testUser, testPassword))
		if err != nil {
			panic(err)
		}
		defer adminDB.Close()

		dbName = "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		if _, err := adminDB.Exec("CREATE DATABASE " + dbName); err != nil {
			panic(fmt.Errorf("creating database: %w", err))
		}

		options = append(options, backend.WithStickyTimeout(0))

		// Create backend pointing to the newly created database
		return NewPostgresBackend("localhost", 5432, testUser, testPassword, dbName, WithBackendOptions(options...))
	}, func(b test.TestBackend) {
		// Close backend DB first
		if err := b.(*postgresBackend).db.Close(); err != nil {
			panic(err)
		}

		adminDB, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=postgres sslmode=disable", testUser, testPassword))
		if err != nil {
			panic(err)
		}
		defer adminDB.Close()

		if _, err := adminDB.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)"); err != nil {
			panic(fmt.Errorf("dropping database: %w", err))
		}
	})
}

func TestPostgresBackendE2E(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	var dbName string

	test.EndToEndBackendTest(t, func(options ...backend.BackendOption) test.TestBackend {
		adminDB, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=postgres sslmode=disable", testUser, testPassword))
		if err != nil {
			panic(err)
		}
		defer adminDB.Close()

		dbName = "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		if _, err := adminDB.Exec("CREATE DATABASE " + dbName); err != nil {
			panic(fmt.Errorf("creating database: %w", err))
		}

		options = append(options, backend.WithStickyTimeout(0))

		return NewPostgresBackend("localhost", 5432, testUser, testPassword, dbName, WithBackendOptions(options...))
	}, func(b test.TestBackend) {
		if err := b.(*postgresBackend).db.Close(); err != nil {
			panic(err)
		}

		adminDB, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=postgres sslmode=disable", testUser, testPassword))
		if err != nil {
			panic(err)
		}
		defer adminDB.Close()

		if _, err := adminDB.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)"); err != nil {
			panic(fmt.Errorf("dropping database: %w", err))
		}
	})
}

var _ test.TestBackend = (*postgresBackend)(nil)

// GetFutureEvents returns all pending events that have a non-null visible_at (timers / scheduled future events).
func (pb *postgresBackend) GetFutureEvents(ctx context.Context) ([]*history.Event, error) {
	tx, err := pb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// attributes now lives in the separate attributes table (joined by event_id + instance_id + execution_id)
	rows, err := tx.QueryContext(ctx,
		`SELECT 
            pe.event_id,
            pe.sequence_id,
            pe.event_type,
            pe.timestamp,
            pe.schedule_event_id,
            a.data,
            pe.visible_at
         FROM pending_events pe
         JOIN attributes a 
           ON a.event_id = pe.event_id 
          AND a.instance_id = pe.instance_id 
          AND a.execution_id = pe.execution_id
         WHERE pe.visible_at IS NOT NULL`)
	if err != nil {
		return nil, fmt.Errorf("querying future events: %w", err)
	}
	defer rows.Close()

	var future []*history.Event
	for rows.Next() {
		var raw []byte
		e := &history.Event{}
		if err := rows.Scan(
			&e.ID,
			&e.SequenceID,
			&e.Type,
			&e.Timestamp,
			&e.ScheduleEventID,
			&raw,
			&e.VisibleAt,
		); err != nil {
			return nil, fmt.Errorf("scanning future event: %w", err)
		}

		attr, err := history.DeserializeAttributes(e.Type, raw)
		if err != nil {
			return nil, fmt.Errorf("deserializing attributes: %w", err)
		}
		e.Attributes = attr

		future = append(future, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return future, nil
}

func Test_PostgresBackend_WorkerName(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Run("DefaultWorkerName", func(t *testing.T) {
		options := &options{
			Options: backend.ApplyOptions(),
		}
		workerName := getWorkerName(options)

		if !strings.HasPrefix(workerName, "worker-") {
			t.Errorf("Expected worker name to start with 'worker-', got: %s", workerName)
		}
		if len(workerName) != 43 { // 7 + 36
			t.Errorf("Expected worker name length 43, got: %d", len(workerName))
		}
	})

	t.Run("CustomWorkerName", func(t *testing.T) {
		custom := "test-worker-123"
		options := &options{
			Options: backend.ApplyOptions(backend.WithWorkerName(custom)),
		}
		if got := getWorkerName(options); got != custom {
			t.Errorf("Expected '%s', got '%s'", custom, got)
		}
	})

	t.Run("EmptyWorkerNameUsesDefault", func(t *testing.T) {
		options := &options{
			Options: backend.ApplyOptions(backend.WithWorkerName("")),
		}
		workerName := getWorkerName(options)
		if !strings.HasPrefix(workerName, "worker-") {
			t.Errorf("Expected worker name to start with 'worker-', got: %s", workerName)
		}
		if len(workerName) != 43 {
			t.Errorf("Expected worker name length 43, got: %d", len(workerName))
		}
	})
}

func Test_PostgresBackendWithDB(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Run("UsesProvidedConnection", func(t *testing.T) {
		// Create database for test
		adminDB, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=postgres sslmode=disable", testUser, testPassword))
		if err != nil {
			t.Fatal(err)
		}

		dbName := "test_withdb_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		if _, err := adminDB.Exec("CREATE DATABASE " + dbName); err != nil {
			t.Fatal(err)
		}
		defer func() {
			adminDB.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)")
			adminDB.Close()
		}()

		// Create our own connection to the test database
		db, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=%s sslmode=disable", testUser, testPassword, dbName))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		// Create backend with existing connection and apply migrations
		backend := NewPostgresBackendWithDB(db, WithApplyMigrations(true))

		// Verify the backend uses our connection
		if backend.db != db {
			t.Error("Backend should use provided db connection")
		}
		if backend.ownsConnection {
			t.Error("Backend should not own the connection")
		}

		// Close backend - should NOT close our connection
		if err := backend.Close(); err != nil {
			t.Fatal(err)
		}

		// Verify our connection is still usable
		if err := db.Ping(); err != nil {
			t.Errorf("Connection should still be open after backend.Close(): %v", err)
		}
	})

	t.Run("MigrationsDisabledByDefault", func(t *testing.T) {
		// Create database for test
		adminDB, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=postgres sslmode=disable", testUser, testPassword))
		if err != nil {
			t.Fatal(err)
		}

		dbName := "test_withdb2_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		if _, err := adminDB.Exec("CREATE DATABASE " + dbName); err != nil {
			t.Fatal(err)
		}
		defer func() {
			adminDB.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)")
			adminDB.Close()
		}()

		db, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=%s sslmode=disable", testUser, testPassword, dbName))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		// Create backend without enabling migrations
		backend := NewPostgresBackendWithDB(db)
		defer backend.Close()

		// Tables should not exist since migrations weren't applied
		_, err = db.Exec("SELECT 1 FROM instances LIMIT 1")
		if err == nil {
			t.Error("Expected error because table should not exist")
		}
	})

	t.Run("MigrationsCanBeEnabled", func(t *testing.T) {
		// Create database for test
		adminDB, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=postgres sslmode=disable", testUser, testPassword))
		if err != nil {
			t.Fatal(err)
		}

		dbName := "test_withdb3_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		if _, err := adminDB.Exec("CREATE DATABASE " + dbName); err != nil {
			t.Fatal(err)
		}
		defer func() {
			adminDB.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)")
			adminDB.Close()
		}()

		db, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=%s sslmode=disable", testUser, testPassword, dbName))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		// Create backend with migrations enabled
		backend := NewPostgresBackendWithDB(db, WithApplyMigrations(true))
		defer backend.Close()

		// Tables should exist
		_, err = db.Exec("SELECT 1 FROM instances LIMIT 1")
		if err != nil {
			t.Errorf("Table should exist after migrations: %v", err)
		}
	})
}


// Test_PostgresBackend_ConcurrentCreate verifies the fix for the duplicate active
// instance race (upstream #489): N concurrent CreateWorkflowInstance calls with the
// same InstanceID must result in exactly one active instance; the rest must fail
// with ErrInstanceAlreadyExists. Without the partial unique index, multiple active
// rows could be created.
func Test_PostgresBackend_ConcurrentCreate(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	adminDB, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=postgres sslmode=disable", testUser, testPassword))
	if err != nil {
		t.Fatal(err)
	}
	defer adminDB.Close()

	dbName := "test_race_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminDB.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatal(err)
	}
	defer adminDB.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)")

	b := NewPostgresBackend("localhost", 5432, testUser, testPassword, dbName)
	defer b.db.Close()

	ctx := context.Background()
	instanceID := "race-" + uuid.NewString()

	const n = 20
	var wg sync.WaitGroup
	results := make([]error, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start // release all goroutines at once to maximize contention
			results[idx] = b.CreateWorkflowInstance(
				ctx,
				core.NewWorkflowInstance(instanceID, uuid.NewString()),
				history.NewHistoryEvent(1, time.Now(), history.EventType_WorkflowExecutionStarted, &history.ExecutionStartedAttributes{
					Queue: workflow.QueueDefault,
				}),
			)
		}(i)
	}

	close(start)
	wg.Wait()

	success := 0
	alreadyExists := 0
	for _, err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, backend.ErrInstanceAlreadyExists):
			alreadyExists++
		default:
			t.Fatalf("unexpected error from CreateWorkflowInstance: %v", err)
		}
	}

	if success != 1 {
		t.Fatalf("expected exactly 1 successful create, got %d (alreadyExists=%d)", success, alreadyExists)
	}
	if alreadyExists != n-1 {
		t.Fatalf("expected %d ErrInstanceAlreadyExists, got %d", n-1, alreadyExists)
	}

	// Confirm exactly one active row exists for the instance id.
	var activeCount int
	if err := b.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM instances WHERE instance_id = $1 AND state = $2",
		instanceID, core.WorkflowInstanceStateActive,
	).Scan(&activeCount); err != nil {
		t.Fatal(err)
	}
	if activeCount != 1 {
		t.Fatalf("expected exactly 1 active instance row, got %d", activeCount)
	}
}
