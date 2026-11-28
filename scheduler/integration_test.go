package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/denisaditya0/go-workflows/backend/postgres"
	"github.com/denisaditya0/go-workflows/core"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestSchedulerPostgres runs the scheduler against a real PostgreSQL database and
// verifies that a due schedule creates a workflow instance. It is skipped in
// -short mode and requires the docker-compose Postgres service.
func TestSchedulerPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires PostgreSQL")
	}

	const host = "localhost"
	const port = 5432
	const user = "root"
	const password = "root"

	admin, err := sql.Open("pgx", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable", host, port, user, password))
	if err != nil {
		t.Fatalf("connecting to postgres: %v", err)
	}
	defer admin.Close()

	if err := admin.Ping(); err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	dbName := "sched_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatalf("creating database: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)"); err != nil {
			t.Logf("dropping database: %v", err)
		}
	})

	b := postgres.NewPostgresBackend(host, port, user, password, dbName)
	t.Cleanup(func() {
		_ = b.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := New(b, &Options{PollInterval: 50 * time.Millisecond, LeaseDuration: 5 * time.Second, BatchSize: 10})
	if err != nil {
		t.Fatal(err)
	}

	startAt := time.Now().Add(-time.Second).UTC().Truncate(time.Second)
	_, err = s.Create(ctx, ScheduleSpec{
		ID:       "nightly",
		Workflow: "NightlyWorkflow",
		Interval: time.Hour,
		StartAt:  &startAt,
		Args:     []any{"hello"},
	})
	if err != nil {
		t.Fatalf("creating schedule: %v", err)
	}

	if err := s.Start(ctx); err != nil {
		t.Fatalf("starting scheduler: %v", err)
	}

	var instanceID, executionID string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		schedule, err := s.Get(ctx, "nightly")
		if err != nil {
			t.Fatalf("getting schedule: %v", err)
		}
		if schedule.LastInstanceID != "" {
			instanceID = schedule.LastInstanceID
			executionID = schedule.LastExecutionID
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if instanceID == "" {
		t.Fatal("scheduler did not fire within timeout")
	}

	want := scheduledInstanceID("nightly", startAt)
	if instanceID != want {
		t.Fatalf("instance ID = %q, want %q", instanceID, want)
	}

	state, err := b.GetWorkflowInstanceState(ctx, &core.WorkflowInstance{InstanceID: instanceID, ExecutionID: executionID})
	if err != nil {
		t.Fatalf("getting instance state: %v", err)
	}
	if state != core.WorkflowInstanceStateActive {
		t.Fatalf("instance state = %v, want active", state)
	}

	s.Stop()
	if err := s.WaitForCompletion(); err != nil {
		t.Fatalf("stopping scheduler: %v", err)
	}
}
