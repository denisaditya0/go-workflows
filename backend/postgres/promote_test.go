package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/cschleiden/go-workflows/workflow"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func promoteWorkflow(ctx workflow.Context, n int) (int, error) {
	return n * 2, nil
}

func promoteFailWorkflow(ctx workflow.Context) (int, error) {
	return 0, errors.New("boom")
}

// newPromoteBackend spins up an isolated DB + worker for the promotion tests.
func newPromoteBackend(t *testing.T, ctx context.Context, register func(*worker.Worker)) (*postgresBackend, *client.Client) {
	t.Helper()

	adminDB, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=postgres sslmode=disable", testUser, testPassword))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { adminDB.Close() })

	dbName := "test_promote_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminDB.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { adminDB.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)") })

	b := NewPostgresBackend("localhost", 5432, testUser, testPassword, dbName)
	t.Cleanup(func() { b.db.Close() })

	w := worker.New(b, nil)
	register(w)
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}

	return b, client.New(b)
}

// Test_Promote_InputAndName verifies that workflow_name and input are populated on the
// instances row at creation time (migration 000004 / Phase 1).
func Test_Promote_InputAndName(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, c := newPromoteBackend(t, ctx, func(w *worker.Worker) {
		if err := w.RegisterWorkflow(promoteWorkflow); err != nil {
			t.Fatal(err)
		}
	})

	wfi, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: "promote-" + uuid.NewString(),
	}, promoteWorkflow, 21)
	if err != nil {
		t.Fatal(err)
	}

	var name string
	var input []byte
	if err := b.db.QueryRowContext(ctx,
		"SELECT workflow_name, input FROM instances WHERE instance_id = $1 AND execution_id = $2",
		wfi.InstanceID, wfi.ExecutionID,
	).Scan(&name, &input); err != nil {
		t.Fatal(err)
	}
	if name != "promoteWorkflow" {
		t.Fatalf("expected workflow_name 'promoteWorkflow', got %q", name)
	}
	if len(input) == 0 {
		t.Fatal("expected input column populated on create")
	}
}

// Test_Promote_ResultOnSuccess verifies result is populated and error is null when a
// workflow completes successfully.
func Test_Promote_ResultOnSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, c := newPromoteBackend(t, ctx, func(w *worker.Worker) {
		if err := w.RegisterWorkflow(promoteWorkflow); err != nil {
			t.Fatal(err)
		}
	})

	wfi, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: "promote-ok-" + uuid.NewString(),
	}, promoteWorkflow, 21)
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.GetWorkflowResult[int](ctx, c, wfi, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if res != 42 {
		t.Fatalf("expected 42, got %d", res)
	}

	var result, errCol []byte
	if err := b.db.QueryRowContext(ctx,
		"SELECT result, error FROM instances WHERE instance_id = $1 AND execution_id = $2",
		wfi.InstanceID, wfi.ExecutionID,
	).Scan(&result, &errCol); err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Fatal("expected result column populated on success")
	}
	if len(errCol) != 0 {
		t.Fatalf("expected error column null on success, got %s", string(errCol))
	}
}

// Test_Promote_ErrorOnFailure verifies error is populated when a workflow fails.
func Test_Promote_ErrorOnFailure(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, c := newPromoteBackend(t, ctx, func(w *worker.Worker) {
		if err := w.RegisterWorkflow(promoteFailWorkflow); err != nil {
			t.Fatal(err)
		}
	})

	wfi, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: "promote-fail-" + uuid.NewString(),
	}, promoteFailWorkflow)
	if err != nil {
		t.Fatal(err)
	}

	// Workflow fails; GetWorkflowResult returns the error.
	_, _ = client.GetWorkflowResult[int](ctx, c, wfi, 15*time.Second)

	var errCol []byte
	if err := b.db.QueryRowContext(ctx,
		"SELECT error FROM instances WHERE instance_id = $1 AND execution_id = $2",
		wfi.InstanceID, wfi.ExecutionID,
	).Scan(&errCol); err != nil {
		t.Fatal(err)
	}
	if len(errCol) == 0 {
		t.Fatal("expected error column populated on failure")
	}
	if !strings.Contains(string(errCol), "boom") {
		t.Fatalf("expected error to contain 'boom', got %s", string(errCol))
	}
}
