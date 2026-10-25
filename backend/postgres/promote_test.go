package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/backend/converter"
	"github.com/cschleiden/go-workflows/backend/payload"
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

// continueOnceWorkflow continues-as-new exactly once (input 0 -> continue with 1 -> finish).
func continueOnceWorkflow(ctx workflow.Context, n int) (int, error) {
	if n == 0 {
		return 0, workflow.ContinueAsNew(ctx, 1)
	}
	return n, nil
}

func promoteSubChild(ctx workflow.Context) (int, error) { return 7, nil }

func promoteSubParent(ctx workflow.Context) (int, error) {
	return workflow.CreateSubWorkflowInstance[int](ctx, workflow.DefaultSubWorkflowOptions, promoteSubChild).Get(ctx)
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


// Test_Promote_PreviousExecutionID_Continuation verifies that a ContinueAsNew
// continuation records its predecessor's execution_id in previous_execution_id, while
// the original run has it null.
func Test_Promote_PreviousExecutionID_Continuation(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, c := newPromoteBackend(t, ctx, func(w *worker.Worker) {
		if err := w.RegisterWorkflow(continueOnceWorkflow); err != nil {
			t.Fatal(err)
		}
	})

	wfi, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: "cont-" + uuid.NewString(),
	}, continueOnceWorkflow, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the logical workflow to finish (across the continuation).
	if _, err := client.GetWorkflowResult[int](ctx, c, wfi, 15*time.Second); err != nil {
		t.Fatal(err)
	}

	// There should be two execution rows for this instance_id: the original
	// (previous_execution_id null) and the continuation (previous_execution_id = original).
	rows, err := b.db.QueryContext(ctx,
		"SELECT execution_id, previous_execution_id FROM instances WHERE instance_id = $1",
		wfi.InstanceID,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var originals, continuations int
	var originalExecID string
	type row struct {
		exec string
		prev sql.NullString
	}
	var all []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.exec, &r.prev); err != nil {
			t.Fatal(err)
		}
		all = append(all, r)
		if !r.prev.Valid {
			originals++
			originalExecID = r.exec
		} else {
			continuations++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if len(all) != 2 {
		t.Fatalf("expected 2 execution rows for the instance, got %d", len(all))
	}
	if originals != 1 {
		t.Fatalf("expected exactly 1 original (null previous_execution_id), got %d", originals)
	}
	if continuations != 1 {
		t.Fatalf("expected exactly 1 continuation, got %d", continuations)
	}
	// The continuation's previous_execution_id must point at the original's execution_id.
	for _, r := range all {
		if r.prev.Valid && r.prev.String != originalExecID {
			t.Fatalf("continuation previous_execution_id = %q, expected original %q", r.prev.String, originalExecID)
		}
	}
}

// Test_Promote_PreviousExecutionID_FreshAndSub verifies that a plain top-level
// workflow and a sub-workflow both have null previous_execution_id (and the sub-workflow
// keeps its parent_* link — the two relationships are not confused).
func Test_Promote_PreviousExecutionID_FreshAndSub(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, c := newPromoteBackend(t, ctx, func(w *worker.Worker) {
		if err := w.RegisterWorkflow(promoteSubParent); err != nil {
			t.Fatal(err)
		}
		if err := w.RegisterWorkflow(promoteSubChild); err != nil {
			t.Fatal(err)
		}
	})

	wfi, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: "sub-" + uuid.NewString(),
	}, promoteSubParent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetWorkflowResult[int](ctx, c, wfi, 15*time.Second); err != nil {
		t.Fatal(err)
	}

	// Parent (fresh top-level): previous_execution_id null, parent_instance_id null.
	var parentPrev, parentParent sql.NullString
	if err := b.db.QueryRowContext(ctx,
		"SELECT previous_execution_id, parent_instance_id FROM instances WHERE instance_id = $1 AND execution_id = $2",
		wfi.InstanceID, wfi.ExecutionID,
	).Scan(&parentPrev, &parentParent); err != nil {
		t.Fatal(err)
	}
	if parentPrev.Valid {
		t.Fatalf("fresh top-level workflow should have null previous_execution_id, got %q", parentPrev.String)
	}
	if parentParent.Valid {
		t.Fatalf("top-level workflow should have null parent_instance_id, got %q", parentParent.String)
	}

	// Sub-workflow: previous_execution_id null, parent_instance_id = the parent.
	var subPrev, subParent sql.NullString
	if err := b.db.QueryRowContext(ctx,
		"SELECT previous_execution_id, parent_instance_id FROM instances WHERE parent_instance_id = $1",
		wfi.InstanceID,
	).Scan(&subPrev, &subParent); err != nil {
		t.Fatal(err)
	}
	if subPrev.Valid {
		t.Fatalf("sub-workflow should have null previous_execution_id, got %q", subPrev.String)
	}
	if !subParent.Valid || subParent.String != wfi.InstanceID {
		t.Fatalf("sub-workflow parent_instance_id = %v, expected %q", subParent, wfi.InstanceID)
	}
}


// Test_Promote_InputSourcedFromColumn is the decisive proof for Phase 2 step 1:
// it makes instances.input DISAGREE with the attributes blob, then checks which
// value the workflow actually runs with. If the column wins, injection is genuinely
// the source. (promoteWorkflow returns n*2, so the result reveals the input used.)
func Test_Promote_InputSourcedFromColumn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build the backend WITHOUT starting a worker yet, so we can corrupt the column
	// before the first GetWorkflowTask reads it.
	adminDB, err := sql.Open("pgx", fmt.Sprintf("host=localhost port=5432 user=%s password=%s dbname=postgres sslmode=disable", testUser, testPassword))
	if err != nil {
		t.Fatal(err)
	}
	defer adminDB.Close()
	dbName := "test_diverge_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminDB.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatal(err)
	}
	defer adminDB.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)")

	b := NewPostgresBackend("localhost", 5432, testUser, testPassword, dbName)
	defer b.db.Close()

	c := client.New(b)

	// Create with blob input = 21 (blob-sourced result would be 42).
	wfi, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: "diverge-" + uuid.NewString(),
	}, promoteWorkflow, 21)
	if err != nil {
		t.Fatal(err)
	}

	// Overwrite ONLY the instances.input column with a different valid value: 5.
	// Column-sourced result would be 10.
	p5, err := converter.DefaultConverter.To(5)
	if err != nil {
		t.Fatal(err)
	}
	colInput, err := json.Marshal([]payload.Payload{p5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.db.ExecContext(ctx,
		"UPDATE instances SET input = $1 WHERE instance_id = $2 AND execution_id = $3",
		colInput, wfi.InstanceID, wfi.ExecutionID,
	); err != nil {
		t.Fatal(err)
	}

	// NOW start the worker so the first task read sees the corrupted column.
	w := worker.New(b, nil)
	if err := w.RegisterWorkflow(promoteWorkflow); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := client.GetWorkflowResult[int](ctx, c, wfi, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// 10 => column was the source (PROVEN). 42 => blob was the source (injection is a no-op).
	if result != 10 {
		t.Fatalf("expected result 10 (input from column=5), got %d — input was NOT sourced from the column", result)
	}
}

// Test_Promote_InputColumn_ConcurrentLoad is the robustness backstop: many workflows
// with distinct inputs run concurrently through real workers; all must complete with
// the correct per-instance result. Exercises the modified GetWorkflowTask read path
// under concurrency (guards against connection/race regressions like the earlier
// "bad connection" bug).
func Test_Promote_InputColumn_ConcurrentLoad(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, c := newPromoteBackend(t, ctx, func(w *worker.Worker) {
		if err := w.RegisterWorkflow(promoteWorkflow); err != nil {
			t.Fatal(err)
		}
	})

	const n = 40
	type started struct {
		wfi   *workflow.Instance
		input int
	}
	starts := make([]started, 0, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wfi, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
				InstanceID: fmt.Sprintf("load-%d-%s", i, uuid.NewString()),
			}, promoteWorkflow, i)
			if err != nil {
				t.Errorf("create %d: %v", i, err)
				return
			}
			mu.Lock()
			starts = append(starts, started{wfi, i})
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if len(starts) != n {
		t.Fatalf("expected %d started, got %d", n, len(starts))
	}

	for _, s := range starts {
		result, err := client.GetWorkflowResult[int](ctx, c, s.wfi, 30*time.Second)
		if err != nil {
			t.Fatalf("instance input=%d: %v", s.input, err)
		}
		if result != s.input*2 {
			t.Fatalf("instance input=%d: expected %d, got %d", s.input, s.input*2, result)
		}
	}
}
