package postgres

import (
	"context"
	"testing"

	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/diag"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/cschleiden/go-workflows/workflow"
	"github.com/google/uuid"
)

// filterWfAlpha and filterWfBeta are two distinctly-named workflows so the
// workflow-name filter has something to discriminate on.
func filterWfAlpha(ctx workflow.Context) (int, error) { return 1, nil }
func filterWfBeta(ctx workflow.Context) (int, error)  { return 2, nil }

// Test_Diag_GetWorkflowInstancesFiltered exercises the dynamic filtered query
// (workflow name substring + queue) against a real database.
func Test_Diag_GetWorkflowInstancesFiltered(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, c := newPromoteBackend(t, ctx, func(w *worker.Worker) {
		if err := w.RegisterWorkflow(filterWfAlpha); err != nil {
			t.Fatal(err)
		}
		if err := w.RegisterWorkflow(filterWfBeta); err != nil {
			t.Fatal(err)
		}
	})

	// Two alpha instances on the default queue, one beta on a custom queue.
	for i := 0; i < 2; i++ {
		if _, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
			InstanceID: "alpha-" + uuid.NewString(),
		}, filterWfAlpha); err != nil {
			t.Fatal(err)
		}
	}
	betaWfi, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: "beta-" + uuid.NewString(),
		Queue:      "custom-q",
	}, filterWfBeta)
	if err != nil {
		t.Fatal(err)
	}

	// Filter by workflow name substring "Alpha" -> exactly the 2 alpha instances.
	byName, err := b.GetWorkflowInstancesFiltered(ctx, diag.InstanceFilter{WorkflowName: "Alpha"}, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(byName) != 2 {
		t.Fatalf("name filter 'Alpha': expected 2, got %d", len(byName))
	}
	for _, r := range byName {
		if r.WorkflowName != "filterWfAlpha" {
			t.Fatalf("expected filterWfAlpha, got %q", r.WorkflowName)
		}
	}

	// Filter by queue "custom-q" -> exactly the 1 beta instance.
	byQueue, err := b.GetWorkflowInstancesFiltered(ctx, diag.InstanceFilter{Queue: "custom-q"}, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(byQueue) != 1 {
		t.Fatalf("queue filter 'custom-q': expected 1, got %d", len(byQueue))
	}
	if byQueue[0].Instance.InstanceID != betaWfi.InstanceID {
		t.Fatalf("queue filter returned wrong instance: %s", byQueue[0].Instance.InstanceID)
	}

	// State filter: the query must run and only return instances in the requested
	// state. Workflows here finish near-instantly, so we don't assert a count
	// (avoids a race with the worker); we assert the state constraint holds.
	active := core.WorkflowInstanceStateActive
	byState, err := b.GetWorkflowInstancesFiltered(ctx, diag.InstanceFilter{State: &active}, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range byState {
		if r.State != core.WorkflowInstanceStateActive {
			t.Fatalf("state filter Active returned a %v instance", r.State)
		}
	}

	// Empty filter behaves like the unfiltered call.
	all, err := b.GetWorkflowInstancesFiltered(ctx, diag.InstanceFilter{}, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < 3 {
		t.Fatalf("empty filter: expected >=3, got %d", len(all))
	}
}
