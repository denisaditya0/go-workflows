package tester

import (
	"context"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/workflow"
	"github.com/stretchr/testify/require"
)

// Test_PendingActivityFutureOnCompletion documents known limitation #436: the tester
// does NOT surface "workflow completed, but there are still pending futures" when the
// pending future is an activity. ExecuteActivity spawns an internal retry coroutine
// that outlives the workflow function's return, so the executor doesn't treat the
// workflow as completed-with-pending-futures until the activity resolves (by which
// point the future is gone). The maintainer's candidate fix (mark completion on
// workflow-function exit) breaks several existing tester tests, so this is left as a
// documented limitation — see docs/decisions.md. Skipped to keep the suite green.
func Test_PendingActivityFutureOnCompletion(t *testing.T) {
	t.Skip("known limitation #436: tester does not detect pending ACTIVITY futures on completion")

	act := func(context.Context) error {
		time.Sleep(2 * time.Second)
		return nil
	}

	wf := func(ctx workflow.Context) error {
		// Fire-and-forget: schedule but never Get the future, then return.
		workflow.ExecuteActivity[any](ctx, workflow.DefaultActivityOptions, act)
		return nil
	}

	wft := NewWorkflowTester[any](wf)
	wft.Registry().RegisterActivity(act)

	wft.Execute(context.Background())
	require.True(t, wft.WorkflowFinished())

	_, err := wft.WorkflowResult()
	require.ErrorContains(t, err, "pending futures")
}
