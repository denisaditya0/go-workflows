package tester

import (
	"context"
	"testing"

	"github.com/cschleiden/go-workflows/workflow"
	"github.com/stretchr/testify/require"
)

// Test_SpawnWorkflow verifies that a workflow which spawns a detached, top-level
// workflow via workflow.SpawnWorkflow completes cleanly. Because SpawnWorkflow does
// not track a future, the parent must not trigger the pending-futures panic even
// though it never awaits the spawned workflow.
func Test_SpawnWorkflow(t *testing.T) {
	const spawnedID = "spawned-instance-id"

	spawned := func(ctx workflow.Context, input string) (string, error) {
		return input + "-spawned", nil
	}

	parent := func(ctx workflow.Context, input string) (string, error) {
		inst, err := workflow.SpawnWorkflow(ctx, workflow.SpawnOptions{
			InstanceID: spawnedID,
		}, spawned, input)
		if err != nil {
			return "", err
		}

		// The caller gets the instance id but does not (and cannot) await a result.
		return inst.InstanceID, nil
	}

	tester := NewWorkflowTester[string](parent)
	tester.Registry().RegisterWorkflow(spawned)

	tester.Execute(context.Background(), "hello")

	// The key assertion: the parent finished cleanly with no pending-futures panic.
	require.True(t, tester.WorkflowFinished())

	wfR, wfErr := tester.WorkflowResult()
	require.Empty(t, wfErr)
	require.Equal(t, spawnedID, wfR)

	tester.AssertExpectations(t)
}

// Test_SpawnWorkflow_RequiresInstanceID verifies that SpawnWorkflow rejects an empty
// InstanceID rather than falling back to a random UUID.
func Test_SpawnWorkflow_RequiresInstanceID(t *testing.T) {
	spawned := func(ctx workflow.Context) error {
		return nil
	}

	parent := func(ctx workflow.Context) (bool, error) {
		_, err := workflow.SpawnWorkflow(ctx, workflow.SpawnOptions{}, spawned)
		// Return whether an error was produced so we can assert on it.
		return err != nil, nil
	}

	tester := NewWorkflowTester[bool](parent)
	tester.Registry().RegisterWorkflow(spawned)

	tester.Execute(context.Background())

	require.True(t, tester.WorkflowFinished())

	gotErr, _ := tester.WorkflowResult()
	require.True(t, gotErr, "SpawnWorkflow should return an error when InstanceID is empty")
}
