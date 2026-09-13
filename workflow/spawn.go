package workflow

import (
	"errors"
	"fmt"

	"github.com/cschleiden/go-workflows/core"
	a "github.com/cschleiden/go-workflows/internal/args"
	"github.com/cschleiden/go-workflows/internal/command"
	"github.com/cschleiden/go-workflows/internal/contextvalue"
	"github.com/cschleiden/go-workflows/internal/fn"
	"github.com/cschleiden/go-workflows/internal/tracing"
	"github.com/cschleiden/go-workflows/internal/workflowstate"
)

// SpawnOptions configures a detached, top-level workflow launch via SpawnWorkflow.
type SpawnOptions struct {
	// InstanceID is the ID of the spawned workflow instance. It is required and must
	// be deterministic so the instance stays addressable (for signals and cancellation).
	InstanceID string

	// Queue to use for the spawned workflow. If not set, the queue is resolved by the
	// backend (same behavior as sub-workflows).
	Queue Queue
}

// SpawnWorkflow launches a new, detached, top-level workflow instance from within a
// running workflow.
//
// Unlike CreateSubWorkflowInstance, the spawned workflow:
//   - has no parent/child relationship (it is a root instance and shows as a top-level run),
//   - has no tracked future, so the caller cannot await its result and the calling
//     workflow will not fail the pending-futures check on completion,
//   - is not cancelled automatically when the calling workflow completes or is cancelled.
//
// The spawned workflow remains addressable by its InstanceID: it can be signalled and
// explicitly cancelled (e.g. via client.SignalWorkflow / client.CancelWorkflowInstance).
//
// InstanceID is required. If a workflow instance with the same InstanceID already exists,
// the backend rejects the duplicate and the existing instance is left running.
func SpawnWorkflow(ctx Context, options SpawnOptions, wf Workflow, args ...any) (*core.WorkflowInstance, error) {
	if options.InstanceID == "" {
		return nil, errors.New("SpawnWorkflow: InstanceID is required")
	}

	// If the context is already canceled, do not spawn.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Resolve workflow name and validate arguments.
	var workflowName string
	if name, ok := wf.(string); ok {
		workflowName = name
	} else {
		workflowName = fn.Name(wf)

		if err := a.ParamsMatch(wf, args...); err != nil {
			return nil, err
		}
	}

	cv := contextvalue.Converter(ctx)
	inputs, err := a.ArgsToInputs(cv, args...)
	if err != nil {
		return nil, fmt.Errorf("converting spawn workflow input: %w", err)
	}

	wfState := workflowstate.WorkflowState(ctx)

	// Consume a schedule event ID so replay ordering stays deterministic, matching the
	// sub-workflow path.
	scheduleEventID := wfState.GetNextScheduleEventID()

	// Capture context (propagators) into metadata, same as sub-workflow.
	propagators := propagators(ctx)
	md := &Metadata{}
	if err := injectFromWorkflow(ctx, md, propagators); err != nil {
		return nil, fmt.Errorf("injecting workflow context: %w", err)
	}

	tracer := wfState.Tracer()
	workflowSpanID := tracing.GetNewSpanID(tracer)

	cmd := command.NewSpawnWorkflowCommand(
		scheduleEventID,
		options.Queue,
		options.InstanceID,
		workflowName,
		inputs,
		md,
		workflowSpanID,
	)

	wfState.AddCommand(cmd)
	// Intentionally NOT calling wfState.TrackFuture: the spawned workflow is detached and
	// has no future to await, which is what keeps the caller free of pending futures.

	return cmd.Instance, nil
}
