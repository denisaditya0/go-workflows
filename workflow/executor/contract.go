// Package executor holds the public contract for the workflow execution engine.
//
// The engine implementation lives in internal/workflowengine; this package keeps
// only the types that callers need to reference (e.g. to provide a custom
// workflow-executor Cache via worker.Options): WorkflowExecutor, ExecutionResult,
// WorkflowHistoryProvider, and Cache.
package executor

import (
	"context"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/core"
)

// ExecutionResult is the outcome of executing a workflow task.
type ExecutionResult struct {
	// New state of the workflow instance
	State core.WorkflowInstanceState

	// Events executed during the task execution
	Executed []*history.Event

	// Activities that were scheduled
	ActivityEvents []*history.Event

	// Timers that were scheduled
	TimerEvents []*history.Event

	// Events for other workflow instances
	WorkflowEvents []*history.WorkflowEvent
}

// WorkflowHistoryProvider loads prior history for an instance so the executor can
// replay it to rebuild in-memory state.
type WorkflowHistoryProvider interface {
	GetWorkflowInstanceHistory(ctx context.Context, instance *core.WorkflowInstance, lastSequenceID *int64) ([]*history.Event, error)
}

// WorkflowExecutor executes workflow tasks and can be cached between tasks.
type WorkflowExecutor interface {
	ExecuteTask(ctx context.Context, t *backend.WorkflowTask) (*ExecutionResult, error)

	Close()
}
