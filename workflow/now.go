package workflow

import (
	"time"

	"github.com/denisaditya0/go-workflows/internal/workflowstate"
)

// Now returns the current time.
func Now(ctx Context) time.Time {
	wfState := workflowstate.WorkflowState(ctx)
	return wfState.Time()
}
