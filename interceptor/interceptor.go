// Package interceptor provides interfaces for intercepting workflow and activity execution.
//
// Interceptors allow cross-cutting behavior (logging, metrics, validation, delays) to be added
// to workflow and activity execution without modifying individual functions.
//
// Interceptors are called in order, forming a chain. Each interceptor must call the next handler
// to proceed, or return without calling it to short-circuit execution.
package interceptor

import (
	"context"

	"github.com/cschleiden/go-workflows/core"
	isync "github.com/cschleiden/go-workflows/internal/sync"
)

// WorkflowContext is the context type used in workflow interceptors.
// This is the same type as workflow.Context.
type WorkflowContext = isync.Context

// WorkflowInfo provides metadata about the workflow being executed.
type WorkflowInfo struct {
	// Name is the registered name of the workflow function.
	Name string

	// Instance is the workflow instance being executed.
	Instance *core.WorkflowInstance
}

// ActivityInfo provides metadata about the activity being executed.
type ActivityInfo struct {
	// Name is the registered name of the activity function.
	Name string

	// Attempt is the current attempt number (starts at 1).
	Attempt int
}

// WorkflowHandler is the next function in the workflow interceptor chain.
type WorkflowHandler func(ctx WorkflowContext) error

// ActivityHandler is the next function in the activity interceptor chain.
type ActivityHandler func(ctx context.Context) error

// WorkflowInterceptor intercepts workflow execution.
type WorkflowInterceptor interface {
	// ExecuteWorkflow is called when a workflow is about to execute.
	// Call next(ctx) to proceed to the next interceptor or the actual workflow.
	// Return without calling next to short-circuit execution.
	ExecuteWorkflow(ctx WorkflowContext, info *WorkflowInfo, next WorkflowHandler) error
}

// ActivityInterceptor intercepts activity execution.
type ActivityInterceptor interface {
	// ExecuteActivity is called when an activity is about to execute.
	// Call next(ctx) to proceed to the next interceptor or the actual activity.
	// Return without calling next to short-circuit execution.
	ExecuteActivity(ctx context.Context, info *ActivityInfo, next ActivityHandler) error
}

// Interceptor combines both workflow and activity interception.
// Implement this interface for interceptors that need to intercept both.
type Interceptor interface {
	WorkflowInterceptor
	ActivityInterceptor
}
