// Package middleware defines optional extension points around go-workflows.
//
// There are two distinct kinds of extension point:
//
//   - Middleware wraps an operation. It receives the operation's parameters and
//     a next function; it must call next to continue, and may run code before and
//     after. This is the place for instrumentation, tracing adapters, logging,
//     retries of client calls, and similar concerns.
//
//   - Hook observes events. It is a pure callback with no control over
//     execution: it cannot veto, short-circuit, delay, or modify anything. This is
//     the place for event streams, metrics, and audit logs.
//
// Both extension points are optional. An empty (or nil) Middlewares and an empty
// (or nil) Hooks slice are valid and add no overhead beyond a function call.
//
// # Determinism
//
// Middleware and hooks are deliberately kept out of the deterministic workflow
// execution. A workflow task replay must produce the exact same commands and
// events as the original execution; anything that could change that belongs in
// neither extension point.
//
// Concretely:
//
//   - Hooks are invoked for events only once they have been committed to the
//     backend, never while replaying history. They must not write to the
//     backend or emit workflow commands.
//   - Middleware receives read-only parameters. It must not mutate the task or
//     params, and it must not make work derived from them depend on
//     non-deterministic values that the workflow code would later replay.
//
// # Forward compatibility
//
// Embed the base struct that corresponds to the interface you are implementing:
//
//	type myMiddleware struct {
//		middleware.MiddlewareBase
//	}
//
//	func (m *myMiddleware) ExecuteActivity(ctx context.Context, params *middleware.ActivityTaskParams, next func(context.Context) error) error {
//		// ...
//		return next(ctx)
//	}
//
// and pass a pointer to your struct. New methods can then be added to the
// interfaces in future releases without breaking existing implementations, since
// the embedded base supplies them.
package middleware

import (
	"context"

	"github.com/denisaditya0/go-workflows/backend"
	"github.com/denisaditya0/go-workflows/backend/metadata"
	"github.com/denisaditya0/go-workflows/backend/payload"
	"github.com/denisaditya0/go-workflows/core"
	"github.com/denisaditya0/go-workflows/workflow/executor"
)

// Middleware wraps operations at the edges of the engine: control-plane calls
// made through the client, and task execution performed by a worker.
//
// A middleware is the outermost of a chain, and must call next to continue. The
// parameters passed to a middleware are read-only.
//
// Middleware must not be added to, removed from, or reorder the events a
// workflow task produces; that would break replay.
type Middleware interface {
	// CreateWorkflowInstance wraps client.Client.CreateWorkflowInstance.
	//
	// Instance, Queue, Name, Inputs, and Metadata are populated before the
	// middleware is invoked.
	CreateWorkflowInstance(ctx context.Context, params *CreateWorkflowInstanceParams, next func(context.Context) error) error

	// CancelWorkflowInstance wraps client.Client.CancelWorkflowInstance.
	CancelWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance, next func(context.Context) error) error

	// SignalWorkflow wraps client.Client.SignalWorkflow.
	SignalWorkflow(ctx context.Context, params *SignalWorkflowParams, next func(context.Context) error) error

	// ExecuteWorkflowTask wraps one workflow task executed on a worker. It runs
	// outside the deterministic replay: the task is already materialized from
	// history and the workflow code executes underneath it.
	//
	// Result is nil when the middleware is invoked and is populated by next.
	ExecuteWorkflowTask(ctx context.Context, params *WorkflowTaskParams, next func(context.Context) error) error

	// ExecuteActivity wraps one activity execution on a worker.
	//
	// Result is zero when the middleware is invoked and is populated by next.
	ExecuteActivity(ctx context.Context, params *ActivityTaskParams, next func(context.Context) error) error
}

// CreateWorkflowInstanceParams carries the inputs of a create call.
type CreateWorkflowInstanceParams struct {
	// Instance is the instance that will be created.
	Instance *core.WorkflowInstance

	// Queue is the queue the instance is created in.
	Queue core.Queue

	// Name is the registered workflow name.
	Name string

	// Inputs are the already-converted workflow arguments.
	Inputs []payload.Payload

	// Metadata is the initial workflow metadata, including anything injected by
	// the configured context propagators.
	Metadata *metadata.WorkflowMetadata
}

// SignalWorkflowParams carries the inputs of a signal call.
type SignalWorkflowParams struct {
	// InstanceID is the ID of the instance being signaled.
	InstanceID string

	// Name is the signal name.
	Name string

	// Arg is the already-converted signal argument.
	Arg payload.Payload
}

// WorkflowTaskParams carries a workflow task and its execution result.
type WorkflowTaskParams struct {
	// Task is the workflow task being executed.
	Task *backend.WorkflowTask

	// Result is populated by next. It is nil while the middleware runs before
	// next and is set after next returns successfully.
	Result *executor.ExecutionResult
}

// ActivityTaskParams carries an activity task and its execution result.
type ActivityTaskParams struct {
	// Task is the activity task being executed.
	Task *backend.ActivityTask

	// Result is populated by next. It is nil while the middleware runs before
	// next and is set after next returns successfully.
	Result payload.Payload
}

// MiddlewareBase is an embeddable no-op Middleware. Embed it in your own struct
// so that future additions to Middleware do not break your code.
//
//	type myMiddleware struct {
//		middleware.MiddlewareBase
//	}
type MiddlewareBase struct{}

var _ Middleware = (*MiddlewareBase)(nil)

// CreateWorkflowInstance implements Middleware.
func (MiddlewareBase) CreateWorkflowInstance(ctx context.Context, _ *CreateWorkflowInstanceParams, next func(context.Context) error) error {
	return next(ctx)
}

// CancelWorkflowInstance implements Middleware.
func (MiddlewareBase) CancelWorkflowInstance(ctx context.Context, _ *core.WorkflowInstance, next func(context.Context) error) error {
	return next(ctx)
}

// SignalWorkflow implements Middleware.
func (MiddlewareBase) SignalWorkflow(ctx context.Context, _ *SignalWorkflowParams, next func(context.Context) error) error {
	return next(ctx)
}

// ExecuteWorkflowTask implements Middleware.
func (MiddlewareBase) ExecuteWorkflowTask(ctx context.Context, _ *WorkflowTaskParams, next func(context.Context) error) error {
	return next(ctx)
}

// ExecuteActivity implements Middleware.
func (MiddlewareBase) ExecuteActivity(ctx context.Context, _ *ActivityTaskParams, next func(context.Context) error) error {
	return next(ctx)
}

// Middlewares is an ordered chain of Middleware. The first element is outermost:
// it is entered first and exits last. Nil entries are skipped.
//
// A nil or empty Middlewares is a no-op and simply calls next.
type Middlewares []Middleware

var _ Middleware = Middlewares(nil)

// CreateWorkflowInstance implements Middleware.
func (mws Middlewares) CreateWorkflowInstance(ctx context.Context, params *CreateWorkflowInstanceParams, next func(context.Context) error) error {
	mw, rest := nextNonNil(mws)
	if mw == nil {
		return next(ctx)
	}

	return mw.CreateWorkflowInstance(ctx, params, func(ctx context.Context) error {
		return rest.CreateWorkflowInstance(ctx, params, next)
	})
}

// CancelWorkflowInstance implements Middleware.
func (mws Middlewares) CancelWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance, next func(context.Context) error) error {
	mw, rest := nextNonNil(mws)
	if mw == nil {
		return next(ctx)
	}

	return mw.CancelWorkflowInstance(ctx, instance, func(ctx context.Context) error {
		return rest.CancelWorkflowInstance(ctx, instance, next)
	})
}

// SignalWorkflow implements Middleware.
func (mws Middlewares) SignalWorkflow(ctx context.Context, params *SignalWorkflowParams, next func(context.Context) error) error {
	mw, rest := nextNonNil(mws)
	if mw == nil {
		return next(ctx)
	}

	return mw.SignalWorkflow(ctx, params, func(ctx context.Context) error {
		return rest.SignalWorkflow(ctx, params, next)
	})
}

// ExecuteWorkflowTask implements Middleware.
func (mws Middlewares) ExecuteWorkflowTask(ctx context.Context, params *WorkflowTaskParams, next func(context.Context) error) error {
	mw, rest := nextNonNil(mws)
	if mw == nil {
		return next(ctx)
	}

	return mw.ExecuteWorkflowTask(ctx, params, func(ctx context.Context) error {
		return rest.ExecuteWorkflowTask(ctx, params, next)
	})
}

// ExecuteActivity implements Middleware.
func (mws Middlewares) ExecuteActivity(ctx context.Context, params *ActivityTaskParams, next func(context.Context) error) error {
	mw, rest := nextNonNil(mws)
	if mw == nil {
		return next(ctx)
	}

	return mw.ExecuteActivity(ctx, params, func(ctx context.Context) error {
		return rest.ExecuteActivity(ctx, params, next)
	})
}

// nextNonNil returns the first non-nil middleware and everything after it.
func nextNonNil(mws Middlewares) (Middleware, Middlewares) {
	for i, mw := range mws {
		if mw != nil {
			return mw, mws[i+1:]
		}
	}

	return nil, nil
}
