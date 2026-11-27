package middleware

import (
	"context"

	"github.com/denisaditya0/go-workflows/backend/history"
	"github.com/denisaditya0/go-workflows/core"
)

// Hook observes engine activity. Hooks are pure callbacks: they cannot change
// control flow, veto an operation, or alter the events a workflow produces.
// Use Middleware when you need to wrap an operation instead.
//
// Hooks run outside the deterministic workflow execution. They are invoked for
// events only after they have been committed to the backend, never while
// replaying history, so an event is delivered exactly once per append from the
// perspective of one hook set.
//
// A hook must be fast and must not panic. Slow hooks delay the worker that
// invoked them; run expensive work asynchronously.
type Hook interface {
	// WorkflowEvent is called for each event newly appended to a workflow
	// instance's history.
	//
	// It is not called during replay.
	WorkflowEvent(ctx context.Context, ev *Event)
}

// Event describes a workflow history event observed by a Hook.
type Event struct {
	// Instance is the workflow instance the event belongs to. It is never nil.
	Instance *core.WorkflowInstance

	// Queue is the queue the workflow instance is running on.
	Queue core.Queue

	// WorkflowName is the registered workflow name.
	WorkflowName string

	// History is the committed event. It is never nil.
	History *history.Event
}

// HookBase is an embeddable no-op Hook. Embed it in your own struct so that
// future additions to Hook do not break your code.
//
//	type myHooks struct {
//		middleware.HookBase
//	}
//
//	func (h *myHooks) WorkflowEvent(ctx context.Context, ev *middleware.Event) {
//		// ...
//	}
type HookBase struct{}

var _ Hook = (*HookBase)(nil)

// WorkflowEvent implements Hook.
func (HookBase) WorkflowEvent(context.Context, *Event) {}

// HookFunc adapts a plain function to Hook. A nil HookFunc is valid and does
// nothing.
type HookFunc func(ctx context.Context, ev *Event)

var _ Hook = HookFunc(nil)

// WorkflowEvent implements Hook.
func (f HookFunc) WorkflowEvent(ctx context.Context, ev *Event) {
	if f != nil {
		f(ctx, ev)
	}
}

// Hooks is an ordered collection of Hook. Every hook is called in order. Nil
// entries are skipped.
//
// A nil or empty Hooks is a valid no-op.
type Hooks []Hook

var _ Hook = Hooks(nil)

// WorkflowEvent implements Hook.
func (hs Hooks) WorkflowEvent(ctx context.Context, ev *Event) {
	for _, h := range hs {
		if h != nil {
			h.WorkflowEvent(ctx, ev)
		}
	}
}
