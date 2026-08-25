package interceptor

import (
	"context"
)

// Base is a no-op interceptor that passes through to next.
// Embed this in your interceptor structs to only override the methods you need.
//
//	type MyInterceptor struct {
//	    interceptor.Base
//	}
//
//	func (m *MyInterceptor) ExecuteWorkflow(ctx interceptor.WorkflowContext, info *interceptor.WorkflowInfo, next interceptor.WorkflowHandler) error {
//	    // custom logic
//	    return next(ctx)
//	}
type Base struct{}

func (Base) ExecuteWorkflow(ctx WorkflowContext, _ *WorkflowInfo, next WorkflowHandler) error {
	return next(ctx)
}

func (Base) ExecuteActivity(ctx context.Context, _ *ActivityInfo, next ActivityHandler) error {
	return next(ctx)
}

// Verify Base implements Interceptor at compile time.
var _ Interceptor = Base{}
