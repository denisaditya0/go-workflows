package interceptor

import (
	"context"
)

// WorkflowInterceptorFunc adapts a function to the WorkflowInterceptor interface.
type WorkflowInterceptorFunc func(ctx WorkflowContext, info *WorkflowInfo, next WorkflowHandler) error

func (f WorkflowInterceptorFunc) ExecuteWorkflow(ctx WorkflowContext, info *WorkflowInfo, next WorkflowHandler) error {
	return f(ctx, info, next)
}

// ActivityInterceptorFunc adapts a function to the ActivityInterceptor interface.
type ActivityInterceptorFunc func(ctx context.Context, info *ActivityInfo, next ActivityHandler) error

func (f ActivityInterceptorFunc) ExecuteActivity(ctx context.Context, info *ActivityInfo, next ActivityHandler) error {
	return f(ctx, info, next)
}
