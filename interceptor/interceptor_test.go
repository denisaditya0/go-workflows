package interceptor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/interceptor"
	"github.com/stretchr/testify/require"
)

// --- Compile-time interface checks ---

func TestAdapters_SatisfyInterface(t *testing.T) {
	var _ interceptor.WorkflowInterceptor = interceptor.WorkflowInterceptorFunc(nil)
	var _ interceptor.ActivityInterceptor = interceptor.ActivityInterceptorFunc(nil)
	var _ interceptor.Interceptor = interceptor.Base{}
	var _ interceptor.Interceptor = (*interceptor.LoggingInterceptor)(nil)
}

// --- Workflow Interceptor Chain Tests ---

// buildWorkflowChain constructs the interceptor chain as the executor does.
func buildWorkflowChain(interceptors []interceptor.WorkflowInterceptor, info *interceptor.WorkflowInfo, inner interceptor.WorkflowHandler) interceptor.WorkflowHandler {
	handler := inner
	for i := len(interceptors) - 1; i >= 0; i-- {
		ic := interceptors[i]
		next := handler
		handler = func(ctx interceptor.WorkflowContext) error {
			return ic.ExecuteWorkflow(ctx, info, next)
		}
	}
	return handler
}

// buildActivityChain constructs the interceptor chain as the activity executor does.
func buildActivityChain(interceptors []interceptor.ActivityInterceptor, info *interceptor.ActivityInfo, inner interceptor.ActivityHandler) interceptor.ActivityHandler {
	handler := inner
	for i := len(interceptors) - 1; i >= 0; i-- {
		ic := interceptors[i]
		next := handler
		handler = func(ctx context.Context) error {
			return ic.ExecuteActivity(ctx, info, next)
		}
	}
	return handler
}

func TestWorkflowChain_ExecutionOrder(t *testing.T) {
	var order []string

	a := interceptor.WorkflowInterceptorFunc(func(ctx interceptor.WorkflowContext, info *interceptor.WorkflowInfo, next interceptor.WorkflowHandler) error {
		order = append(order, "A.before")
		err := next(ctx)
		order = append(order, "A.after")
		return err
	})

	b := interceptor.WorkflowInterceptorFunc(func(ctx interceptor.WorkflowContext, info *interceptor.WorkflowInfo, next interceptor.WorkflowHandler) error {
		order = append(order, "B.before")
		err := next(ctx)
		order = append(order, "B.after")
		return err
	})

	c := interceptor.WorkflowInterceptorFunc(func(ctx interceptor.WorkflowContext, info *interceptor.WorkflowInfo, next interceptor.WorkflowHandler) error {
		order = append(order, "C.before")
		err := next(ctx)
		order = append(order, "C.after")
		return err
	})

	info := &interceptor.WorkflowInfo{Name: "test", Instance: core.NewWorkflowInstance("id", "exec")}
	interceptors := []interceptor.WorkflowInterceptor{a, b, c}

	inner := func(ctx interceptor.WorkflowContext) error {
		order = append(order, "workflow")
		return nil
	}

	chain := buildWorkflowChain(interceptors, info, inner)
	err := chain(nil)

	require.NoError(t, err)
	require.Equal(t, []string{
		"A.before", "B.before", "C.before",
		"workflow",
		"C.after", "B.after", "A.after",
	}, order)
}

func TestWorkflowChain_ShortCircuit(t *testing.T) {
	workflowCalled := false
	shortCircuitErr := errors.New("rejected")

	blocker := interceptor.WorkflowInterceptorFunc(func(ctx interceptor.WorkflowContext, info *interceptor.WorkflowInfo, next interceptor.WorkflowHandler) error {
		// Don't call next — short-circuit
		return shortCircuitErr
	})

	info := &interceptor.WorkflowInfo{Name: "test", Instance: core.NewWorkflowInstance("id", "exec")}
	interceptors := []interceptor.WorkflowInterceptor{blocker}

	inner := func(ctx interceptor.WorkflowContext) error {
		workflowCalled = true
		return nil
	}

	chain := buildWorkflowChain(interceptors, info, inner)
	err := chain(nil)

	require.ErrorIs(t, err, shortCircuitErr)
	require.False(t, workflowCalled)
}

func TestWorkflowChain_ErrorPropagation(t *testing.T) {
	workflowErr := errors.New("workflow failed")
	var interceptorSawError error

	observer := interceptor.WorkflowInterceptorFunc(func(ctx interceptor.WorkflowContext, info *interceptor.WorkflowInfo, next interceptor.WorkflowHandler) error {
		err := next(ctx)
		interceptorSawError = err
		return err
	})

	info := &interceptor.WorkflowInfo{Name: "test", Instance: core.NewWorkflowInstance("id", "exec")}
	interceptors := []interceptor.WorkflowInterceptor{observer}

	inner := func(ctx interceptor.WorkflowContext) error {
		return workflowErr
	}

	chain := buildWorkflowChain(interceptors, info, inner)
	err := chain(nil)

	require.ErrorIs(t, err, workflowErr)
	require.ErrorIs(t, interceptorSawError, workflowErr)
}

func TestWorkflowChain_NoInterceptors(t *testing.T) {
	workflowCalled := false

	info := &interceptor.WorkflowInfo{Name: "test", Instance: core.NewWorkflowInstance("id", "exec")}
	var interceptors []interceptor.WorkflowInterceptor

	inner := func(ctx interceptor.WorkflowContext) error {
		workflowCalled = true
		return nil
	}

	chain := buildWorkflowChain(interceptors, info, inner)
	err := chain(nil)

	require.NoError(t, err)
	require.True(t, workflowCalled)
}

func TestWorkflowChain_InfoPassedCorrectly(t *testing.T) {
	var receivedInfo *interceptor.WorkflowInfo

	observer := interceptor.WorkflowInterceptorFunc(func(ctx interceptor.WorkflowContext, info *interceptor.WorkflowInfo, next interceptor.WorkflowHandler) error {
		receivedInfo = info
		return next(ctx)
	})

	instance := core.NewWorkflowInstance("my-instance", "my-execution")
	info := &interceptor.WorkflowInfo{Name: "MyWorkflow", Instance: instance}
	interceptors := []interceptor.WorkflowInterceptor{observer}

	inner := func(ctx interceptor.WorkflowContext) error { return nil }

	chain := buildWorkflowChain(interceptors, info, inner)
	_ = chain(nil)

	require.NotNil(t, receivedInfo)
	require.Equal(t, "MyWorkflow", receivedInfo.Name)
	require.Equal(t, "my-instance", receivedInfo.Instance.InstanceID)
}

// --- Activity Interceptor Chain Tests ---

func TestActivityChain_ExecutionOrder(t *testing.T) {
	var order []string

	a := interceptor.ActivityInterceptorFunc(func(ctx context.Context, info *interceptor.ActivityInfo, next interceptor.ActivityHandler) error {
		order = append(order, "A.before")
		err := next(ctx)
		order = append(order, "A.after")
		return err
	})

	b := interceptor.ActivityInterceptorFunc(func(ctx context.Context, info *interceptor.ActivityInfo, next interceptor.ActivityHandler) error {
		order = append(order, "B.before")
		err := next(ctx)
		order = append(order, "B.after")
		return err
	})

	info := &interceptor.ActivityInfo{Name: "MyActivity", Attempt: 1}
	interceptors := []interceptor.ActivityInterceptor{a, b}

	inner := func(ctx context.Context) error {
		order = append(order, "activity")
		return nil
	}

	chain := buildActivityChain(interceptors, info, inner)
	err := chain(context.Background())

	require.NoError(t, err)
	require.Equal(t, []string{
		"A.before", "B.before",
		"activity",
		"B.after", "A.after",
	}, order)
}

func TestActivityChain_ShortCircuit(t *testing.T) {
	activityCalled := false
	shortCircuitErr := errors.New("rate limited")

	blocker := interceptor.ActivityInterceptorFunc(func(ctx context.Context, info *interceptor.ActivityInfo, next interceptor.ActivityHandler) error {
		return shortCircuitErr
	})

	info := &interceptor.ActivityInfo{Name: "MyActivity", Attempt: 1}
	interceptors := []interceptor.ActivityInterceptor{blocker}

	inner := func(ctx context.Context) error {
		activityCalled = true
		return nil
	}

	chain := buildActivityChain(interceptors, info, inner)
	err := chain(context.Background())

	require.ErrorIs(t, err, shortCircuitErr)
	require.False(t, activityCalled)
}

func TestActivityChain_ErrorPropagation(t *testing.T) {
	activityErr := errors.New("connection refused")
	var interceptorSawError error

	observer := interceptor.ActivityInterceptorFunc(func(ctx context.Context, info *interceptor.ActivityInfo, next interceptor.ActivityHandler) error {
		err := next(ctx)
		interceptorSawError = err
		return err
	})

	info := &interceptor.ActivityInfo{Name: "MyActivity", Attempt: 2}
	interceptors := []interceptor.ActivityInterceptor{observer}

	inner := func(ctx context.Context) error {
		return activityErr
	}

	chain := buildActivityChain(interceptors, info, inner)
	err := chain(context.Background())

	require.ErrorIs(t, err, activityErr)
	require.ErrorIs(t, interceptorSawError, activityErr)
}

func TestActivityChain_InfoPassedCorrectly(t *testing.T) {
	var receivedInfo *interceptor.ActivityInfo

	observer := interceptor.ActivityInterceptorFunc(func(ctx context.Context, info *interceptor.ActivityInfo, next interceptor.ActivityHandler) error {
		receivedInfo = info
		return next(ctx)
	})

	info := &interceptor.ActivityInfo{Name: "DoWork", Attempt: 3}
	interceptors := []interceptor.ActivityInterceptor{observer}

	inner := func(ctx context.Context) error { return nil }

	chain := buildActivityChain(interceptors, info, inner)
	_ = chain(context.Background())

	require.NotNil(t, receivedInfo)
	require.Equal(t, "DoWork", receivedInfo.Name)
	require.Equal(t, 3, receivedInfo.Attempt)
}

// --- Base Interceptor Tests ---

func TestBase_WorkflowPassthrough(t *testing.T) {
	base := interceptor.Base{}
	called := false

	info := &interceptor.WorkflowInfo{Name: "test", Instance: core.NewWorkflowInstance("id", "exec")}

	err := base.ExecuteWorkflow(nil, info, func(ctx interceptor.WorkflowContext) error {
		called = true
		return nil
	})

	require.NoError(t, err)
	require.True(t, called)
}

func TestBase_ActivityPassthrough(t *testing.T) {
	base := interceptor.Base{}
	called := false

	info := &interceptor.ActivityInfo{Name: "test", Attempt: 1}

	err := base.ExecuteActivity(context.Background(), info, func(ctx context.Context) error {
		called = true
		return nil
	})

	require.NoError(t, err)
	require.True(t, called)
}

func TestBase_WorkflowErrorPassthrough(t *testing.T) {
	base := interceptor.Base{}
	expectedErr := errors.New("some error")

	info := &interceptor.WorkflowInfo{Name: "test", Instance: core.NewWorkflowInstance("id", "exec")}

	err := base.ExecuteWorkflow(nil, info, func(ctx interceptor.WorkflowContext) error {
		return expectedErr
	})

	require.ErrorIs(t, err, expectedErr)
}

// --- Context modification test ---

type ctxKey struct{}

func TestActivityChain_ContextModification(t *testing.T) {
	// Interceptor that adds a value to context
	modifier := interceptor.ActivityInterceptorFunc(func(ctx context.Context, info *interceptor.ActivityInfo, next interceptor.ActivityHandler) error {
		ctx = context.WithValue(ctx, ctxKey{}, "injected-value")
		return next(ctx)
	})

	var receivedValue string

	info := &interceptor.ActivityInfo{Name: "test", Attempt: 1}
	interceptors := []interceptor.ActivityInterceptor{modifier}

	inner := func(ctx context.Context) error {
		if v, ok := ctx.Value(ctxKey{}).(string); ok {
			receivedValue = v
		}
		return nil
	}

	chain := buildActivityChain(interceptors, info, inner)
	_ = chain(context.Background())

	require.Equal(t, "injected-value", receivedValue)
}
