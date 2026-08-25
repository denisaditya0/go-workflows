package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cschleiden/go-workflows/interceptor"
)

// DurationInterceptor tracks execution duration of workflows and activities.
// This demonstrates how to write a custom interceptor.
type DurationInterceptor struct {
	interceptor.Base
}

func (d *DurationInterceptor) ExecuteWorkflow(ctx interceptor.WorkflowContext, info *interceptor.WorkflowInfo, next interceptor.WorkflowHandler) error {
	start := time.Now()

	err := next(ctx)

	duration := time.Since(start)
	status := "OK"
	if err != nil {
		status = "FAILED"
	}

	fmt.Printf("  [duration] workflow=%s instance=%s duration=%v status=%s\n",
		info.Name, info.Instance.InstanceID, duration, status)

	return err
}

func (d *DurationInterceptor) ExecuteActivity(ctx context.Context, info *interceptor.ActivityInfo, next interceptor.ActivityHandler) error {
	start := time.Now()

	err := next(ctx)

	duration := time.Since(start)
	status := "OK"
	if err != nil {
		status = "FAILED"
	}

	fmt.Printf("  [duration] activity=%s attempt=%d duration=%v status=%s\n",
		info.Name, info.Attempt, duration, status)

	return err
}
