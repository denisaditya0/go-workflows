package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cschleiden/go-workflows/workflow"
)

// ---- Activities (ordinary Go; capture a fake HTTP trace like the real worker) ----

type StepInput struct {
	Name string `json:"name"`
}

// CallService simulates an activity that makes an HTTP call and records a trace,
// then returns a Result[T] carrying that trace (so it lands in the result payload).
func CallService(ctx context.Context, in StepInput) (Result[string], error) {
	ctx = withTrace(ctx)

	// Simulate two upstream calls being captured into the trace.
	appendTrace(ctx, requestTrace{Method: "GET", URL: "https://api.example/" + in.Name, Status: 200, RespBody: `{"ok":true}`, Duration: "42ms"})
	appendTrace(ctx, requestTrace{Method: "POST", URL: "https://api.example/" + in.Name + "/commit", Status: 201, RespBody: `{"id":123}`, Duration: "17ms"})

	return actDone("called "+in.Name, "value-for-"+in.Name, ctx)
}

// FlakyActivity always fails permanently — drives the Failed path.
func FlakyActivity(ctx context.Context) (Result[string], error) {
	return Result[string]{Status: StatusFailed, Message: "upstream rejected", Error: "boom: upstream 500"}, errors.New("boom: upstream 500")
}

// ---- Workflows ----

// RootWorkflow is the parent: it runs two sub-workflows (a tree) and an activity.
func RootWorkflow(ctx workflow.Context, source string) (Result[string], error) {
	// Carry the source so sub-workflows inherit it via SourcePropagator.
	ctx = withSourceWf(ctx, source)

	// Fan out to two child sub-workflows.
	f1 := workflow.CreateSubWorkflowInstance[Result[string]](ctx, workflow.DefaultSubWorkflowOptions, ChildWorkflow, "child-A")
	f2 := workflow.CreateSubWorkflowInstance[Result[string]](ctx, workflow.DefaultSubWorkflowOptions, ChildWorkflow, "child-B")

	if _, err := f1.Get(ctx); err != nil {
		return Result[string]{Status: StatusFailed, Message: "child-A failed"}, err
	}
	if _, err := f2.Get(ctx); err != nil {
		return Result[string]{Status: StatusFailed, Message: "child-B failed"}, err
	}

	// One direct activity at the root too.
	r, err := workflow.ExecuteActivity[Result[string]](ctx, workflow.DefaultActivityOptions, CallService, StepInput{Name: "root-step"}).Get(ctx)
	if err != nil {
		return Result[string]{Status: StatusFailed, Message: "root step failed"}, err
	}

	return Result[string]{Status: StatusDone, Message: "root complete", Context: r.Message}, nil
}

// ChildWorkflow is a sub-workflow that runs one trace-capturing activity.
func ChildWorkflow(ctx workflow.Context, name string) (Result[string], error) {
	r, err := workflow.ExecuteActivity[Result[string]](ctx, workflow.DefaultActivityOptions, CallService, StepInput{Name: name}).Get(ctx)
	if err != nil {
		return Result[string]{Status: StatusFailed, Message: name + " failed"}, err
	}
	return Result[string]{Status: StatusDone, Message: name + " done", Context: r.Context}, nil
}

// FailingWorkflow finishes with an error — exercises the Failed badge + promoted
// error column (#469).
func FailingWorkflow(ctx workflow.Context) (Result[string], error) {
	opts := workflow.DefaultActivityOptions
	opts.RetryOptions = workflow.RetryOptions{MaxAttempts: 1} // no retries, fail fast
	_, err := workflow.ExecuteActivity[Result[string]](ctx, opts, FlakyActivity).Get(ctx)
	if err != nil {
		return Result[string]{}, fmt.Errorf("workflow failed: %w", err)
	}
	return Result[string]{Status: StatusDone}, nil
}

// LoopWorkflow continues-as-new a few times, then finishes — exercises the
// previous_execution_id lineage column.
func LoopWorkflow(ctx workflow.Context, iteration int) (Result[string], error) {
	if err := workflow.Sleep(ctx, 100*time.Millisecond); err != nil {
		return Result[string]{}, err
	}
	if iteration >= 3 {
		return Result[string]{Status: StatusDone, Message: fmt.Sprintf("looped %d times", iteration)}, nil
	}
	return Result[string]{}, workflow.ContinueAsNew(ctx, iteration+1)
}
