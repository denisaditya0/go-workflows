package executor

import (
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"

	"github.com/cschleiden/go-workflows/backend/payload"
	"github.com/cschleiden/go-workflows/interceptor"
	"github.com/cschleiden/go-workflows/internal/args"
	"github.com/cschleiden/go-workflows/internal/contextvalue"
	"github.com/cschleiden/go-workflows/internal/sync"
	"github.com/cschleiden/go-workflows/internal/workflowerrors"
)

type workflow struct {
	s            *sync.Scheduler
	fn           reflect.Value
	result       payload.Payload
	err          error
	interceptors []interceptor.WorkflowInterceptor
}

func newWorkflow(workflowFn reflect.Value, interceptors []interceptor.WorkflowInterceptor) *workflow {
	s := sync.NewScheduler()

	return &workflow{
		s:            s,
		fn:           workflowFn,
		interceptors: interceptors,
	}
}

func (w *workflow) Execute(ctx sync.Context, inputs []payload.Payload, info *interceptor.WorkflowInfo) error {
	w.s.NewCoroutine(ctx, func(ctx sync.Context) error {
		converter := contextvalue.Converter(ctx)
		args, addContext, err := args.InputsToArgs(converter, w.fn, inputs)
		if err != nil {
			return fmt.Errorf("converting workflow inputs: %w", err)
		}

		if !addContext {
			return errors.New("workflow must accept context as first argument")
		}

		args[0] = reflect.ValueOf(ctx)

		// Handle panics in workflows
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())

				w.err = workflowerrors.NewPanicError(fmt.Sprintf("panic in workflow: %v\n%v", r, stack))
			}
		}()

		// Build the innermost handler that calls the actual workflow function
		inner := func(ctx sync.Context) error {
			// Update args[0] with the potentially-modified context from interceptors
			args[0] = reflect.ValueOf(ctx)

			r := w.fn.Call(args)

			// Process result
			if len(r) < 1 || len(r) > 2 {
				return errors.New("workflow has to return either (error) or (result, error)")
			}

			var result payload.Payload

			if len(r) > 1 {
				var err error
				result, err = converter.To(r[0].Interface())
				if err != nil {
					return fmt.Errorf("converting workflow result: %w", err)
				}
			} else {
				result, err = converter.To(nil)
				if err != nil {
					return fmt.Errorf("converting workflow result: %w", err)
				}
			}

			w.result = result

			errResult := r[len(r)-1]
			if !errResult.IsNil() {
				errInterface, ok := errResult.Interface().(error)
				if !ok {
					return fmt.Errorf("workflow error result does not satisfy error interface (%T): %v", errResult, errResult)
				}

				w.err = errInterface
			}

			return nil
		}

		// Build the interceptor chain: interceptor[0] → ... → interceptor[n] → inner
		handler := inner
		for i := len(w.interceptors) - 1; i >= 0; i-- {
			ic := w.interceptors[i]
			next := handler
			handler = func(ctx sync.Context) error {
				return ic.ExecuteWorkflow(ctx, info, next)
			}
		}

		// Execute the chain with the workflow context
		if chainErr := handler(ctx); chainErr != nil {
			// If the interceptor chain itself returns an error (not the workflow),
			// and no workflow error was already set, use the chain error.
			if w.err == nil {
				w.err = chainErr
			}
		}

		return nil
	})

	return w.s.Execute()
}

func (w *workflow) Continue() error {
	return w.s.Execute()
}

func (w *workflow) Completed() bool {
	return w.s.RunningCoroutines() == 0
}

// Result returns the return value of a finished workflow as a payload
func (w *workflow) Result() payload.Payload {
	return w.result
}

// Error returns the error of a finished workflow, can be nil
func (w *workflow) Error() error {
	return w.err
}

func (w *workflow) Close() {
	// End coroutine execution to prevent goroutine leaks
	w.s.Exit()
}
