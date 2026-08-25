package activity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/converter"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/backend/payload"
	"github.com/cschleiden/go-workflows/interceptor"
	"github.com/cschleiden/go-workflows/internal/args"
	"github.com/cschleiden/go-workflows/internal/log"
	"github.com/cschleiden/go-workflows/internal/tracing"
	"github.com/cschleiden/go-workflows/internal/workflowerrors"
	"github.com/cschleiden/go-workflows/registry"
	wf "github.com/cschleiden/go-workflows/workflow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Executor struct {
	logger       *slog.Logger
	tracer       trace.Tracer
	converter    converter.Converter
	propagators  []wf.ContextPropagator
	r            *registry.Registry
	interceptors []interceptor.ActivityInterceptor
}

func NewExecutor(
	logger *slog.Logger,
	tracer trace.Tracer,
	converter converter.Converter,
	propagators []wf.ContextPropagator,
	r *registry.Registry,
	interceptors []interceptor.Interceptor,
) *Executor {
	// Extract activity interceptors from the combined interceptor list
	var actInterceptors []interceptor.ActivityInterceptor
	for _, i := range interceptors {
		actInterceptors = append(actInterceptors, i)
	}

	return &Executor{
		logger:       logger,
		tracer:       tracer,
		converter:    converter,
		propagators:  propagators,
		r:            r,
		interceptors: actInterceptors,
	}
}

func (e *Executor) ExecuteActivity(ctx context.Context, task *backend.ActivityTask) (payload.Payload, error) {
	a := task.Event.Attributes.(*history.ActivityScheduledAttributes)

	// Add activity state to context
	as := NewActivityState(
		task.Event.ID,
		a.Attempt,
		task.WorkflowInstance,
		e.logger)
	activityCtx := WithActivityState(ctx, as)

	for _, propagator := range e.propagators {
		var err error
		activityCtx, err = propagator.Extract(activityCtx, a.Metadata)
		if err != nil {
			return nil, workflowerrors.NewPermanentError(fmt.Errorf("extracting context from propagator: %w", err))
		}
	}

	activityCtx, span := e.tracer.Start(activityCtx, fmt.Sprintf("ActivityTaskExecution: %s", a.Name), trace.WithAttributes(
		attribute.String(log.ActivityNameKey, a.Name),
		attribute.String(log.InstanceIDKey, task.WorkflowInstance.InstanceID),
		attribute.String(log.ActivityIDKey, task.ID),
		attribute.Int(log.AttemptKey, a.Attempt),
	))
	defer span.End()

	activity, err := e.r.GetActivity(a.Name)
	if err != nil {
		return nil, workflowerrors.NewPermanentError(tracing.WithSpanError(span, fmt.Errorf("activity not found: %w", err)))
	}

	activityFn := reflect.ValueOf(activity)
	if activityFn.Type().Kind() != reflect.Func {
		return nil, workflowerrors.NewPermanentError(tracing.WithSpanError(span, errors.New("activity not a function")))
	}

	args, addContext, err := args.InputsToArgs(e.converter, activityFn, a.Inputs)
	if err != nil {
		return nil, workflowerrors.NewPermanentError(tracing.WithSpanError(span, fmt.Errorf("converting activity inputs: %w", err)))
	}

	// Execute activity
	if addContext {
		args[0] = reflect.ValueOf(activityCtx)
	}

	info := &interceptor.ActivityInfo{
		Name:    a.Name,
		Attempt: a.Attempt,
	}

	var result payload.Payload

	// Build the innermost handler that calls the actual activity function
	inner := func(ctx context.Context) error {
		// Update args[0] with the potentially-modified context from interceptors
		if addContext {
			args[0] = reflect.ValueOf(ctx)
		}

		done := make(chan struct{})
		var rv []reflect.Value

		go func() {
			// Recover any panic encountered during activity execution
			defer func() {
				if r := recover(); r != nil {
					err := workflowerrors.NewPanicError(fmt.Sprintf("panic: %v", r))
					rv = []reflect.Value{reflect.ValueOf(err)}
				}

				close(done)
			}()

			rv = activityFn.Call(args)
		}()

		<-done

		if len(rv) < 1 || len(rv) > 2 {
			return workflowerrors.NewPermanentError(
				tracing.WithSpanError(span, errors.New("activity has to return either (error) or (<result>, error)")))
		}

		// Convert activity result to payload. We always expect at least an error
		if len(rv) > 1 {
			var err error
			result, err = e.converter.To(rv[0].Interface())
			if err != nil {
				return workflowerrors.NewPermanentError(tracing.WithSpanError(span, fmt.Errorf("converting activity result: %w", err)))
			}
		}

		// Was an error returned?
		errResult := rv[len(rv)-1]
		if errResult.IsNil() {
			// No error from activity execution
			return nil
		}

		actErr, ok := errResult.Interface().(error)
		if !ok {
			return workflowerrors.NewPermanentError(
				tracing.WithSpanError(span, fmt.Errorf("activity error result does not satisfy error interface (%T): %v", errResult, errResult)))
		}

		return workflowerrors.FromError(tracing.WithSpanError(span, actErr))
	}

	// Build the interceptor chain: interceptor[0] → ... → interceptor[n] → inner
	// Combine global interceptors with per-activity interceptors
	allInterceptors := e.interceptors
	if perAct := e.r.GetActivityInterceptors(a.Name); len(perAct) > 0 {
		allInterceptors = make([]interceptor.ActivityInterceptor, 0, len(e.interceptors)+len(perAct))
		allInterceptors = append(allInterceptors, e.interceptors...)
		allInterceptors = append(allInterceptors, perAct...)
	}

	handler := inner
	for i := len(allInterceptors) - 1; i >= 0; i-- {
		ic := allInterceptors[i]
		next := handler
		handler = func(ctx context.Context) error {
			return ic.ExecuteActivity(ctx, info, next)
		}
	}

	// Execute the chain
	if err := handler(activityCtx); err != nil {
		return result, err
	}

	return result, nil
}
