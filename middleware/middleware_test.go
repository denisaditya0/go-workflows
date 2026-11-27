package middleware_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/denisaditya0/go-workflows/backend"
	"github.com/denisaditya0/go-workflows/backend/payload"
	"github.com/denisaditya0/go-workflows/middleware"
)

type traceMiddleware struct {
	middleware.MiddlewareBase

	name  string
	trace *[]string
}

func (m *traceMiddleware) ExecuteActivity(ctx context.Context, _ *middleware.ActivityTaskParams, next func(context.Context) error) error {
	*m.trace = append(*m.trace, m.name+":before")
	err := next(ctx)
	*m.trace = append(*m.trace, m.name+":after")
	return err
}

func TestMiddlewares_Order(t *testing.T) {
	var trace []string

	chain := middleware.Middlewares{
		&traceMiddleware{name: "a", trace: &trace},
		&traceMiddleware{name: "b", trace: &trace},
	}

	err := chain.ExecuteActivity(context.Background(), &middleware.ActivityTaskParams{Task: &backend.ActivityTask{}}, func(context.Context) error {
		trace = append(trace, "inner")
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"a:before", "b:before", "inner", "b:after", "a:after"}
	if !slices.Equal(trace, want) {
		t.Fatalf("order = %v, want %v", trace, want)
	}
}

func TestMiddlewares_SkipsNil(t *testing.T) {
	var trace []string

	chain := middleware.Middlewares{
		nil,
		&traceMiddleware{name: "a", trace: &trace},
		nil,
	}

	if err := chain.ExecuteActivity(context.Background(), &middleware.ActivityTaskParams{}, func(context.Context) error {
		trace = append(trace, "inner")
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"a:before", "inner", "a:after"}
	if !slices.Equal(trace, want) {
		t.Fatalf("order = %v, want %v", trace, want)
	}
}

func TestMiddlewares_EmptyIsNoop(t *testing.T) {
	var nilChain middleware.Middlewares

	called := false
	if err := nilChain.ExecuteWorkflowTask(context.Background(), &middleware.WorkflowTaskParams{}, func(context.Context) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !called {
		t.Fatal("next was not called")
	}
}

var errBlocked = errors.New("blocked")

// shortCircuitMiddleware returns an error without calling next.
type shortCircuitMiddleware struct {
	middleware.MiddlewareBase
}

func (shortCircuitMiddleware) ExecuteActivity(context.Context, *middleware.ActivityTaskParams, func(context.Context) error) error {
	return errBlocked
}

func TestMiddlewares_ShortCircuit(t *testing.T) {
	chain := middleware.Middlewares{shortCircuitMiddleware{}}

	called := false
	err := chain.ExecuteActivity(context.Background(), &middleware.ActivityTaskParams{}, func(context.Context) error {
		called = true
		return nil
	})

	if !errors.Is(err, errBlocked) {
		t.Fatalf("error = %v, want %v", err, errBlocked)
	}

	if called {
		t.Fatal("next should not be called")
	}
}

// resultMiddleware records the result it observes after next returns.
type resultMiddleware struct {
	middleware.MiddlewareBase
	seen *string
}

func (m *resultMiddleware) ExecuteActivity(ctx context.Context, params *middleware.ActivityTaskParams, next func(context.Context) error) error {
	if err := next(ctx); err != nil {
		return err
	}

	*m.seen = string(params.Result)
	return nil
}

func TestMiddlewares_ResultVisibleAfterNext(t *testing.T) {
	var seen string

	params := &middleware.ActivityTaskParams{}
	chain := middleware.Middlewares{&resultMiddleware{seen: &seen}}

	err := chain.ExecuteActivity(context.Background(), params, func(context.Context) error {
		params.Result = payload.Payload("result")
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if seen != "result" {
		t.Fatalf("result = %q, want %q", seen, "result")
	}
}
