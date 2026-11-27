package middleware_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/denisaditya0/go-workflows/backend/history"
	"github.com/denisaditya0/go-workflows/middleware"
)

type traceHook struct {
	middleware.HookBase

	name  string
	trace *[]string
}

func (h *traceHook) WorkflowEvent(_ context.Context, ev *middleware.Event) {
	*h.trace = append(*h.trace, h.name+":"+ev.History.Type.String())
}

func TestHooks_Order(t *testing.T) {
	var trace []string

	hooks := middleware.Hooks{
		&traceHook{name: "a", trace: &trace},
		&traceHook{name: "b", trace: &trace},
	}

	ev := &middleware.Event{
		History: history.NewPendingEvent(time.Now(), history.EventType_ActivityScheduled, &history.ActivityScheduledAttributes{}),
	}

	hooks.WorkflowEvent(context.Background(), ev)

	want := []string{"a:ActivityScheduled", "b:ActivityScheduled"}
	if !slices.Equal(trace, want) {
		t.Fatalf("trace = %v, want %v", trace, want)
	}
}

func TestHooks_SkipsNil(t *testing.T) {
	var trace []string

	hooks := middleware.Hooks{
		nil,
		&traceHook{name: "a", trace: &trace},
		nil,
	}

	hooks.WorkflowEvent(context.Background(), &middleware.Event{
		History: history.NewPendingEvent(time.Now(), history.EventType_SignalReceived, &history.SignalReceivedAttributes{}),
	})

	want := []string{"a:SignalReceived"}
	if !slices.Equal(trace, want) {
		t.Fatalf("trace = %v, want %v", trace, want)
	}
}

func TestHooks_EmptyIsNoop(t *testing.T) {
	var nilHooks middleware.Hooks
	nilHooks.WorkflowEvent(context.Background(), &middleware.Event{})
}

func TestHookFunc(t *testing.T) {
	var got string

	var hook middleware.Hooks = middleware.Hooks{
		middleware.HookFunc(func(_ context.Context, ev *middleware.Event) {
			got = ev.History.Type.String()
		}),
		nil, // a nil HookFunc must be safe
	}

	hook.WorkflowEvent(context.Background(), &middleware.Event{
		History: history.NewPendingEvent(time.Now(), history.EventType_TimerFired, &history.TimerFiredAttributes{}),
	})

	if got != "TimerFired" {
		t.Fatalf("got = %q, want %q", got, "TimerFired")
	}
}

func TestHookBase_IsNoop(t *testing.T) {
	var hook middleware.HookBase
	hook.WorkflowEvent(context.Background(), &middleware.Event{})
}
