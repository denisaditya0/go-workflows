// Package main is a standalone "dummy worker" that mirrors the shape of the real
// apimicro-testing-worker so the new (templ) diagnostics UI can be exercised
// against realistic data WITHOUT touching the production worker.
//
// It reproduces the parts that make diag interesting:
//   - a universal Result[T] return type with an embedded `trace` (like the real
//     worker's schema.Result), so activity/workflow results carry a trace array in
//     attributes.data / the promoted result column,
//   - a SourcePropagator that stamps a "source" (manual/scheduled/retry) into
//     workflow metadata and is inherited by sub-workflows,
//   - a workflow tree (parent -> sub-workflows -> activities),
//   - a deliberately failing workflow (so the Failed badge + #469 fix show), and
//   - a ContinueAsNew'd workflow (so the lineage / previous-execution column shows).
//
// It uses the local go-workflows directly (no module replace), so it is isolated
// from the real worker.
package main

import (
	"context"
)

// ---- Result[T]: mirrors apimicro-testing-worker/engine/schema.Result ----

type Result[T any] struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Error   string `json:"error,omitempty"`
	Context T      `json:"context"`
	Trace   any    `json:"trace,omitempty"`
}

const (
	StatusReady  = "ready"
	StatusDone   = "done"
	StatusFailed = "failed"
)

// requestTrace mirrors the real worker's RequestTrace: one captured HTTP call.
type requestTrace struct {
	Method   string `json:"method"`
	URL      string `json:"url"`
	Status   int    `json:"status"`
	RespBody string `json:"resp_body,omitempty"`
	Duration string `json:"duration"`
}

// traceKey carries accumulated HTTP traces on the Go context (like utils.WithTrace).
type traceCtxKey struct{}

func withTrace(ctx context.Context) context.Context {
	return context.WithValue(ctx, traceCtxKey{}, &[]requestTrace{})
}

func appendTrace(ctx context.Context, t requestTrace) {
	if v, ok := ctx.Value(traceCtxKey{}).(*[]requestTrace); ok && v != nil {
		*v = append(*v, t)
	}
}

func getTrace(ctx context.Context) any {
	if v, ok := ctx.Value(traceCtxKey{}).(*[]requestTrace); ok && v != nil && len(*v) > 0 {
		return *v
	}
	return nil
}

// actDone builds a successful activity result carrying any captured trace.
func actDone[T any](msg string, bag T, ctx context.Context) (Result[T], error) {
	return Result[T]{Status: StatusDone, Message: msg, Context: bag, Trace: getTrace(ctx)}, nil
}
