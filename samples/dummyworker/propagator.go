package main

import (
	"context"

	"github.com/denisaditya0/go-workflows/workflow"
)

// This mirrors apimicro-testing-worker/engine/schema.SourcePropagator: it carries
// a "source" string (manual/scheduled/retry) into workflow metadata so spawned /
// sub workflows inherit the originating source. It is the one custom value the
// real worker stamps into attributes.data's metadata map.

const metadataSourceKey = "source"

type sourceCtxKey struct{}

func withSource(ctx context.Context, source string) context.Context {
	return context.WithValue(ctx, sourceCtxKey{}, source)
}

func sourceFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(sourceCtxKey{}).(string); ok {
		return v
	}
	return ""
}

func withSourceWf(ctx workflow.Context, source string) workflow.Context {
	return workflow.WithValue(ctx, sourceCtxKey{}, source)
}

func sourceFromWorkflow(ctx workflow.Context) string {
	if v, ok := ctx.Value(sourceCtxKey{}).(string); ok {
		return v
	}
	return ""
}

type SourcePropagator struct{}

var _ workflow.ContextPropagator = (*SourcePropagator)(nil)

func (p *SourcePropagator) Inject(ctx context.Context, m *workflow.Metadata) error {
	if src := sourceFromContext(ctx); src != "" {
		m.Set(metadataSourceKey, src)
	}
	return nil
}

func (p *SourcePropagator) Extract(ctx context.Context, m *workflow.Metadata) (context.Context, error) {
	if src := m.Get(metadataSourceKey); src != "" {
		return withSource(ctx, src), nil
	}
	return ctx, nil
}

func (p *SourcePropagator) InjectFromWorkflow(ctx workflow.Context, m *workflow.Metadata) error {
	if src := sourceFromWorkflow(ctx); src != "" {
		m.Set(metadataSourceKey, src)
	}
	return nil
}

func (p *SourcePropagator) ExtractToWorkflow(ctx workflow.Context, m *workflow.Metadata) (workflow.Context, error) {
	if src := m.Get(metadataSourceKey); src != "" {
		return withSourceWf(ctx, src), nil
	}
	return ctx, nil
}
