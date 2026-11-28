package main

import (
	"github.com/denisaditya0/go-workflows/workflow"
)

// Tick is a trivial scheduled workflow. Any registered workflow can be scheduled.
func Tick(ctx workflow.Context, source string) error {
	workflow.Logger(ctx).Info("scheduled tick", "source", source, "at", workflow.Now(ctx))
	return nil
}
