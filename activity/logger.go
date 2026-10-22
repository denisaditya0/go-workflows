package activity

import (
	"context"
	"log/slog"

	"github.com/cschleiden/go-workflows/internal/activityengine"
)

// Logger returns a logger with the workflow instance this activity is executed for set as default fields
func Logger(ctx context.Context) *slog.Logger {
	return activityengine.GetActivityState(ctx).Logger
}

// Attempt returns the current attempt of this activity execution
func Attempt(ctx context.Context) int {
	return activityengine.GetActivityState(ctx).Attempt
}
