package interceptor

import (
	"context"
	"log/slog"
)

// LoggingInterceptor logs workflow and activity execution start/completion.
// For workflow logging in a replay-aware manner, use workflow.Logger(ctx) inside
// a custom interceptor. This built-in interceptor uses slog directly, which means
// log messages will appear during replays as well.
type LoggingInterceptor struct {
	Base

	// Logger is the logger to use. If nil, slog.Default() is used.
	Logger *slog.Logger
}

func (l *LoggingInterceptor) logger() *slog.Logger {
	if l.Logger != nil {
		return l.Logger
	}
	return slog.Default()
}

func (l *LoggingInterceptor) ExecuteWorkflow(ctx WorkflowContext, info *WorkflowInfo, next WorkflowHandler) error {
	logger := l.logger()
	logger.Info("workflow started",
		slog.String("name", info.Name),
		slog.String("instance_id", info.Instance.InstanceID),
	)

	err := next(ctx)

	if err != nil {
		logger.Error("workflow failed",
			slog.String("name", info.Name),
			slog.String("instance_id", info.Instance.InstanceID),
			slog.String("error", err.Error()),
		)
	} else {
		logger.Info("workflow completed",
			slog.String("name", info.Name),
			slog.String("instance_id", info.Instance.InstanceID),
		)
	}

	return err
}

func (l *LoggingInterceptor) ExecuteActivity(ctx context.Context, info *ActivityInfo, next ActivityHandler) error {
	logger := l.logger()
	logger.Info("activity started",
		slog.String("name", info.Name),
		slog.Int("attempt", info.Attempt),
	)

	err := next(ctx)

	if err != nil {
		logger.Error("activity failed",
			slog.String("name", info.Name),
			slog.Int("attempt", info.Attempt),
			slog.String("error", err.Error()),
		)
	} else {
		logger.Info("activity completed",
			slog.String("name", info.Name),
			slog.Int("attempt", info.Attempt),
		)
	}

	return err
}

// Verify LoggingInterceptor implements Interceptor at compile time.
var _ Interceptor = (*LoggingInterceptor)(nil)
