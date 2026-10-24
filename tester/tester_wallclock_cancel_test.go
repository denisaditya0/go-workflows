package tester

import (
	"context"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/workflow"
	"github.com/stretchr/testify/require"
)

// Test_WallClockTimer_CancelDoesNotPanic is a regression test for #494: a timer
// armed on the wall clock (wall-clock mode active due to a running activity) is
// cancelled; the stale wall-clock timer must not later fire and pop an empty timer
// slice ("slice bounds out of range [1:0]").
func Test_WallClockTimer_CancelDoesNotPanic(t *testing.T) {
	// Short activity: finishes first so the workflow cancels the still-armed,
	// longer wall-clock timer.
	shortActivity := func(ctx context.Context) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	}
	// A second, longer activity run after the cancel keeps the tester alive (and in
	// wall-clock mode) long enough for the stale cancelled timer to fire.
	keepAlive := func(ctx context.Context) error {
		time.Sleep(120 * time.Millisecond)
		return nil
	}

	wf := func(ctx workflow.Context) (string, error) {
		actFuture := workflow.ExecuteActivity[any](ctx, workflow.DefaultActivityOptions, shortActivity)

		// Longer timer, armed on the wall clock while shortActivity runs.
		timerCtx, cancelTimer := workflow.WithCancel(ctx)
		timer := workflow.ScheduleTimer(timerCtx, 80*time.Millisecond)

		workflow.Select(ctx,
			workflow.Await[any](actFuture, func(ctx workflow.Context, f workflow.Future[any]) {
				cancelTimer() // cancel the still-armed wall-clock timer
			}),
			workflow.Await[any](timer, func(ctx workflow.Context, f workflow.Future[any]) {}),
		)

		// Keep running past the cancelled timer's original fire time so a stale
		// wall-clock timer would fire into an empty slice.
		_, _ = workflow.ExecuteActivity[any](ctx, workflow.DefaultActivityOptions, keepAlive).Get(ctx)

		return "done", nil
	}

	tester := NewWorkflowTester[string](wf)
	tester.Registry().RegisterActivity(shortActivity)
	tester.Registry().RegisterActivity(keepAlive)

	require.NotPanics(t, func() {
		tester.Execute(context.Background())
	})
	require.True(t, tester.WorkflowFinished())
}
