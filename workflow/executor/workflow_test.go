package executor

import (
	"reflect"
	"testing"

	"github.com/cschleiden/go-workflows/backend/converter"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/interceptor"
	"github.com/cschleiden/go-workflows/internal/contextvalue"
	"github.com/cschleiden/go-workflows/internal/sync"
	"github.com/cschleiden/go-workflows/internal/workflowerrors"
	"github.com/stretchr/testify/require"
)

func Test_Workflow_WrapsPanic(t *testing.T) {
	f := func() {
		panic("wf panic")
	}

	w := func(ctx sync.Context) error {
		f()

		return nil
	}

	ctx := sync.Background()
	ctx = contextvalue.WithConverter(ctx, converter.DefaultConverter)

	info := &interceptor.WorkflowInfo{
		Name:     "test",
		Instance: core.NewWorkflowInstance("id", "exec"),
	}

	wf := newWorkflow(reflect.ValueOf(w), nil)
	err := wf.Execute(ctx, nil, info)
	require.NoError(t, err)

	for !wf.Completed() {
		require.NoError(t, wf.Continue())
	}

	wfErr := wf.Error()
	require.Error(t, wfErr)
	var panicErr *workflowerrors.PanicError
	require.ErrorAs(t, wfErr, &panicErr)
}
