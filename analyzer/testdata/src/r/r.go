// nolint
package r

import (
	"time"

	"github.com/denisaditya0/go-workflows/workflow"
)

// registry stands in for the worker/tester registration API.
type registry struct{}

func (registry) RegisterWorkflow(any) {}

var reg = registry{}

// wfRegistered is registered below, so its body is analyzed.
func wfRegistered(ctx workflow.Context) error { // want wfRegistered:"workflow"
	time.Sleep(time.Second) // want "`time.Sleep` is not allowed in workflows, use `workflow.Sleep` instead"

	return nil
}

// helperNotRegistered takes a workflow.Context but is not registered, so it is
// not a workflow and must not be analyzed. This is the false positive that
// registration-based detection fixes.
func helperNotRegistered(ctx workflow.Context) error {
	time.Sleep(time.Second)

	return nil
}

// wfLiteral is registered by reference and must be analyzed.
var wfLiteral = func(ctx workflow.Context) error {
	time.Sleep(time.Second) // want "`time.Sleep` is not allowed in workflows, use `workflow.Sleep` instead"

	return nil
}

// literalNotRegistered is not registered and must not be analyzed.
var literalNotRegistered = func(ctx workflow.Context) error {
	time.Sleep(time.Second)

	return nil
}

func init() {
	reg.RegisterWorkflow(wfRegistered)
	reg.RegisterWorkflow(wfLiteral)

	// A literal registered inline.
	reg.RegisterWorkflow(func(ctx workflow.Context) error {
		time.Sleep(time.Second) // want "`time.Sleep` is not allowed in workflows, use `workflow.Sleep` instead"

		return nil
	})
}
