// nolint
package p

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/denisaditya0/go-workflows/workflow"

	"sync"
)

var foo int = 42

func wfSimple(ctx workflow.Context) error {
	fmt.Println(foo)

	return nil
}

func wfWithResult(ctx workflow.Context) (string, error) {
	return "", nil
}

func wfWithTooManyResults(ctx workflow.Context) (int, string, error) { // want "workflow `wfWithTooManyResults` returns more than two values"
	return 42, "", nil
}

func wfWrongOrder(ctx workflow.Context) (error, string) { // want "workflow `wfWrongOrder` doesn't return `error` as last return value"
	return nil, ""
}

func wfWithoutReturn(ctx workflow.Context) { // want "workflow `wfWithoutReturn` doesn't return anything. needs to return at least `error`"
}

func wfIteratingOverMap(ctx workflow.Context) error {
	x := make(map[string]string)

	fmt.Println("log")

	for _, v := range x { // want "iterating over a `map` is not deterministic and not allowed in workflows"
		if v == "a" {
			return nil
		}
	}

	return nil
}

func wfUsingGoRoutine(ctx workflow.Context) error {
	go func() { // want "use `workflow.Go` instead of `go` in workflows"
		fmt.Println("hello")
	}()

	return nil
}

func wfUsingSelect(ctx workflow.Context) error {
	select { // want "`select` statements are not allowed in workflows, use `workflow.Select` instead"
	}

	return nil
}

func wfChanRange(ctx workflow.Context) error {
	for range make(chan int, 0) { // want "using native channels is not allowed in workflows, use `workflow.Channel` instead"
		if ctx == nil {
			v := make(chan int, 0) // want "using native channels is not allowed in workflows, use `workflow.Channel` instead"
			<-v

			for range make(chan int, 0) { // want "using native channels is not allowed in workflows, use `workflow.Channel` instead"
			}
		}
	}

	return nil
}

func wfVarUsage(ctx workflow.Context) error {
	var wg sync.WaitGroup // want "`sync.WaitGroup` is not allowed in workflows"
	wg.Wait()

	wg2 := &sync.WaitGroup{} // want "`sync.WaitGroup` is not allowed in workflows"
	wg2.Wait()

	return nil
}

func wfChanNestedFunc(ctx workflow.Context) error {
	x := func() {
		select {} // want "`select` statements are not allowed in workflows, use `workflow.Select` instead"
	}

	x()

	return nil
}

func wfFunctionUsage(ctx workflow.Context) error {
	time.Sleep(10 * time.Second) // want "`time.Sleep` is not allowed in workflows, use `workflow.Sleep` instead"
	fmt.Println(time.Now())      // want "`time.Now` is not allowed in workflows, use `workflow.Now` instead"

	return nil
}

func wfUsingTimers(ctx workflow.Context) error {
	_ = time.After(time.Second)     // want "`time.After` is not allowed in workflows, use `workflow.ScheduleTimer` or `workflow.Sleep` instead"
	_ = time.Tick(time.Second)      // want "`time.Tick` is not allowed in workflows, use `workflow.ScheduleTimer` or `workflow.Sleep` instead"
	_ = time.NewTimer(time.Second)  // want "`time.NewTimer` is not allowed in workflows, use `workflow.ScheduleTimer` or `workflow.Sleep` instead"
	_ = time.NewTicker(time.Second) // want "`time.NewTicker` is not allowed in workflows, use `workflow.ScheduleTimer` or `workflow.Sleep` instead"
	_ = time.Since(time.Time{})     // want "`time.Since` is not allowed in workflows, use `workflow.ScheduleTimer` or `workflow.Sleep` instead"

	return nil
}

func wfUsingRand(ctx workflow.Context) error {
	_ = rand.Intn(10) // want "`rand.Intn` is not deterministic and not allowed in workflows"

	return nil
}

var globalCounter int

func wfUsingStdContext(ctx workflow.Context) error {
	_ = context.Background() // want "`context.Background` is not allowed in workflows, use the `workflow.Context` passed into the workflow"
	_ = context.TODO()       // want "`context.TODO` is not allowed in workflows, use the `workflow.Context` passed into the workflow"

	return nil
}

func wfUsingSyncPrimitives(ctx workflow.Context) error {
	var mu sync.Mutex // want "using `sync.Mutex` is not allowed in workflows"
	mu.Lock()
	mu.Unlock()

	var once sync.Once // want "using `sync.Once` is not allowed in workflows"
	once.Do(func() {})

	return nil
}

func wfWritingPackageLevelVar(ctx workflow.Context) error {
	globalCounter = 1 // want "writing to package-level variable `globalCounter` from a workflow is not deterministic"
	globalCounter++   // want "writing to package-level variable `globalCounter` from a workflow is not deterministic"

	return nil
}

func activity(ctx context.Context) error {
	go fmt.Println("test")

	select {}

	return nil
}

type SomeField struct {
	f func() error
}

var sf SomeField = SomeField{
	f: func() error {
		go fmt.Println("24")

		c := make(chan int)
		select {
		case <-c:
		}

		for x := range c {
			fmt.Println(x)
		}

		return nil
	},
}

// Package-level function literals are not workflows and must not be reported.
var packageLevelFunc = func() {
	time.Sleep(time.Second)
	go fmt.Println("not a workflow")
}

// A function literal whose first parameter is a workflow.Context is a workflow
// and its body must be checked.
var workflowLiteral = func(ctx workflow.Context) error {
	time.Sleep(time.Second) // want "`time.Sleep` is not allowed in workflows, use `workflow.Sleep` instead"
	select {}               // want "`select` statements are not allowed in workflows, use `workflow.Select` instead"

	return nil
}

// Variables inside nested literals must be reported exactly once, even though
// both the enclosing workflow literal and the nested literal are visited.
var workflowLiteralWithNested = func(ctx workflow.Context) error {
	f := func() {
		c := make(chan int) // want "using native channels is not allowed in workflows, use `workflow.Channel` instead"
		_ = c
	}
	f()

	return nil
}

// A workflow literal assigned with `:=` inside a non-workflow package-level
// literal must still be discovered (the assignment node must not prune it).
var outerNonWorkflowLiteral = func() {
	wf := func(ctx workflow.Context) error {
		time.Sleep(time.Second) // want "`time.Sleep` is not allowed in workflows, use `workflow.Sleep` instead"

		return nil
	}
	_ = wf
}
