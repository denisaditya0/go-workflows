package main

import (
	"context"
	"log"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/interceptor"
	"github.com/cschleiden/go-workflows/samples"
	"github.com/cschleiden/go-workflows/worker"

	"github.com/google/uuid"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	b := samples.GetBackend("interceptor", true)

	// Run worker with interceptors
	w := RunWorker(ctx, b)

	// Start workflow via client
	c := client.New(b)

	runWorkflow(ctx, c)

	cancel()

	if err := w.WaitForCompletion(); err != nil {
		panic("could not stop worker: " + err.Error())
	}
}

func runWorkflow(ctx context.Context, c *client.Client) {
	wf, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: uuid.NewString(),
	}, OrderWorkflow, OrderInput{
		CustomerID: "customer-123",
		Amount:     99.99,
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := client.GetWorkflowResult[string](ctx, c, wf, time.Second*30)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Workflow finished. Result:", result)
}

func RunWorker(ctx context.Context, mb backend.Backend) *worker.Worker {
	// Create worker with global interceptors
	opts := worker.DefaultOptions
	opts.Interceptors = []interceptor.Interceptor{
		// Global: applies to ALL workflows and activities
		&interceptor.LoggingInterceptor{},

		// Custom: duration tracking for all executions
		&DurationInterceptor{},
	}

	w := worker.New(mb, &opts)

	w.RegisterWorkflow(OrderWorkflow)

	w.RegisterActivity(ValidateOrder)
	w.RegisterActivity(ChargePayment)
	w.RegisterActivity(SendConfirmation)

	if err := w.Start(ctx); err != nil {
		panic("could not start worker")
	}

	return w
}
