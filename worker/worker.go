package worker

import (
	"context"
	"fmt"

	"github.com/benbjohnson/clock"
	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/interceptor"
	"github.com/cschleiden/go-workflows/internal/signals"
	internal "github.com/cschleiden/go-workflows/internal/worker"
	"github.com/cschleiden/go-workflows/internal/workflows"
	"github.com/cschleiden/go-workflows/registry"
	"github.com/cschleiden/go-workflows/workflow"
)

type Worker struct {
	backend backend.Backend

	registry *registry.Registry

	workers []worker
}

type worker interface {
	Start(context.Context) error
	WaitForCompletion() error
}

// New creates a worker that processes workflows and activities.
func New(backend backend.Backend, options *Options) *Worker {
	registry := registry.New()

	if options == nil {
		options = &DefaultOptions
	} else {
		applyDefaults(options)
	}

	var interceptors []interceptor.Interceptor
	if options.Interceptors != nil {
		interceptors = options.Interceptors
	}

	workflowWorker := newWorkflowWorker(backend, registry, &options.WorkflowWorkerOptions, interceptors)
	activityWorker := newActivityWorker(backend, registry, &options.ActivityWorkerOptions, interceptors)

	// Register internal activities
	return newWorker(backend, registry, []worker{workflowWorker, activityWorker})
}

// NewWorkflowWorker creates a worker that only processes workflows.
func NewWorkflowWorker(backend backend.Backend, options *WorkflowWorkerOptions) *Worker {
	registry := registry.New()

	return newWorker(backend, registry, []worker{newWorkflowWorker(backend, registry, options, nil)})
}

// NewActivityWorker creates a worker that only processes activities.
func NewActivityWorker(backend backend.Backend, options *ActivityWorkerOptions) *Worker {
	registry := registry.New()

	return newWorker(backend, registry, []worker{newActivityWorker(backend, registry, options, nil)})
}

func newWorker(backend backend.Backend, registry *registry.Registry, workers []worker) *Worker {
	// Register system activites and workflows
	if err := registry.RegisterActivity(&signals.Activities{Signaler: client.New(backend)}); err != nil {
		panic(fmt.Errorf("registering internal activities: %w", err))
	}

	if err := registry.RegisterActivity(&workflows.Activities{Backend: backend}); err != nil {
		panic(fmt.Errorf("registering internal activities: %w", err))
	}

	if err := registry.RegisterWorkflow(workflows.ExpireWorkflowInstances); err != nil {
		panic(fmt.Errorf("registering internal workflow: %w", err))
	}

	return &Worker{
		backend: backend,

		workers:  workers,
		registry: registry,
	}
}

func newActivityWorker(backend backend.Backend, registry *registry.Registry, options *ActivityWorkerOptions, interceptors []interceptor.Interceptor) worker {
	if options == nil {
		options = &DefaultOptions.ActivityWorkerOptions
	}

	activityWorker := internal.NewActivityWorker(backend, registry, clock.New(), internal.WorkerOptions{
		Pollers:            options.ActivityPollers,
		PollingInterval:    options.ActivityPollingInterval,
		MaxPollingInterval: options.MaxActivityPollingInterval,
		BackoffMultiplier:  options.ActivityPollingBackoffMultiplier,
		MaxParallelTasks:   options.MaxParallelActivityTasks,
		HeartbeatInterval:  options.ActivityHeartbeatInterval,
		Queues:             options.ActivityQueues,
	}, interceptors)

	return activityWorker
}

func newWorkflowWorker(backend backend.Backend, registry *registry.Registry, options *WorkflowWorkerOptions, interceptors []interceptor.Interceptor) worker {
	if options == nil {
		options = &DefaultOptions.WorkflowWorkerOptions
	}

	workflowWorker := internal.NewWorkflowWorker(backend, registry, internal.WorkflowWorkerOptions{
		WorkerOptions: internal.WorkerOptions{
			Pollers:            options.WorkflowPollers,
			PollingInterval:    options.WorkflowPollingInterval,
			MaxPollingInterval: options.MaxWorkflowPollingInterval,
			BackoffMultiplier:  options.WorkflowPollingBackoffMultiplier,
			MaxParallelTasks:   options.MaxParallelWorkflowTasks,
			HeartbeatInterval:  options.WorkflowHeartbeatInterval,
			Queues:             options.WorkflowQueues,
		},
		WorkflowExecutorCache:     options.WorkflowExecutorCache,
		WorkflowExecutorCacheSize: options.WorkflowExecutorCacheSize,
		WorkflowExecutorCacheTTL:  options.WorkflowExecutorCacheTTL,
	}, interceptors)

	return workflowWorker
}

// Start starts the worker.
//
// To stop the worker, cancel the context passed to Start. To wait for completion of the active
// tasks, call `WaitForCompletion`.
func (w *Worker) Start(ctx context.Context) error {
	for _, worker := range w.workers {
		if err := worker.Start(ctx); err != nil {
			return fmt.Errorf("starting worker: %w", err)
		}
	}

	return nil
}

// WaitForCompletion waits for all active tasks to complete.
func (w *Worker) WaitForCompletion() error {
	for _, worker := range w.workers {
		if err := worker.WaitForCompletion(); err != nil {
			return fmt.Errorf("waiting for worker completion: %w", err)
		}
	}

	return nil
}

// RegisterWorkflow registers a workflow with the worker's registry.
func (w *Worker) RegisterWorkflow(wf workflow.Workflow, opts ...registry.RegisterOption) error {
	return w.registry.RegisterWorkflow(wf, opts...)
}

// RegisterActivity registers an activity with the worker's registry.
func (w *Worker) RegisterActivity(a workflow.Activity, opts ...registry.RegisterOption) error {
	return w.registry.RegisterActivity(a, opts...)
}

// applyDefaults fills zero-value fields in options with values from DefaultOptions.
// This allows users to only specify the fields they care about without losing defaults.
func applyDefaults(o *Options) {
	d := &DefaultOptions

	// Workflow worker defaults
	if o.WorkflowPollers == 0 {
		o.WorkflowPollers = d.WorkflowPollers
	}
	if o.WorkflowPollingInterval == 0 {
		o.WorkflowPollingInterval = d.WorkflowPollingInterval
	}
	if o.WorkflowHeartbeatInterval == 0 {
		o.WorkflowHeartbeatInterval = d.WorkflowHeartbeatInterval
	}
	if o.WorkflowExecutorCacheSize == 0 {
		o.WorkflowExecutorCacheSize = d.WorkflowExecutorCacheSize
	}
	if o.WorkflowExecutorCacheTTL == 0 {
		o.WorkflowExecutorCacheTTL = d.WorkflowExecutorCacheTTL
	}

	// Activity worker defaults
	if o.ActivityPollers == 0 {
		o.ActivityPollers = d.ActivityPollers
	}
	if o.ActivityPollingInterval == 0 {
		o.ActivityPollingInterval = d.ActivityPollingInterval
	}
	if o.ActivityHeartbeatInterval == 0 {
		o.ActivityHeartbeatInterval = d.ActivityHeartbeatInterval
	}
}
