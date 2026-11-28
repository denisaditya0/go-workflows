# Samples

## Activity Registration

Shows to register activities, both as plain functions and on `struct`s to share state.

## Cancellation

Shows how to cancel a workflow and now to react to cancellation in workflow implementations.

## Complex Parameters

Demonstrates how to pass complex parameters to workflows and activities.

## Concurrent

Demonstrates how to run multiple activities concurrently.

## Concurrent (errgroup)

Runs activities concurrently and collects their results with `golang.org/x/sync/errgroup`.

## Context Propagation

Shows how to propagate context into workflows and from there to activities.

## Dummy Worker

A standalone worker seeded with realistic data (traces, sub-workflows, failures) intended to exercise the diagnostics web UI without running a real workload.

## Continue-As-New

Shows how to use `ContinueAsNew` to restart a workflow for "infinite" workflows.

## Converter

Shows how to use a custom converter to convert between different types.

## Errors

Shows how to handle errors in workflows and activities.

## Retries

Demonstrates sub-workflow and activity retries.

## Queues

Shows how to queue workflows and activities to different queues and how to create workers that only listen for specific queues or specific tasks (workflows or activities).

## Orchestrator

Uses `worker.NewWorkflowOrchestrator` to combine a worker and client in a single component for simple, single-process scenarios.

## Scheduler

Creates durable interval and cron schedules and runs the scheduler loop embedded in a worker. `docs/scheduler.md` describes the standalone mode as well.

## Scale

Simple sample with a split worker and "starter" process.

## Signal

Shows how to send and receive signals to workflow.

## Signal to a Sub-Workflow

Shows how to send signals to a sub-workflows.

## Simple

Simple end-to-end sample with a single activity.

## Simple (Split Worker)

Same as the simple example, but with a split worker and "starter" process.

## Sub-Workflow

Shows how to implement and call a sub-workflow from a workflow.

## Timer

Shows how to use timers in workflows.

## Web / Diagnostic UI

Shows how to use the Web UI to monitor workflows and activities.

## Workflow Info

Shows how to read workflow information such as the current history length via `workflow.InstanceExecutionDetails`.

## Workflow Registration

Shows how to register workflows and activities with custom names and options, and how the tester resolves registered names.