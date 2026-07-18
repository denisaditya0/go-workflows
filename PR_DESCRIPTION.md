## Problem

When workers are idle, pollers hit the database at a fixed interval (default 200ms). With many pollers this creates significant unnecessary DB load. For example, 20 pollers at 200ms = 100 queries/sec per queue, even when there are no tasks.

In production scenarios with multiple queues and higher poller counts (e.g., 150 pollers total), this translates to ~750 queries/sec of pure overhead during idle periods.

## Current workaround

The only option today is to manually increase `PollingInterval` (e.g., from 200ms to 1-2s) and reduce poller count. This reduces idle load but introduces a permanent trade-off: task pickup latency is always slower, even under load when fast response matters.

```go
// Current: pick one fixed interval — can't optimize for both idle and busy
WorkflowPollingInterval: 1 * time.Second,         // low DB load, but 1s pickup latency even when busy
WorkflowPollingInterval: 200 * time.Millisecond,  // fast pickup, but high DB load when idle
```

There's no way to get fast pickup under load AND low DB pressure when idle with a fixed interval.

## Solution

Add optional exponential backoff on consecutive empty polls. The interval resets immediately when a task is found, ensuring fast pickup under load.

This gives the best of both worlds:
- Under load: pollers operate at full speed (base interval), tasks are picked up immediately
- When idle: pollers gradually slow down, dramatically reducing database pressure

### New options

| Option | Default | Description |
|--------|---------|-------------|
| `MaxPollingInterval` | `0` (disabled) | Upper bound for backoff. 0 = no backoff (current behavior) |
| `BackoffMultiplier` | `2.0` | Growth factor per empty poll |

These are exposed for both workflow and activity workers:

- `MaxWorkflowPollingInterval` / `WorkflowPollingBackoffMultiplier`
- `MaxActivityPollingInterval` / `ActivityPollingBackoffMultiplier`

### Usage

```go
worker.New(backend, &worker.Options{
    WorkflowWorkerOptions: worker.WorkflowWorkerOptions{
        WorkflowPollers:                  8,
        WorkflowPollingInterval:          200 * time.Millisecond,
        MaxWorkflowPollingInterval:       2 * time.Second,
        WorkflowPollingBackoffMultiplier: 2.0,
    },
    ActivityWorkerOptions: worker.ActivityWorkerOptions{
        ActivityPollers:                  8,
        ActivityPollingInterval:          200 * time.Millisecond,
        MaxActivityPollingInterval:       2 * time.Second,
        ActivityPollingBackoffMultiplier: 2.0,
    },
})
```

### Behavior

```
Empty poll:  200ms -> 400ms -> 800ms -> 1.6s -> 2s (capped at MaxPollingInterval)
Task found:  immediately resets to 200ms (base PollingInterval)
```

Each poller maintains its own independent interval. No shared state, no mutex, no coordination overhead.

## Benchmark results

```
goos: windows
goarch: amd64
cpu: Intel(R) Core(TM) Ultra 5 225U

BenchmarkPolling_WithoutBackoff-14    1212    1000521 ns/op    732.0 polls
BenchmarkPolling_WithBackoff-14       1196     999492 ns/op     28.00 polls
```

| Metric | Without backoff | With backoff | Reduction |
|--------|----------------|--------------|-----------|
| Polls per second (1 poller) | 732 | 28 | 96% |
| Projected DB queries (20 pollers) | 14,640/sec | 560/sec | 96% |
| Projected DB queries (150 pollers) | 109,800/sec | 4,200/sec | 96% |

Task pickup latency when busy is unchanged (immediate `continue` on task found, same as before).

## Backward-compatible

- `MaxPollingInterval = 0` (default) preserves exact current behavior
- No changes needed for existing users
- No new dependencies
- All existing tests pass without modification

## Implementation details

- Replaced fixed `time.Ticker` with `time.Timer` + manual `Reset()` to support variable intervals
- `backoff()` is a simple pure function: `next = min(current * multiplier, max)`
- Per-poller goroutine state only (no shared mutable state between pollers)
- The `continue` fast-path on task found is preserved, no regression in throughput

## Changes

| File | Change |
|------|--------|
| `internal/worker/worker.go` | Adaptive poller loop + `backoff()` helper, new fields in `WorkerOptions` |
| `worker/options.go` | New public options for workflow + activity workers |
| `worker/worker.go` | Pass-through new options to internal worker |
| `internal/worker/worker_test.go` | 6 unit tests + 2 benchmarks for adaptive polling |

## Test coverage

- Backoff increases interval on consecutive empty polls
- Backoff caps at MaxPollingInterval
- Default multiplier (2.0) when not specified
- Custom multiplier values
- Interval resets immediately when task is found
- No backoff when MaxPollingInterval is 0 (backward compat)
- Measurably fewer polls with backoff enabled
- All existing tests pass unchanged
