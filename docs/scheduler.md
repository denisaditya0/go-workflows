# Scheduler

The scheduler adds durable, cron- and interval-driven triggers to the engine. A
schedule is stored in PostgreSQL and, when due, creates a workflow instance. This
is the same shape as Temporal Schedules and River periodic jobs: the trigger is
durable state, and firing it produces ordinary engine work.

Because schedules live in the same database as workflows, the scheduler can run:

- **embedded** in the worker (`worker.WithScheduler`), or
- **standalone** in its own process (`scheduler.New(...).Start(ctx)`), pointed at
  the same database.

Multiple scheduler processes can run at once. A due schedule is leased before it
is fired, so only one process fires a given occurrence; if a process crashes
mid-fire, the lease expires and another process retries. Instance IDs are
deterministic per fire, which makes the retry idempotent.

## Concepts

| Concept | Meaning |
|---|---|
| Schedule | Durable trigger: workflow name + encoded inputs + cron/interval + policies. |
| Fire | Creating one workflow instance for the schedule's `NextRunAt`. |
| Overlap | What to do when the previous instance is still active (`skip` or `allow`). |
| Catch-up | How missed fires after downtime are handled (`none` or `all`). |
| Lease | A short reservation that prevents two processes firing the same occurrence. |

Scheduling is workflow-centric: a scheduled "task" is a workflow instance. For a
single side-effecting job, register a thin workflow that executes one activity.

## Defining schedules

```go
sched, err := scheduler.New(b, nil) // management only; loop not started
if err != nil {
    panic(err)
}

// Fixed interval ("schedule timer")
_, err = sched.Create(ctx, scheduler.ScheduleSpec{
    ID:       "cleanup-interval",
    Workflow: CleanupWorkflow,
    Interval: 15 * time.Minute,
    Args:     []any{"nightly"},
})

// Cron expression (5 fields, optional 6th seconds field, or an @macro)
_, err = sched.Create(ctx, scheduler.ScheduleSpec{
    ID:       "report-daily",
    Workflow: ReportWorkflow,
    Cron:     "0 9 * * mon-fri",
    TimeZone: "America/New_York",
})
```

Exactly one of `Cron` and `Interval` is required. Cron supports `*`, lists,
ranges, steps, month/weekday names, `?`, and the macros `@yearly`, `@monthly`,
`@weekly`, `@daily`, `@hourly`, and `@every <duration>`.

Management methods: `Get`, `List`, `Pause`, `Resume`, `Trigger`, `Delete`.

## Running the scheduler

Embedded in the worker:

```go
opts := worker.WithScheduler(&scheduler.Options{
    PollInterval:  1 * time.Second,
    LeaseDuration: 30 * time.Second,
    BatchSize:     50,
})
w := worker.New(b, opts)

// The embedded scheduler is also available for management calls:
sched := w.Scheduler()
```

Standalone (its own process):

```go
sched, err := scheduler.New(b, &scheduler.Options{PollInterval: time.Second})
if err != nil {
    panic(err)
}
if err := sched.Start(ctx); err != nil {
    panic(err)
}
defer sched.Stop()

<-ctx.Done()
sched.WaitForCompletion()
```

## Firing semantics

- `NextRunAt` is stored as an absolute UTC timestamp; the time zone only affects
  cron evaluation.
- A fire creates an instance with ID `<scheduleID>-<base36(unixNano)>`, so the
  same schedule + fire time always maps to the same instance.
- `OverlapSkip` (default): if the instance from the previous fire is still
  active, the fire is skipped and the schedule advances. `OverlapAllow` always
  creates a new instance. Overlap detection tracks the exact execution created
  by the previous fire; if that workflow used `ContinueAsNew`, the successor
  execution is not tracked, so prefer `OverlapAllow` for such schedules.
- `CatchupNone` (default): after downtime, fire once and resume at the next
  occurrence after now. `CatchupAll`: fire missed occurrences one per tick, in
  order, until caught up.
- `StartAt` prevents firing early; `EndAt` disables the schedule after the last
  fire at or before it.
- `Trigger` makes a schedule due immediately without changing its definition.

## Storage

`schedules` is created by migration `000006_schedules`. The worker task and
schedule claim loops both use `FOR UPDATE SKIP LOCKED`-style leases so multiple
processes are safe. The backend exposes this via `backend.ScheduleStore`;
`scheduler.New` returns `ErrSchedulesNotSupported` for backends that do not
implement it.
