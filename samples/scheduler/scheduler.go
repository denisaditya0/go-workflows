package main

import (
	"context"
	"log"
	"time"

	"github.com/denisaditya0/go-workflows/samples"
	"github.com/denisaditya0/go-workflows/scheduler"
	"github.com/denisaditya0/go-workflows/worker"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	b := samples.GetBackend("scheduler", true)

	// The management API only needs the backend. This could just as well run in a
	// separate control-plane process; it does not have to be the process that runs
	// the scheduler loop.
	sched, err := scheduler.New(b, nil)
	if err != nil {
		panic(err)
	}

	// A fixed-interval ("schedule timer") trigger.
	if _, err := sched.Create(ctx, scheduler.ScheduleSpec{
		ID:       "tick-interval",
		Workflow: Tick,
		Interval: 2 * time.Second,
		Args:     []any{"interval"},
	}); err != nil {
		panic(err)
	}

	// A cron trigger. @every is a convenience macro; standard 5/6-field expressions
	// such as "*/5 * * * *" work too.
	if _, err := sched.Create(ctx, scheduler.ScheduleSpec{
		ID:       "tick-cron",
		Workflow: Tick,
		Cron:     "@every 3s",
		Args:     []any{"cron"},
	}); err != nil {
		panic(err)
	}

	// Run the scheduler embedded in the worker. To run it standalone instead, call
	// scheduler.New(b, opts) and Start it in its own process.
	w := worker.New(b, worker.WithScheduler(&scheduler.Options{
		PollInterval:  500 * time.Millisecond,
		LeaseDuration: 10 * time.Second,
		BatchSize:     20,
	}))
	w.RegisterWorkflow(Tick)

	if err := w.Start(ctx); err != nil {
		panic(err)
	}

	log.Println("Schedules are live; watching for 8 seconds...")
	time.Sleep(8 * time.Second)

	cancel()
	if err := w.WaitForCompletion(); err != nil {
		log.Println("stopping worker:", err)
	}
}
