// Package scheduler adds durable, cron- and interval-driven triggers to the
// engine. A schedule lives in the backend and, when due, creates a workflow
// instance. The runtime can run embedded in the worker or as a standalone
// process pointed at the same PostgreSQL database, so scheduling can be scaled
// or operated independently of execution.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/denisaditya0/go-workflows/backend"
	"github.com/denisaditya0/go-workflows/client"
	"github.com/denisaditya0/go-workflows/core"
	a "github.com/denisaditya0/go-workflows/internal/args"
	"github.com/denisaditya0/go-workflows/internal/common/fn"
	"github.com/denisaditya0/go-workflows/workflow"
)

// MaxScheduleIDLength matches the schedules.id column width.
const MaxScheduleIDLength = 200

// ErrSchedulesNotSupported is returned when the backend does not implement
// backend.ScheduleStore.
var ErrSchedulesNotSupported = errors.New("backend does not support schedules")

// Options configures the scheduler runtime.
type Options struct {
	// PollInterval is how often the scheduler looks for due schedules.
	// Defaults to 1s.
	PollInterval time.Duration

	// LeaseDuration is how long a claimed schedule is reserved before another
	// process may retry it. It should comfortably exceed the time needed to
	// create one workflow instance. Defaults to 30s.
	LeaseDuration time.Duration

	// BatchSize is the maximum number of due schedules claimed per poll.
	// Defaults to 50.
	BatchSize int
}

// DefaultOptions are the default scheduler runtime options.
var DefaultOptions = Options{
	PollInterval:  time.Second,
	LeaseDuration: 30 * time.Second,
	BatchSize:     50,
}

func (o *Options) applyDefaults() {
	if o.PollInterval <= 0 {
		o.PollInterval = DefaultOptions.PollInterval
	}
	if o.LeaseDuration <= 0 {
		o.LeaseDuration = DefaultOptions.LeaseDuration
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultOptions.BatchSize
	}
}

// ScheduleSpec is a convenient, user-facing description of a schedule. Args are
// converted with the backend's converter, like client.CreateWorkflowInstance.
type ScheduleSpec struct {
	// ID is the stable identifier of the schedule. Required, max 200 chars.
	ID string

	// Workflow is either a workflow function or its registered name.
	Workflow workflow.Workflow

	// Queue is the queue to start instances on. Defaults to the default queue.
	Queue workflow.Queue

	// Args are the workflow arguments.
	Args []any

	// Cron is a 5/6-field cron expression or an @macro (e.g. "@every 5m").
	// Mutually exclusive with Interval.
	Cron string

	// Interval is a fixed delay between fires. Mutually exclusive with Cron.
	Interval time.Duration

	// TimeZone is the IANA timezone used for Cron. Defaults to UTC.
	TimeZone string

	// StartAt, if set, prevents firing before this time.
	StartAt *time.Time

	// EndAt, if set, disables the schedule after this time.
	EndAt *time.Time

	// Enabled defaults to true when nil.
	Enabled *bool

	Overlap backend.ScheduleOverlapPolicy
	Catchup backend.ScheduleCatchupPolicy
}

// Scheduler owns schedule management and the polling loop.
type Scheduler struct {
	backend backend.Backend
	store   backend.ScheduleStore
	client  *client.Client

	options Options
	logger  *slog.Logger
	clock   clock.Clock

	cronMu sync.Mutex
	crons  map[string]*cronSchedule
	locs   map[string]*time.Location

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// New creates a scheduler for backend. It returns ErrSchedulesNotSupported if the
// backend does not implement backend.ScheduleStore.
func New(b backend.Backend, options *Options) (*Scheduler, error) {
	store, ok := b.(backend.ScheduleStore)
	if !ok {
		return nil, ErrSchedulesNotSupported
	}

	opts := DefaultOptions
	if options != nil {
		opts = *options
	}
	opts.applyDefaults()

	return &Scheduler{
		backend: b,
		store:   store,
		client:  client.New(b),
		options: opts,
		logger:  b.Options().Logger,
		clock:   clock.New(),
		crons:   make(map[string]*cronSchedule),
		locs:    make(map[string]*time.Location),
	}, nil
}

// Store exposes the underlying schedule store for advanced management.
func (s *Scheduler) Store() backend.ScheduleStore {
	return s.store
}

// Create validates and persists a new schedule, computing its first run time.
func (s *Scheduler) Create(ctx context.Context, spec ScheduleSpec) (*backend.Schedule, error) {
	schedule, err := s.buildSchedule(ctx, spec)
	if err != nil {
		return nil, err
	}

	if err := s.store.CreateSchedule(ctx, schedule); err != nil {
		return nil, err
	}

	return schedule, nil
}

func (s *Scheduler) buildSchedule(ctx context.Context, spec ScheduleSpec) (*backend.Schedule, error) {
	if err := validateScheduleID(spec.ID); err != nil {
		return nil, err
	}

	workflowName, err := resolveWorkflowName(spec.Workflow)
	if err != nil {
		return nil, err
	}
	if len(workflowName) > 128 {
		return nil, fmt.Errorf("workflow name too long: %d > 128", len(workflowName))
	}

	if spec.StartAt != nil && spec.EndAt != nil && spec.EndAt.Before(*spec.StartAt) {
		return nil, errors.New("EndAt must not be before StartAt")
	}

	queue := spec.Queue
	if queue == "" {
		queue = workflow.QueueDefault
	}
	if err := core.ValidQueue(queue); err != nil {
		return nil, fmt.Errorf("invalid queue %q: %w", queue, err)
	}

	if (spec.Cron == "") == (spec.Interval <= 0) {
		return nil, errors.New("exactly one of Cron or a positive Interval must be set")
	}
	if spec.Cron != "" {
		if err := validateCron(spec.Cron); err != nil {
			return nil, fmt.Errorf("invalid cron expression: %w", err)
		}
	}

	tz := spec.TimeZone
	if tz == "" {
		tz = "UTC"
	}
	if _, err := s.location(tz); err != nil {
		return nil, fmt.Errorf("invalid timezone %q: %w", tz, err)
	}

	inputs, err := a.ArgsToInputs(s.backend.Options().Converter, spec.Args...)
	if err != nil {
		return nil, fmt.Errorf("converting schedule args: %w", err)
	}

	now := s.clock.Now()
	first, err := s.firstRun(spec.Cron, spec.Interval, tz, spec.StartAt, now)
	if err != nil {
		return nil, err
	}

	enabled := true
	if spec.Enabled != nil {
		enabled = *spec.Enabled
	}

	return &backend.Schedule{
		ID:           spec.ID,
		WorkflowName: workflowName,
		Queue:        queue,
		Inputs:       inputs,
		Cron:         spec.Cron,
		Interval:     spec.Interval,
		TimeZone:     tz,
		StartAt:      spec.StartAt,
		EndAt:        spec.EndAt,
		Enabled:      enabled,
		Overlap:      spec.Overlap,
		Catchup:      spec.Catchup,
		NextRunAt:    first,
	}, nil
}

// Delete removes a schedule.
func (s *Scheduler) Delete(ctx context.Context, id string) error {
	return s.store.DeleteSchedule(ctx, id)
}

// Get returns a schedule by ID.
func (s *Scheduler) Get(ctx context.Context, id string) (*backend.Schedule, error) {
	return s.store.GetSchedule(ctx, id)
}

// List returns all schedules.
func (s *Scheduler) List(ctx context.Context) ([]*backend.Schedule, error) {
	return s.store.ListSchedules(ctx)
}

// Pause stops a schedule from firing until it is resumed.
func (s *Scheduler) Pause(ctx context.Context, id string) error {
	schedule, err := s.store.GetSchedule(ctx, id)
	if err != nil {
		return err
	}
	schedule.Enabled = false
	return s.store.UpdateSchedule(ctx, schedule)
}

// Resume re-enables a paused schedule, recomputing its next run from now.
func (s *Scheduler) Resume(ctx context.Context, id string) error {
	schedule, err := s.store.GetSchedule(ctx, id)
	if err != nil {
		return err
	}

	next, err := s.nextRun(schedule, s.clock.Now(), s.clock.Now())
	if err != nil {
		return err
	}

	schedule.Enabled = true
	schedule.NextRunAt = next
	return s.store.UpdateSchedule(ctx, schedule)
}

// Trigger makes a schedule due immediately, without changing its trigger.
func (s *Scheduler) Trigger(ctx context.Context, id string) error {
	schedule, err := s.store.GetSchedule(ctx, id)
	if err != nil {
		return err
	}
	schedule.NextRunAt = s.clock.Now()
	return s.store.UpdateSchedule(ctx, schedule)
}

// Start begins the polling loop. Cancel ctx (or call Stop) to shut it down.
func (s *Scheduler) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	s.wg.Add(1)
	go s.loop(ctx)

	return nil
}

// Stop cancels the polling loop. It is safe to call multiple times.
func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

// WaitForCompletion waits for the polling loop to exit.
func (s *Scheduler) WaitForCompletion() error {
	s.wg.Wait()
	return nil
}

func (s *Scheduler) loop(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.options.PollInterval)
	defer ticker.Stop()

	for {
		if err := s.tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.ErrorContext(ctx, "scheduler tick failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) error {
	now := s.clock.Now()
	claimed, err := s.store.ClaimDueSchedules(ctx, now, now.Add(s.options.LeaseDuration), s.options.BatchSize)
	if err != nil {
		return err
	}

	for _, schedule := range claimed {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.fire(ctx, schedule, now)
	}

	return nil
}

func (s *Scheduler) fire(ctx context.Context, schedule *backend.Schedule, now time.Time) {
	fireTime := schedule.NextRunAt

	// If the schedule is past its end, disable it instead of firing.
	if schedule.EndAt != nil && fireTime.After(*schedule.EndAt) {
		schedule.Enabled = false
		if err := s.store.UpdateSchedule(ctx, schedule); err != nil {
			s.logger.ErrorContext(ctx, "disabling ended schedule", "error", err, "schedule_id", schedule.ID)
		}
		_ = s.store.ReleaseSchedule(ctx, schedule.ID)
		return
	}

	// Overlap: optionally skip this fire while the previous instance is active.
	if schedule.Overlap == backend.ScheduleOverlapSkip && schedule.LastInstanceID != "" {
		if s.previousActive(ctx, schedule) {
			s.advance(ctx, schedule, fireTime, now, "", "")
			return
		}
	}

	instanceID := scheduledInstanceID(schedule.ID, fireTime)
	wfi, err := s.client.CreateWorkflowInstanceWithInputs(ctx, client.WorkflowInstanceOptions{
		Queue:      schedule.Queue,
		InstanceID: instanceID,
	}, schedule.WorkflowName, schedule.Inputs)
	if err != nil {
		if errors.Is(err, backend.ErrInstanceAlreadyExists) {
			// A previous attempt for this exact fire time already created the
			// instance (e.g. a retry after its lease expired). Treat as fired.
			s.logger.WarnContext(ctx, "schedule instance already exists; treating fire as complete",
				"schedule_id", schedule.ID, "instance_id", instanceID)
			s.advance(ctx, schedule, fireTime, now, "", "")
			return
		}

		s.logger.ErrorContext(ctx, "creating scheduled workflow instance", "error", err, "schedule_id", schedule.ID)
		if relErr := s.store.ReleaseSchedule(ctx, schedule.ID); relErr != nil {
			s.logger.ErrorContext(ctx, "releasing schedule", "error", relErr, "schedule_id", schedule.ID)
		}
		return
	}

	s.logger.DebugContext(ctx, "started scheduled workflow instance",
		"schedule_id", schedule.ID,
		"workflow", schedule.WorkflowName,
		"instance_id", wfi.InstanceID,
		"execution_id", wfi.ExecutionID,
	)

	s.advance(ctx, schedule, fireTime, now, wfi.InstanceID, wfi.ExecutionID)
}

func (s *Scheduler) advance(ctx context.Context, schedule *backend.Schedule, fireTime, now time.Time, instanceID, executionID string) {
	next, err := s.nextRun(schedule, fireTime, now)
	if err != nil {
		// A schedule that cannot produce a next run would otherwise spin; disable it.
		s.logger.ErrorContext(ctx, "computing next schedule run; disabling schedule",
			"error", err, "schedule_id", schedule.ID)
		schedule.Enabled = false
		schedule.NextRunAt = now.AddDate(100, 0, 0)
		if uerr := s.store.UpdateSchedule(ctx, schedule); uerr != nil {
			s.logger.ErrorContext(ctx, "disabling schedule", "error", uerr, "schedule_id", schedule.ID)
		}
		_ = s.store.ReleaseSchedule(ctx, schedule.ID)
		return
	}

	if err := s.store.AdvanceSchedule(ctx, schedule.ID, fireTime, next, now, instanceID, executionID); err != nil {
		if errors.Is(err, backend.ErrScheduleStale) {
			s.logger.WarnContext(ctx, "schedule was advanced by another process", "schedule_id", schedule.ID)
			return
		}
		s.logger.ErrorContext(ctx, "advancing schedule", "error", err, "schedule_id", schedule.ID)
	}
}

// previousActive reports whether the instance created by the last fire is still
// running.
func (s *Scheduler) previousActive(ctx context.Context, schedule *backend.Schedule) bool {
	state, err := s.backend.GetWorkflowInstanceState(ctx, &core.WorkflowInstance{
		InstanceID:  schedule.LastInstanceID,
		ExecutionID: schedule.LastExecutionID,
	})
	if err != nil {
		if errors.Is(err, backend.ErrInstanceNotFound) {
			return false
		}
		// Be conservative: on an unknown error, assume active to avoid overlapping.
		s.logger.WarnContext(ctx, "checking previous scheduled instance state",
			"error", err, "schedule_id", schedule.ID)
		return true
	}
	return state == core.WorkflowInstanceStateActive
}

// firstRun computes the first fire time for a new schedule.
func (s *Scheduler) firstRun(cronExpr string, interval time.Duration, tz string, startAt *time.Time, now time.Time) (time.Time, error) {
	if interval > 0 {
		if startAt != nil {
			return startAt.UTC(), nil
		}
		return now.Add(interval).UTC(), nil
	}

	cron, err := s.cron(cronExpr)
	if err != nil {
		return time.Time{}, err
	}

	loc, err := s.location(tz)
	if err != nil {
		return time.Time{}, err
	}

	base := now
	if startAt != nil && startAt.After(now) {
		// Step back one second so an explicit StartAt that lands exactly on a
		// matching boundary is included.
		base = startAt.Add(-time.Second)
	}

	next := cron.next(base.In(loc))
	if next.IsZero() {
		return time.Time{}, fmt.Errorf("cron expression %q has no occurrence within %d years", cronExpr, maxSearchYears)
	}
	return next.UTC(), nil
}

// nextRun computes the next fire time after fireTime, honouring the catch-up
// policy relative to now.
func (s *Scheduler) nextRun(schedule *backend.Schedule, fireTime, now time.Time) (time.Time, error) {
	if schedule.Interval > 0 {
		next := fireTime.Add(schedule.Interval)
		if schedule.Catchup == backend.ScheduleCatchupNone {
			for !next.After(now) {
				next = next.Add(schedule.Interval)
			}
		}
		return next.UTC(), nil
	}

	cron, err := s.cron(schedule.Cron)
	if err != nil {
		return time.Time{}, err
	}

	loc, err := s.location(schedule.TimeZone)
	if err != nil {
		return time.Time{}, err
	}

	next := cron.next(fireTime.In(loc))
	if next.IsZero() {
		return time.Time{}, fmt.Errorf("cron expression %q has no occurrence within %d years", schedule.Cron, maxSearchYears)
	}

	if schedule.Catchup == backend.ScheduleCatchupNone {
		for !next.After(now) {
			next = cron.next(next)
			if next.IsZero() {
				return time.Time{}, fmt.Errorf("cron expression %q has no occurrence within %d years", schedule.Cron, maxSearchYears)
			}
		}
	}

	return next.UTC(), nil
}

func (s *Scheduler) cron(expr string) (*cronSchedule, error) {
	s.cronMu.Lock()
	defer s.cronMu.Unlock()

	if c, ok := s.crons[expr]; ok {
		return c, nil
	}

	c, err := parseCron(expr)
	if err != nil {
		return nil, err
	}
	s.crons[expr] = c
	return c, nil
}

func (s *Scheduler) location(tz string) (*time.Location, error) {
	s.cronMu.Lock()
	defer s.cronMu.Unlock()

	if loc, ok := s.locs[tz]; ok {
		return loc, nil
	}

	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, err
	}
	s.locs[tz] = loc
	return loc, nil
}

func validateScheduleID(id string) error {
	if id == "" {
		return errors.New("schedule ID must not be empty")
	}
	if len(id) > MaxScheduleIDLength {
		return fmt.Errorf("schedule ID too long: %d > %d", len(id), MaxScheduleIDLength)
	}
	return nil
}

func resolveWorkflowName(wf workflow.Workflow) (string, error) {
	if name, ok := wf.(string); ok {
		if name == "" {
			return "", errors.New("workflow name must not be empty")
		}
		return name, nil
	}
	if wf == nil {
		return "", errors.New("workflow must be set")
	}
	return fn.Name(wf), nil
}

// scheduledInstanceID derives a deterministic, addressable instance ID for a
// single fire. Determinism makes retries idempotent: a replayed fire for the
// same time maps to the same instance.
func scheduledInstanceID(scheduleID string, fireTime time.Time) string {
	return fmt.Sprintf("%s-%s", scheduleID, strconv.FormatInt(fireTime.UTC().UnixNano(), 36))
}
