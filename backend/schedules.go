package backend

import (
	"context"
	"errors"
	"time"

	"github.com/denisaditya0/go-workflows/backend/payload"
	"github.com/denisaditya0/go-workflows/workflow"
)

// ErrScheduleNotFound is returned when a schedule does not exist.
var ErrScheduleNotFound = errors.New("schedule not found")

// ErrScheduleStale is returned when a schedule was modified or claimed by another
// process between being read and being advanced.
var ErrScheduleStale = errors.New("schedule is stale")

// ScheduleOverlapPolicy controls what happens when a schedule is due while the
// workflow instance started by its previous fire is still running.
type ScheduleOverlapPolicy int

const (
	// ScheduleOverlapSkip skips the fire if the previous instance is still active.
	ScheduleOverlapSkip ScheduleOverlapPolicy = iota

	// ScheduleOverlapAllow starts a new instance regardless of the previous one.
	ScheduleOverlapAllow
)

// ScheduleCatchupPolicy controls what happens when the scheduler was down and a
// schedule has missed one or more fire times.
type ScheduleCatchupPolicy int

const (
	// ScheduleCatchupNone fires once and moves on to the next occurrence after now,
	// skipping any missed occurrences.
	ScheduleCatchupNone ScheduleCatchupPolicy = iota

	// ScheduleCatchupAll catches up one missed occurrence per tick, in order, until
	// the schedule is back in sync. Each caught-up occurrence is a distinct fire.
	ScheduleCatchupAll
)

// Schedule is a durable, cron- or interval-driven trigger that creates workflow
// instances. Exactly one of Cron and Interval must be set.
//
// A Schedule is pure data: the scheduler runtime (package scheduler) computes
// fire times and creates workflow instances; the backend only persists it.
type Schedule struct {
	// ID is the stable, user-provided identifier of the schedule. It is also used
	// as the prefix of the instance IDs created by the schedule.
	ID string

	// WorkflowName is the name of the workflow to start. The workflow must be
	// registered with the workers processing Queue.
	WorkflowName string

	// Queue is the queue the created workflow instances are placed on. Defaults to
	// the default queue.
	Queue workflow.Queue

	// Inputs are the pre-encoded arguments passed to the workflow.
	Inputs []payload.Payload

	// Cron is a cron expression (5 or 6 fields, or an @macro). Empty if Interval is used.
	Cron string

	// Interval is a fixed delay between fires. Zero if Cron is used.
	Interval time.Duration

	// TimeZone is the IANA timezone used to evaluate Cron. Defaults to UTC.
	TimeZone string

	// StartAt, when set, prevents the schedule from firing before this time.
	StartAt *time.Time

	// EndAt, when set, disables the schedule after this time.
	EndAt *time.Time

	// Enabled pauses or resumes the schedule. A disabled schedule is never claimed.
	Enabled bool

	// Overlap decides how to handle a fire whose previous run is still active.
	Overlap ScheduleOverlapPolicy

	// Catchup decides how missed fires are handled after downtime.
	Catchup ScheduleCatchupPolicy

	// NextRunAt is the next time the schedule is due. The scheduler advances it.
	NextRunAt time.Time

	// LastRunAt is when the schedule last fired.
	LastRunAt *time.Time

	// LastInstanceID/LastExecutionID identify the most recent instance created by
	// this schedule. Used for overlap checks.
	LastInstanceID  string
	LastExecutionID string

	// NumFired is the total number of successful fires.
	NumFired int64

	CreatedAt time.Time
	UpdatedAt time.Time
}

// ScheduleStore is implemented by backends that support durable schedules. The
// scheduler runtime works against this interface; backends that do not implement
// it cannot host a scheduler.
//
// All methods are safe for concurrent use by multiple scheduler processes. A due
// schedule is leased via ClaimDueSchedules before it is fired, so at most one
// process fires a given occurrence at a time. If a process crashes mid-fire the
// lease expires and the occurrence is retried; deterministic instance IDs keep
// the retry idempotent.
type ScheduleStore interface {
	// CreateSchedule persists a new schedule.
	CreateSchedule(ctx context.Context, schedule *Schedule) error

	// UpdateSchedule replaces the mutable fields of an existing schedule, including
	// Enabled, NextRunAt, and the trigger definition.
	UpdateSchedule(ctx context.Context, schedule *Schedule) error

	// DeleteSchedule removes a schedule.
	DeleteSchedule(ctx context.Context, id string) error

	// GetSchedule returns a single schedule.
	GetSchedule(ctx context.Context, id string) (*Schedule, error)

	// ListSchedules returns all schedules ordered by ID.
	ListSchedules(ctx context.Context) ([]*Schedule, error)

	// ClaimDueSchedules atomically leases up to limit schedules whose NextRunAt is
	// at or before now, skipping rows already leased by another process. Leased
	// schedules are returned with their original NextRunAt.
	ClaimDueSchedules(ctx context.Context, now, leaseUntil time.Time, limit int) ([]*Schedule, error)

	// AdvanceSchedule records a successful fire and sets the next run time. The
	// update only applies if NextRunAt still equals expectedNextRunAt, which guards
	// against a second process racing on the same occurrence.
	AdvanceSchedule(ctx context.Context, id string, expectedNextRunAt, nextRunAt, firedAt time.Time, instanceID, executionID string) error

	// ReleaseSchedule clears the lease on a schedule without advancing it, e.g.
	// when a fire failed before an instance was created.
	ReleaseSchedule(ctx context.Context, id string) error
}
