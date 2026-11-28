package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/denisaditya0/go-workflows/backend"
	"github.com/denisaditya0/go-workflows/backend/payload"
	"github.com/denisaditya0/go-workflows/workflow"
)

// Compile-time check that the Postgres backend hosts schedules.
var _ backend.ScheduleStore = (*postgresBackend)(nil)

const scheduleColumns = `id, workflow_name, queue, inputs, cron_expression, interval_ns,
	timezone, start_at, end_at, enabled, overlap_policy, catchup_policy, next_run_at,
	last_run_at, last_instance_id, last_execution_id, num_fired, created_at, updated_at`

func (pb *postgresBackend) CreateSchedule(ctx context.Context, schedule *backend.Schedule) error {
	inputs, err := marshalScheduleInputs(schedule.Inputs)
	if err != nil {
		return err
	}

	_, err = pb.db.ExecContext(ctx,
		`INSERT INTO schedules
			(id, workflow_name, queue, inputs, cron_expression, interval_ns, timezone,
			 start_at, end_at, enabled, overlap_policy, catchup_policy, next_run_at,
			 last_run_at, last_instance_id, last_execution_id, num_fired)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
		schedule.ID,
		schedule.WorkflowName,
		string(schedule.Queue),
		inputs,
		nullString(schedule.Cron),
		nullInterval(schedule.Interval),
		timezoneOrUTC(schedule.TimeZone),
		schedule.StartAt,
		schedule.EndAt,
		schedule.Enabled,
		int(schedule.Overlap),
		int(schedule.Catchup),
		schedule.NextRunAt,
		schedule.LastRunAt,
		nullString(schedule.LastInstanceID),
		nullString(schedule.LastExecutionID),
		schedule.NumFired,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("schedule %q already exists: %w", schedule.ID, err)
		}
		return fmt.Errorf("creating schedule: %w", err)
	}

	return nil
}

func (pb *postgresBackend) UpdateSchedule(ctx context.Context, schedule *backend.Schedule) error {
	inputs, err := marshalScheduleInputs(schedule.Inputs)
	if err != nil {
		return err
	}

	res, err := pb.db.ExecContext(ctx,
		`UPDATE schedules SET
			workflow_name = $2, queue = $3, inputs = $4, cron_expression = $5,
			interval_ns = $6, timezone = $7, start_at = $8, end_at = $9,
			enabled = $10, overlap_policy = $11, catchup_policy = $12,
			next_run_at = $13, updated_at = CURRENT_TIMESTAMP
		 WHERE id = $1`,
		schedule.ID,
		schedule.WorkflowName,
		string(schedule.Queue),
		inputs,
		nullString(schedule.Cron),
		nullInterval(schedule.Interval),
		timezoneOrUTC(schedule.TimeZone),
		schedule.StartAt,
		schedule.EndAt,
		schedule.Enabled,
		int(schedule.Overlap),
		int(schedule.Catchup),
		schedule.NextRunAt,
	)
	if err != nil {
		return fmt.Errorf("updating schedule: %w", err)
	}

	return expectScheduleRow(res, schedule.ID)
}

func (pb *postgresBackend) DeleteSchedule(ctx context.Context, id string) error {
	res, err := pb.db.ExecContext(ctx, `DELETE FROM schedules WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting schedule: %w", err)
	}
	return expectScheduleRow(res, id)
}

func (pb *postgresBackend) GetSchedule(ctx context.Context, id string) (*backend.Schedule, error) {
	row := pb.db.QueryRowContext(ctx, `SELECT `+scheduleColumns+` FROM schedules WHERE id = $1`, id)
	s, err := scanSchedule(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, backend.ErrScheduleNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("getting schedule: %w", err)
	}
	return s, nil
}

func (pb *postgresBackend) ListSchedules(ctx context.Context) ([]*backend.Schedule, error) {
	rows, err := pb.db.QueryContext(ctx, `SELECT `+scheduleColumns+` FROM schedules ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("listing schedules: %w", err)
	}
	defer rows.Close()

	var schedules []*backend.Schedule
	for rows.Next() {
		s, err := scanSchedule(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning schedule: %w", err)
		}
		schedules = append(schedules, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("listing schedules: %w", err)
	}

	return schedules, nil
}

func (pb *postgresBackend) ClaimDueSchedules(ctx context.Context, now, leaseUntil time.Time, limit int) ([]*backend.Schedule, error) {
	if limit <= 0 {
		return nil, nil
	}

	rows, err := pb.db.QueryContext(ctx,
		`WITH due AS (
			SELECT id FROM schedules
			WHERE enabled
			  AND next_run_at <= $1
			  AND (locked_until IS NULL OR locked_until <= $1)
			  AND (start_at IS NULL OR start_at <= $1)
			ORDER BY next_run_at
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		 )
		 UPDATE schedules s
		 SET locked_until = $2
		 FROM due
		 WHERE s.id = due.id
		 RETURNING `+prefixedScheduleColumns("s."),
		now, leaseUntil, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("claiming due schedules: %w", err)
	}
	defer rows.Close()

	var schedules []*backend.Schedule
	for rows.Next() {
		s, err := scanSchedule(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning claimed schedule: %w", err)
		}
		schedules = append(schedules, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claiming due schedules: %w", err)
	}

	return schedules, nil
}

func (pb *postgresBackend) AdvanceSchedule(ctx context.Context, id string, expectedNextRunAt, nextRunAt, firedAt time.Time, instanceID, executionID string) error {
	var fired int
	if instanceID != "" {
		fired = 1
	}

	res, err := pb.db.ExecContext(ctx,
		`UPDATE schedules SET
			next_run_at = $3,
			last_run_at = $4,
			last_instance_id = CASE WHEN $5 <> '' THEN $5 ELSE last_instance_id END,
			last_execution_id = CASE WHEN $6 <> '' THEN $6 ELSE last_execution_id END,
			num_fired = num_fired + $7,
			locked_until = NULL,
			updated_at = CURRENT_TIMESTAMP
		 WHERE id = $1 AND next_run_at = $2`,
		id, expectedNextRunAt, nextRunAt, firedAt, instanceID, executionID, fired,
	)
	if err != nil {
		return fmt.Errorf("advancing schedule: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("advancing schedule: %w", err)
	}
	if rows == 0 {
		return backend.ErrScheduleStale
	}

	return nil
}

func (pb *postgresBackend) ReleaseSchedule(ctx context.Context, id string) error {
	_, err := pb.db.ExecContext(ctx,
		`UPDATE schedules SET locked_until = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("releasing schedule: %w", err)
	}
	return nil
}

func prefixedScheduleColumns(prefix string) string {
	cols := []string{
		"id", "workflow_name", "queue", "inputs", "cron_expression", "interval_ns",
		"timezone", "start_at", "end_at", "enabled", "overlap_policy", "catchup_policy",
		"next_run_at", "last_run_at", "last_instance_id", "last_execution_id",
		"num_fired", "created_at", "updated_at",
	}
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += ", "
		}
		out += prefix + c
	}
	return out
}

type scheduleScanner interface {
	Scan(dest ...any) error
}

func scanSchedule(row scheduleScanner) (*backend.Schedule, error) {
	var (
		s              backend.Schedule
		queue          string
		inputs         []byte
		cron           sql.NullString
		intervalNS     sql.NullInt64
		startAt        sql.NullTime
		endAt          sql.NullTime
		lastRunAt      sql.NullTime
		lastInstanceID sql.NullString
		lastExecID     sql.NullString
		overlap        int
		catchup        int
	)

	if err := row.Scan(
		&s.ID, &s.WorkflowName, &queue, &inputs, &cron, &intervalNS,
		&s.TimeZone, &startAt, &endAt, &s.Enabled, &overlap, &catchup,
		&s.NextRunAt, &lastRunAt, &lastInstanceID, &lastExecID, &s.NumFired,
		&s.CreatedAt, &s.UpdatedAt,
	); err != nil {
		return nil, err
	}

	s.Queue = workflow.Queue(queue)
	s.Overlap = backend.ScheduleOverlapPolicy(overlap)
	s.Catchup = backend.ScheduleCatchupPolicy(catchup)
	s.Cron = cron.String
	if intervalNS.Valid {
		s.Interval = time.Duration(intervalNS.Int64)
	}
	if startAt.Valid {
		t := startAt.Time
		s.StartAt = &t
	}
	if endAt.Valid {
		t := endAt.Time
		s.EndAt = &t
	}
	if lastRunAt.Valid {
		t := lastRunAt.Time
		s.LastRunAt = &t
	}
	s.LastInstanceID = lastInstanceID.String
	s.LastExecutionID = lastExecID.String

	if len(inputs) > 0 {
		if err := json.Unmarshal(inputs, &s.Inputs); err != nil {
			return nil, fmt.Errorf("unmarshaling schedule inputs: %w", err)
		}
	}

	return &s, nil
}

func marshalScheduleInputs(inputs []payload.Payload) ([]byte, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(inputs)
	if err != nil {
		return nil, fmt.Errorf("marshaling schedule inputs: %w", err)
	}
	return b, nil
}

func expectScheduleRow(res sql.Result, id string) error {
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("schedule %q: %w", id, backend.ErrScheduleNotFound)
	}
	return nil
}

func nullString(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func nullInterval(d time.Duration) *int64 {
	if d <= 0 {
		return nil
	}
	ns := d.Nanoseconds()
	return &ns
}

func timezoneOrUTC(tz string) string {
	if tz == "" {
		return "UTC"
	}
	return tz
}
