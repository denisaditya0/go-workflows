package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/workflow"
)

func (pb *postgresBackend) PrepareActivityQueues(ctx context.Context, queues []workflow.Queue) error {
	return nil
}

// GetActivityTask returns a pending activity task or nil if there are no pending activities
func (pb *postgresBackend) GetActivityTask(ctx context.Context, queues []workflow.Queue) (*backend.ActivityTask, error) {
	if len(queues) == 0 {
		return nil, errors.New("no queues provided")
	}
	tx, err := pb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Lock next activity
	now := time.Now()

	args := make([]interface{}, 0, len(queues)+1)
	args = append(args, now)

	for _, q := range queues {
		args = append(args, string(q))
	}

	res := tx.QueryRowContext(
		ctx,
		fmt.Sprintf(`SELECT a.id, a.activity_id, a.instance_id, a.execution_id, a.queue,
			a.event_type, a.timestamp, a.schedule_event_id, at.data, a.visible_at
			FROM activities a
			JOIN attributes at ON at.event_id = a.activity_id AND at.instance_id = a.instance_id AND at.execution_id = a.execution_id
			WHERE (a.locked_until IS NULL OR a.locked_until < $1) AND a.queue IN (%s)
			LIMIT 1
			FOR UPDATE OF a SKIP LOCKED`, pgPlaceholders(2, len(queues))),
		args...,
	)

	var id int64
	var instanceID, executionID, queue string
	var attributes []byte
	event := &history.Event{}

	if err := res.Scan(
		&id, &event.ID, &instanceID, &executionID, &queue, &event.Type,
		&event.Timestamp, &event.ScheduleEventID, &attributes, &event.VisibleAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("finding activity task to lock: %w", err)
	}

	a, err := history.DeserializeAttributes(event.Type, attributes)
	if err != nil {
		return nil, fmt.Errorf("deserializing attributes: %w", err)
	}

	event.Attributes = a

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE activities SET locked_until = $1, worker = $2 WHERE id = $3`,
		now.Add(pb.options.ActivityLockTimeout),
		pb.workerName,
		id,
	); err != nil {
		return nil, fmt.Errorf("locking activity: %w", err)
	}

	t := &backend.ActivityTask{
		ID:               event.ID,
		ActivityID:       event.ID,
		Queue:            workflow.Queue(queue),
		WorkflowInstance: core.NewWorkflowInstance(instanceID, executionID),
		Event:            event,
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return t, nil
}

// CompleteActivityTask completes a activity task retrieved using GetActivityTask
func (pb *postgresBackend) CompleteActivityTask(ctx context.Context, task *backend.ActivityTask, result *history.Event) error {
	tx, err := pb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Remove activity
	if res, err := tx.ExecContext(
		ctx,
		`DELETE FROM activities WHERE activity_id = $1 AND instance_id = $2 AND execution_id = $3 AND worker = $4 AND queue = $5`,
		task.ActivityID,
		task.WorkflowInstance.InstanceID,
		task.WorkflowInstance.ExecutionID,
		pb.workerName,
		task.Queue,
	); err != nil {
		return fmt.Errorf("completing activity: %w", err)
	} else {
		affected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("checking for completed activity: %w", err)
		}

		if affected == 0 {
			return errors.New("could not find locked activity")
		}
	}

	// Insert new event generated during this workflow execution
	if err := insertPendingEvents(ctx, tx, task.WorkflowInstance, []*history.Event{result}); err != nil {
		return fmt.Errorf("inserting new events for completed activity: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (pb *postgresBackend) ExtendActivityTask(ctx context.Context, task *backend.ActivityTask) error {
	tx, err := pb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	until := time.Now().Add(pb.options.ActivityLockTimeout)
	_, err = tx.ExecContext(
		ctx,
		`UPDATE activities SET locked_until = $1 WHERE activity_id = $2 AND worker = $3`,
		until,
		task.ActivityID,
		pb.workerName,
	)
	if err != nil {
		return fmt.Errorf("extending activity lock: %w", err)
	}

	if err := tx.Commit(); err != nil {
		if errors.Is(err, sql.ErrTxDone) {
			return nil
		}

		return err
	}

	return nil
}

func scheduleActivity(ctx context.Context, tx *sql.Tx, queue workflow.Queue, instance *core.WorkflowInstance, event *history.Event) error {
	// Attributes are already persisted via the history, we do not need to add them again.
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO activities
			(activity_id, instance_id, execution_id, queue, event_type, timestamp, schedule_event_id, visible_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		event.ID,
		instance.InstanceID,
		instance.ExecutionID,
		string(queue),
		event.Type,
		event.Timestamp,
		event.ScheduleEventID,
		event.VisibleAt,
	)

	return err
}
