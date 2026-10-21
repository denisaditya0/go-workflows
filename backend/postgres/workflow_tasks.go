package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/backend/metadata"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/internal/workflowerrors"
	"github.com/cschleiden/go-workflows/workflow"
)

func (pb *postgresBackend) PrepareWorkflowQueues(ctx context.Context, queues []workflow.Queue) error {
	return nil
}

// GetWorkflowTask returns a pending workflow task or nil if there are no pending workflow executions
func (pb *postgresBackend) GetWorkflowTask(ctx context.Context, queues []workflow.Queue) (*backend.WorkflowTask, error) {
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

	now := time.Now()
	args := []any{
		core.WorkflowInstanceStateActive, // state
		now,                              // event.visible_at
		now,                              // locked_until
		now,                              // sticky_until
		pb.workerName,                    // worker
	}

	for _, q := range queues {
		args = append(args, string(q))
	}

	// Lock next workflow task by finding an unlocked instance with new events to process.
	row := tx.QueryRowContext(
		ctx,
		fmt.Sprintf(`SELECT i.id, i.queue, i.instance_id, i.execution_id, i.parent_instance_id, i.parent_execution_id, i.parent_schedule_event_id, i.metadata, i.sticky_until
			FROM instances i
			INNER JOIN pending_events pe ON i.instance_id = pe.instance_id AND i.execution_id = pe.execution_id
			WHERE
				i.state = $1 AND i.completed_at IS NULL
				AND (pe.visible_at IS NULL OR pe.visible_at <= $2)
				AND (i.locked_until IS NULL OR i.locked_until < $3)
				AND (i.sticky_until IS NULL OR i.sticky_until < $4 OR i.worker = $5)
				AND (i.queue in (%s))
			LIMIT 1
			FOR UPDATE OF i SKIP LOCKED`, pgPlaceholders(6, len(queues))),
		args...,
	)

	var id int
	var queue, instanceID, executionID string
	var parentInstanceID, parentExecutionID sql.NullString
	var parentEventID sql.NullInt64
	var metadataJson sql.NullString
	var stickyUntil sql.NullTime
	if err := row.Scan(&id, &queue, &instanceID, &executionID, &parentInstanceID, &parentExecutionID, &parentEventID, &metadataJson, &stickyUntil); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("scanning workflow instance: %w", err)
	}

	res, err := tx.ExecContext(
		ctx,
		`UPDATE instances i
			SET locked_until = $1, worker = $2
			WHERE id = $3`,
		now.Add(pb.options.WorkflowLockTimeout),
		pb.workerName,
		id,
	)
	if err != nil {
		return nil, fmt.Errorf("locking workflow instance: %w", err)
	}

	if affectedRows, err := res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("locking workflow instance: %w", err)
	} else if affectedRows == 0 {
		// No instance locked?
		return nil, nil
	}

	var wfi *workflow.Instance
	if parentInstanceID.Valid && parentExecutionID.Valid && parentEventID.Valid {
		wfi = core.NewSubWorkflowInstance(instanceID, executionID, core.NewWorkflowInstance(parentInstanceID.String, parentExecutionID.String), parentEventID.Int64)
	} else {
		wfi = core.NewWorkflowInstance(instanceID, executionID)
	}

	var metadata *metadata.WorkflowMetadata
	if metadataJson.Valid {
		if err := json.Unmarshal([]byte(metadataJson.String), &metadata); err != nil {
			return nil, fmt.Errorf("parsing workflow metadata: %w", err)
		}
	}

	t := &backend.WorkflowTask{
		ID:                    wfi.InstanceID,
		WorkflowInstance:      wfi,
		WorkflowInstanceState: core.WorkflowInstanceStateActive,
		Metadata:              metadata,
		NewEvents:             []*history.Event{},
		Queue:                 workflow.Queue(queue),
	}

	// Get new events
	events, err := tx.QueryContext(
		ctx,
		"SELECT pe.event_id, pe.sequence_id, pe.event_type, pe.timestamp, pe.schedule_event_id, a.data, pe.visible_at FROM pending_events pe LEFT JOIN attributes a ON pe.instance_id = a.instance_id AND pe.execution_id = a.execution_id AND pe.event_id = a.event_id WHERE pe.instance_id = $1 AND pe.execution_id = $2 AND (pe.visible_at IS NULL OR pe.visible_at <= $3) ORDER BY pe.id",
		instanceID,
		executionID,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("getting new events: %w", err)
	}

	defer events.Close()

	for events.Next() {
		var attributes []byte

		historyEvent := &history.Event{}

		if err := events.Scan(
			&historyEvent.ID,
			&historyEvent.SequenceID,
			&historyEvent.Type,
			&historyEvent.Timestamp,
			&historyEvent.ScheduleEventID,
			&attributes,
			&historyEvent.VisibleAt,
		); err != nil {
			return nil, fmt.Errorf("scanning event: %w", err)
		}

		a, err := history.DeserializeAttributes(historyEvent.Type, attributes)
		if err != nil {
			return nil, fmt.Errorf("deserializing attributes: %w", err)
		}

		historyEvent.Attributes = a

		t.NewEvents = append(t.NewEvents, historyEvent)
	}

	if events.Err() != nil {
		return nil, events.Err()
	}

	// Return if there aren't any new events
	if len(t.NewEvents) == 0 {
		return nil, nil
	}

	// Get most recent sequence id
	var lastSequenceID sql.NullInt64
	row = tx.QueryRowContext(ctx, "SELECT MAX(sequence_id) FROM history WHERE instance_id = $1 AND execution_id = $2", instanceID, executionID)
	if err := row.Scan(
		&lastSequenceID,
	); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("getting most recent sequence id: %w", err)
		}
	}

	if lastSequenceID.Valid {
		t.LastSequenceID = lastSequenceID.Int64
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return t, nil
}

// CompleteWorkflowTask completes a workflow task retrieved using GetWorkflowTask
//
// This checkpoints the execution. events are new events from the last workflow execution
// which will be added to the workflow instance history. workflowEvents are new events for the
// completed or other workflow instances.
func (pb *postgresBackend) CompleteWorkflowTask(
	ctx context.Context,
	task *backend.WorkflowTask,
	state core.WorkflowInstanceState,
	executedEvents, activityEvents, timerEvents []*history.Event,
	workflowEvents []*history.WorkflowEvent,
) error {
	tx, err := pb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	instance := task.WorkflowInstance

	// Unlock instance, but keep it sticky to the current worker
	var completedAt *time.Time
	if state == core.WorkflowInstanceStateContinuedAsNew || state == core.WorkflowInstanceStateFinished {
		t := time.Now()
		completedAt = &t
	}

	res, err := tx.ExecContext(
		ctx,
		`UPDATE instances SET locked_until = NULL, sticky_until = $1, completed_at = $2, state = $3 WHERE instance_id = $4 AND execution_id = $5 AND worker = $6 AND locked_until IS NOT NULL`,
		time.Now().Add(pb.options.StickyTimeout),
		completedAt,
		state,
		instance.InstanceID,
		instance.ExecutionID,
		pb.workerName,
	)
	if err != nil {
		return fmt.Errorf("unlocking instance: %w", err)
	}

	changedRows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking for unlocked workflow instances: %w", err)
	} else if changedRows != 1 {
		return errors.New("could not find workflow instance to unlock")
	}

	// Remove handled events from task
	if len(executedEvents) > 0 {
		args := make([]interface{}, 0, len(executedEvents)+2)
		args = append(args, instance.InstanceID, instance.ExecutionID)
		for _, e := range executedEvents {
			args = append(args, e.ID)
		}

		if _, err := tx.ExecContext(
			ctx,
			fmt.Sprintf(`DELETE FROM pending_events WHERE instance_id = $1 AND execution_id = $2 AND event_id IN (%s)`, pgPlaceholders(3, len(executedEvents))),
			args...,
		); err != nil {
			return fmt.Errorf("deleting handled new events: %w", err)
		}
	}

	// Insert new events generated during this workflow execution to the history
	if err := insertHistoryEvents(ctx, tx, instance, executedEvents); err != nil {
		return fmt.Errorf("inserting new history events: %w", err)
	}

	// Schedule activities
	for _, e := range activityEvents {
		a := e.Attributes.(*history.ActivityScheduledAttributes)
		queue := a.Queue
		if queue == "" {
			// Default to workflow queue
			queue = task.Queue
		}

		if err := scheduleActivity(ctx, tx, queue, instance, e); err != nil {
			return fmt.Errorf("scheduling activity: %w", err)
		}
	}

	// Timer events
	if err := insertPendingEvents(ctx, tx, instance, timerEvents); err != nil {
		return fmt.Errorf("scheduling timers: %w", err)
	}

	for _, event := range executedEvents {
		switch event.Type {
		case history.EventType_TimerCanceled:
			if err := removeFutureEvent(ctx, tx, instance, event.ScheduleEventID); err != nil {
				return fmt.Errorf("removing future event: %w", err)
			}
		}
	}

	// Insert new workflow events
	groupedEvents := history.EventsByWorkflowInstance(workflowEvents)

	for targetInstance, events := range groupedEvents {
		// Are we creating a new sub-workflow instance?
		m := events[0]
		if m.HistoryEvent.Type == history.EventType_WorkflowExecutionStarted {
			a := m.HistoryEvent.Attributes.(*history.ExecutionStartedAttributes)

			queue := a.Queue
			if queue == "" {
				queue = task.Queue
			}

			// Create new instance
			if err := createInstance(ctx, tx, queue, m.WorkflowInstance, a.Metadata); err != nil {
				if errors.Is(err, backend.ErrInstanceAlreadyExists) {
					if err := insertPendingEvents(ctx, tx, instance, []*history.Event{
						history.NewPendingEvent(time.Now(), history.EventType_SubWorkflowFailed, &history.SubWorkflowFailedAttributes{
							Error: workflowerrors.FromError(backend.ErrInstanceAlreadyExists),
						}, history.ScheduleEventID(m.WorkflowInstance.ParentEventID)),
					}); err != nil {
						return fmt.Errorf("inserting sub-workflow failed event: %w", err)
					}

					continue
				}

				return fmt.Errorf("creating sub-workflow instance: %w", err)
			}
		}

		// Insert pending events for target instance
		historyEvents := []*history.Event{}
		for _, m := range events {
			historyEvents = append(historyEvents, m.HistoryEvent)
		}
		if err := insertPendingEvents(ctx, tx, &targetInstance, historyEvents); err != nil {
			return fmt.Errorf("inserting messages: %w", err)
		}
	}

	if pb.options.RemoveContinuedAsNewInstances && state == core.WorkflowInstanceStateContinuedAsNew {
		if err := pb.removeWorkflowInstance(ctx, instance, tx); err != nil {
			return fmt.Errorf("removing old instance: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing complete workflow transaction: %w", err)
	}

	return nil
}

func (pb *postgresBackend) ExtendWorkflowTask(ctx context.Context, task *backend.WorkflowTask) error {
	tx, err := pb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	until := time.Now().Add(pb.options.WorkflowLockTimeout)
	res, err := tx.ExecContext(
		ctx,
		`UPDATE instances SET locked_until = $1 WHERE instance_id = $2 AND execution_id = $3 AND worker = $4`,
		until,
		task.WorkflowInstance.InstanceID,
		task.WorkflowInstance.ExecutionID,
		pb.workerName,
	)
	if err != nil {
		return fmt.Errorf("extending workflow task lock: %w", err)
	}

	if rowsAffected, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("determining if workflow task was extended: %w", err)
	} else if rowsAffected == 0 {
		return errors.New("could not extend workflow task")
	}

	return tx.Commit()
}
