package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/workflow"
)

func (pb *postgresBackend) CreateWorkflowInstance(ctx context.Context, instance *workflow.Instance, event *history.Event) error {
	tx, err := pb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	a := event.Attributes.(*history.ExecutionStartedAttributes)

	// Create workflow instance
	if err := createInstance(ctx, tx, a.Queue, instance, a.Metadata); err != nil {
		return err
	}

	// Initial history is empty, store only new events
	if err := insertPendingEvents(ctx, tx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting new event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("creating workflow instance: %w", err)
	}

	return nil
}

func (pb *postgresBackend) RemoveWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance) error {
	tx, err := pb.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := pb.removeWorkflowInstance(ctx, instance, tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (pb *postgresBackend) removeWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance, tx *sql.Tx) error {
	row := tx.QueryRowContext(ctx, "SELECT state FROM instances WHERE instance_id = $1 AND execution_id = $2 LIMIT 1", instance.InstanceID, instance.ExecutionID)
	var state core.WorkflowInstanceState
	if err := row.Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return backend.ErrInstanceNotFound
		}
		return err
	}

	if state == core.WorkflowInstanceStateActive {
		return backend.ErrInstanceNotFinished
	}

	// Delete from instances and history tables
	if _, err := tx.ExecContext(ctx, "DELETE FROM instances WHERE instance_id = $1 AND execution_id = $2", instance.InstanceID, instance.ExecutionID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM history WHERE instance_id = $1 AND execution_id = $2", instance.InstanceID, instance.ExecutionID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM attributes WHERE instance_id = $1 AND execution_id = $2", instance.InstanceID, instance.ExecutionID); err != nil {
		return err
	}

	return nil
}

func (pb *postgresBackend) RemoveWorkflowInstances(ctx context.Context, options ...backend.RemovalOption) error {
	ro := backend.DefaultRemovalOptions
	for _, opt := range options {
		opt(&ro)
	}

	rows, err := pb.db.QueryContext(ctx, "SELECT instance_id, execution_id FROM instances WHERE completed_at < $1", ro.FinishedBefore)
	if err != nil {
		return err
	}
	defer rows.Close()

	pairs := make([]struct {
		instanceID  string
		executionID string
	}, 0)
	for rows.Next() {
		var id, executionID string
		if err := rows.Scan(&id, &executionID); err != nil {
			return err
		}
		pairs = append(pairs, struct {
			instanceID  string
			executionID string
		}{
			instanceID:  id,
			executionID: executionID,
		})
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	pgPairedPlaceholders := func(startIdx, pairCount int) string {
		placeholders := make([]string, pairCount)
		for i := 0; i < pairCount; i++ {
			placeholders[i] = fmt.Sprintf("($%d, $%d)", startIdx+i*2, startIdx+i*2+1)
		}
		return strings.Join(placeholders, ", ")
	}

	batchSize := ro.BatchSize
	for i := 0; i < len(pairs); i += batchSize {
		tx, err := pb.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		batch := pairs[i:min(i+batchSize, len(pairs))]
		ph := pgPairedPlaceholders(1, len(batch))

		whereCondition := fmt.Sprintf("(instance_id, execution_id) IN (%s)", ph)

		args := make([]interface{}, 0, len(batch)*2)
		for j := range batch {
			args = append(args, batch[j].instanceID)
			args = append(args, batch[j].executionID)
		}

		// Delete from instances, history and attributes tables
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM instances WHERE %v", whereCondition), args...); err != nil {
			_ = tx.Rollback()
			return err
		}

		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM history WHERE %v", whereCondition), args...); err != nil {
			_ = tx.Rollback()
			return err
		}

		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM attributes WHERE %v", whereCondition), args...); err != nil {
			_ = tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (pb *postgresBackend) CancelWorkflowInstance(ctx context.Context, instance *workflow.Instance, event *history.Event) error {
	tx, err := pb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Cancel workflow instance
	// TODO: Combine this with the event insertion
	res := tx.QueryRowContext(ctx, "SELECT 1 FROM instances WHERE instance_id = $1 AND execution_id = $2 LIMIT 1", instance.InstanceID, instance.ExecutionID)
	if err := res.Scan(new(int)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return backend.ErrInstanceNotFound
		}

		return err
	}

	if err := insertPendingEvents(ctx, tx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting cancellation event: %w", err)
	}

	return tx.Commit()
}

func (pb *postgresBackend) GetWorkflowInstanceHistory(ctx context.Context, instance *workflow.Instance, lastSequenceID *int64) ([]*history.Event, error) {
	tx, err := pb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var historyEvents *sql.Rows
	if lastSequenceID != nil {
		historyEvents, err = tx.QueryContext(
			ctx,
			"SELECT h.event_id, h.sequence_id, h.event_type, h.timestamp, h.schedule_event_id, a.data, h.visible_at FROM history h JOIN attributes a ON h.event_id = a.event_id AND a.instance_id = h.instance_id AND a.execution_id = h.execution_id WHERE h.instance_id = $1 AND h.execution_id = $2 AND h.sequence_id > $3 ORDER BY h.sequence_id",
			instance.InstanceID,
			instance.ExecutionID,
			*lastSequenceID,
		)
	} else {
		historyEvents, err = tx.QueryContext(
			ctx,
			"SELECT h.event_id, h.sequence_id, h.event_type, h.timestamp, h.schedule_event_id, a.data, h.visible_at FROM history h JOIN attributes a ON h.event_id = a.event_id AND a.instance_id = h.instance_id AND a.execution_id = h.execution_id WHERE h.instance_id = $1 AND h.execution_id = $2 ORDER BY h.sequence_id",
			instance.InstanceID,
			instance.ExecutionID,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("getting history: %w", err)
	}

	defer historyEvents.Close()

	h := make([]*history.Event, 0)

	for historyEvents.Next() {
		var attributes []byte

		historyEvent := &history.Event{}

		if err := historyEvents.Scan(
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

		h = append(h, historyEvent)
	}

	if historyEvents.Err() != nil {
		return nil, historyEvents.Err()
	}

	return h, nil
}

func (pb *postgresBackend) GetWorkflowInstanceState(ctx context.Context, instance *workflow.Instance) (core.WorkflowInstanceState, error) {
	row := pb.db.QueryRowContext(
		ctx,
		"SELECT state FROM instances WHERE instance_id = $1 AND execution_id = $2",
		instance.InstanceID,
		instance.ExecutionID,
	)

	var state core.WorkflowInstanceState
	if err := row.Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.WorkflowInstanceStateActive, backend.ErrInstanceNotFound
		}
		return core.WorkflowInstanceStateActive, fmt.Errorf("scanning instance state: %w", err)
	}

	return state, nil
}

func createInstance(ctx context.Context, tx *sql.Tx, queue workflow.Queue, wfi *workflow.Instance, metadata *workflow.Metadata) error {
	// Check for existing instance
	err := tx.QueryRowContext(
		ctx,
		"SELECT 1 FROM instances WHERE instance_id = $1 AND state = $2 LIMIT 1",
		wfi.InstanceID,
		core.WorkflowInstanceStateActive).
		Scan(new(int))
	if err == nil {
		return backend.ErrInstanceAlreadyExists
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	var parentInstanceID, parentExecutionID *string
	var parentEventID *int64
	if wfi.SubWorkflow() {
		parentInstanceID = &wfi.Parent.InstanceID
		parentExecutionID = &wfi.Parent.ExecutionID
		parentEventID = &wfi.ParentEventID
	}

	metadataJson, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO instances (queue, instance_id, execution_id, parent_instance_id, parent_execution_id, parent_schedule_event_id, metadata, state) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		string(queue),
		wfi.InstanceID,
		wfi.ExecutionID,
		parentInstanceID,
		parentExecutionID,
		parentEventID,
		metadataJson,
		core.WorkflowInstanceStateActive,
	)
	if err != nil {
		return fmt.Errorf("inserting workflow instance: %w", err)
	}

	return nil
}

// SignalWorkflow signals a running workflow instance
func (pb *postgresBackend) SignalWorkflow(ctx context.Context, instanceID string, event *history.Event) error {
	tx, err := pb.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// TODO: Combine this with the event insertion
	res := tx.QueryRowContext(ctx, "SELECT execution_id FROM instances WHERE instance_id = $1 AND state = $2 LIMIT 1", instanceID, core.WorkflowInstanceStateActive)
	var executionID string
	err = res.Scan(&executionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return backend.ErrInstanceNotFound
		}
		return fmt.Errorf("scanning event: %w", err)
	}

	instance := core.NewWorkflowInstance(instanceID, executionID)

	if err := insertPendingEvents(ctx, tx, instance, []*history.Event{event}); err != nil {
		return fmt.Errorf("inserting signal event: %w", err)
	}

	return tx.Commit()
}
