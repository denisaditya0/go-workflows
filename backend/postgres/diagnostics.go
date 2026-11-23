package postgres

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/diag"
)

var _ diag.Backend = (*postgresBackend)(nil)

// diagInstanceColumns is the shared SELECT list for diag instance reads. It
// includes the promoted columns (workflow_name, error, previous_execution_id) and
// the real state column so the UI can show the workflow name and distinguish
// failed/completed/continued without decoding the full history (#469).
const diagInstanceColumns = `instance_id, execution_id, parent_instance_id, parent_execution_id, parent_schedule_event_id, ` +
	`created_at, completed_at, queue, state, workflow_name, (error IS NOT NULL) AS has_error, previous_execution_id`

// scanInstanceRef scans a row selected with diagInstanceColumns into a ref.
func scanInstanceRef(scan func(dest ...any) error) (*diag.WorkflowInstanceRef, error) {
	var id, executionID, queue string
	var parentID, parentExecutionID *string
	var parentScheduleEventID *int64
	var createdAt time.Time
	var completedAt *time.Time
	var stateVal int
	var workflowName *string
	var hasError bool
	var previousExecutionID *string

	if err := scan(
		&id, &executionID, &parentID, &parentExecutionID, &parentScheduleEventID,
		&createdAt, &completedAt, &queue, &stateVal, &workflowName, &hasError, &previousExecutionID,
	); err != nil {
		return nil, err
	}

	var instance *core.WorkflowInstance
	if parentID != nil {
		parentInstance := core.NewWorkflowInstance(*parentID, *parentExecutionID)
		instance = core.NewSubWorkflowInstance(id, executionID, parentInstance, *parentScheduleEventID)
	} else {
		instance = core.NewWorkflowInstance(id, executionID)
	}

	ref := &diag.WorkflowInstanceRef{
		Instance:    instance,
		CreatedAt:   createdAt,
		CompletedAt: completedAt,
		State:       core.WorkflowInstanceState(stateVal),
		Queue:       queue,
		HasError:    hasError,
	}
	if workflowName != nil {
		ref.WorkflowName = *workflowName
	}
	if previousExecutionID != nil {
		ref.PreviousExecutionID = *previousExecutionID
	}
	return ref, nil
}

func (pb *postgresBackend) GetWorkflowInstances(ctx context.Context, afterInstanceID, afterExecutionID string, count int) ([]*diag.WorkflowInstanceRef, error) {
	return pb.GetWorkflowInstancesFiltered(ctx, diag.InstanceFilter{}, afterInstanceID, afterExecutionID, count)
}

func (pb *postgresBackend) GetWorkflowInstancesFiltered(ctx context.Context, filter diag.InstanceFilter, afterInstanceID, afterExecutionID string, count int) ([]*diag.WorkflowInstanceRef, error) {
	tx, err := pb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Build the WHERE clause dynamically with positional parameters, assigned in
	// append order ($1, $2, ...).
	var (
		conds []string
		args  []any
	)
	param := func(val any) string {
		args = append(args, val)
		return "$" + strconv.Itoa(len(args))
	}

	// Keyset pagination: rows ordered by (created_at DESC, instance_id DESC); return
	// everything strictly "after" the cursor row. The cursor's created_at is resolved
	// via a subquery so the caller only needs to pass instance_id + execution_id.
	if afterInstanceID != "" {
		cur := param(afterInstanceID)
		curExec := param(afterExecutionID)
		sub := "(SELECT created_at FROM instances WHERE instance_id = " + cur + " AND execution_id = " + curExec + ")"
		conds = append(conds,
			"(i.created_at < "+sub+" OR (i.created_at = "+sub+" AND i.instance_id < "+cur+"))")
	}

	if filter.State != nil {
		conds = append(conds, "i.state = "+param(int(*filter.State)))
	}
	if filter.Queue != "" {
		conds = append(conds, "i.queue = "+param(filter.Queue))
	}
	if filter.WorkflowName != "" {
		conds = append(conds, "i.workflow_name ILIKE "+param("%"+filter.WorkflowName+"%"))
	}
	if filter.FailedOnly {
		conds = append(conds, "i.error IS NOT NULL")
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	limitParam := param(count)
	query := `SELECT ` + diagInstanceColumns + `
		FROM instances i
		` + where + `
		ORDER BY i.created_at DESC, i.instance_id DESC
		LIMIT ` + limitParam

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var instances []*diag.WorkflowInstanceRef
	for rows.Next() {
		ref, err := scanInstanceRef(rows.Scan)
		if err != nil {
			return nil, err
		}
		instances = append(instances, ref)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return instances, nil
}

func (pb *postgresBackend) GetWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance) (*diag.WorkflowInstanceRef, error) {
	tx, err := pb.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res := tx.QueryRowContext(
		ctx,
		`SELECT `+diagInstanceColumns+`
			FROM instances i
			WHERE instance_id = $1 AND execution_id = $2`, instance.InstanceID, instance.ExecutionID)

	ref, err := scanInstanceRef(res.Scan)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return ref, nil
}

func (pb *postgresBackend) GetWorkflowInstancePayloads(ctx context.Context, instance *core.WorkflowInstance) (input, result, errPayload []byte, err error) {
	row := pb.db.QueryRowContext(
		ctx,
		`SELECT input, result, error FROM instances WHERE instance_id = $1 AND execution_id = $2`,
		instance.InstanceID, instance.ExecutionID,
	)

	if err := row.Scan(&input, &result, &errPayload); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, err
	}

	return input, result, errPayload, nil
}

func (pb *postgresBackend) GetWorkflowTree(ctx context.Context, instance *core.WorkflowInstance) (*diag.WorkflowInstanceTree, error) {
	itb := diag.NewInstanceTreeBuilder(pb)
	return itb.BuildWorkflowInstanceTree(ctx, instance)
}
