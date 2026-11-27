package diag

import (
	"context"
	"time"

	"github.com/denisaditya0/go-workflows/backend"
	"github.com/denisaditya0/go-workflows/core"
)

// json: serialization in this file needs to be kept in sync with client.ts in the web app

type WorkflowInstanceRef struct {
	Instance    *core.WorkflowInstance     `json:"instance,omitempty"`
	CreatedAt   time.Time                  `json:"created_at,omitempty"`
	CompletedAt *time.Time                 `json:"completed_at,omitempty"`
	State       core.WorkflowInstanceState `json:"state"`
	Queue       string                     `json:"queue"`

	// Promoted columns (fork-specific denormalization on the instances row). These
	// let list/detail views show the workflow name and real outcome without decoding
	// the full history blob. All are best-effort: older rows written before the
	// promotion migration may leave them empty/nil.

	// WorkflowName is the registered workflow name (instances.workflow_name).
	WorkflowName string `json:"workflow_name,omitempty"`

	// HasError is true when the instance finished with an error
	// (instances.error IS NOT NULL). This distinguishes Failed from Completed for a
	// finished instance (fixes #469 in the list/tree without reading history).
	HasError bool `json:"has_error,omitempty"`

	// PreviousExecutionID is the predecessor execution for a ContinueAsNew'd instance
	// (instances.previous_execution_id), or empty for a first/original run.
	PreviousExecutionID string `json:"previous_execution_id,omitempty"`
}

type Event struct {
	ID              string      `json:"id,omitempty"`
	SequenceID      int64       `json:"sequence_id,omitempty"`
	Type            string      `json:"type,omitempty"`
	Timestamp       time.Time   `json:"timestamp,omitempty"`
	ScheduleEventID int64       `json:"schedule_event_id,omitempty"`
	Attributes      interface{} `json:"attributes,omitempty"`
	VisibleAt       *time.Time  `json:"visible_at,omitempty"`
}

type WorkflowInstanceInfo struct {
	*WorkflowInstanceRef

	// Promoted payloads for the detail view, read from the instances columns so the
	// headline input/result/error don't require decoding the full history. Nil when
	// not set (e.g. result/error before completion, or rows predating promotion).
	Input  []byte `json:"input,omitempty"`
	Result []byte `json:"result,omitempty"`
	Error  []byte `json:"error,omitempty"`

	History []*Event `json:"history,omitempty"`
}

type WorkflowInstanceTree struct {
	*WorkflowInstanceRef

	WorkflowName string `json:"workflow_name,omitempty"`
	Error        bool   `json:"error,omitempty"`

	Children []*WorkflowInstanceTree `json:"children,omitempty"`
}

type Backend interface {
	backend.Backend

	GetWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance) (*WorkflowInstanceRef, error)
	GetWorkflowInstances(ctx context.Context, afterInstanceID, afterExecutionID string, count int) ([]*WorkflowInstanceRef, error)
	GetWorkflowTree(ctx context.Context, instance *core.WorkflowInstance) (*WorkflowInstanceTree, error)

	// GetWorkflowInstancePayloads returns the promoted input/result/error payloads
	// for an instance (from the instances columns), so the detail view can show them
	// without decoding history. Any may be nil.
	GetWorkflowInstancePayloads(ctx context.Context, instance *core.WorkflowInstance) (input, result, errPayload []byte, err error)

	// GetWorkflowInstancesFiltered is like GetWorkflowInstances but applies the given
	// filter (state, queue, workflow-name substring). An empty filter behaves exactly
	// like GetWorkflowInstances. Filters leverage the promoted columns so no history
	// decode is needed.
	GetWorkflowInstancesFiltered(ctx context.Context, filter InstanceFilter, afterInstanceID, afterExecutionID string, count int) ([]*WorkflowInstanceRef, error)
}

// InstanceFilter narrows the instance list. Zero value = no filtering.
type InstanceFilter struct {
	// State, when non-nil, restricts to that workflow instance state
	// (Active / ContinuedAsNew / Finished).
	State *core.WorkflowInstanceState

	// Queue, when non-empty, restricts to instances on that queue (exact match).
	Queue string

	// WorkflowName, when non-empty, restricts to instances whose promoted
	// workflow_name contains this substring (case-insensitive).
	WorkflowName string

	// FailedOnly, when true, restricts to instances that finished with an error
	// (error column IS NOT NULL). Combine with State=Finished for "failed runs".
	FailedOnly bool
}

// IsZero reports whether the filter would narrow nothing.
func (f InstanceFilter) IsZero() bool {
	return f.State == nil && f.Queue == "" && f.WorkflowName == "" && !f.FailedOnly
}
