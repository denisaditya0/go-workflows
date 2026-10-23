package core

import (
	"errors"
	"fmt"
)

// MaxInstanceIDLength is the maximum allowed length of a workflow InstanceID. It
// matches the instance_id column width in the Postgres schema (varchar(256)).
const MaxInstanceIDLength = 256

// ErrInstanceIDTooLong is returned when a provided InstanceID exceeds MaxInstanceIDLength.
var ErrInstanceIDTooLong = errors.New("instance ID too long")

// ValidateInstanceID checks that an InstanceID is non-empty and within the storage
// length bound. It returns a clear error instead of letting an oversize value fail
// later at the database.
func ValidateInstanceID(instanceID string) error {
	if instanceID == "" {
		return errors.New("instance ID must not be empty")
	}
	if len(instanceID) > MaxInstanceIDLength {
		return fmt.Errorf("%w: %d > %d", ErrInstanceIDTooLong, len(instanceID), MaxInstanceIDLength)
	}
	return nil
}

type WorkflowInstance struct {
	// InstanceID is the ID of the workflow instance.
	InstanceID string `json:"instance_id,omitempty"`

	// ExecutionID is the ID of the current execution of the workflow instance.
	ExecutionID string `json:"execution_id,omitempty"`

	// Parent refers to the parent workflow instance if this instance is a sub-workflow.
	Parent *WorkflowInstance `json:"parent,omitempty"`

	// ParentEventID is the ID of the event in the parent workflow that started this sub-workflow.
	ParentEventID int64 `json:"parent_event_id,omitempty"`
}

func NewWorkflowInstance(instanceID, executionID string) *WorkflowInstance {
	return &WorkflowInstance{
		InstanceID:  instanceID,
		ExecutionID: executionID,
	}
}

func NewSubWorkflowInstance(instanceID, executionID string, parentInstance *WorkflowInstance, parentEventID int64) *WorkflowInstance {
	return &WorkflowInstance{
		InstanceID:    instanceID,
		ExecutionID:   executionID,
		Parent:        parentInstance,
		ParentEventID: parentEventID,
	}
}

func (wi *WorkflowInstance) SubWorkflow() bool {
	return wi.Parent != nil
}
