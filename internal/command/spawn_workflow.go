package command

import (
	"github.com/benbjohnson/clock"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/backend/metadata"
	"github.com/cschleiden/go-workflows/backend/payload"
	"github.com/cschleiden/go-workflows/core"
	"github.com/google/uuid"
)

// SpawnWorkflowCommand launches a new, detached, top-level workflow instance from
// within a running workflow. Unlike ScheduleSubWorkflowCommand it does not create a
// parent/child relationship and it does not record any event on the calling workflow's
// history: it only emits the WorkflowExecutionStarted event to the new instance.
//
// Because no future is tracked for the spawned workflow, the caller cannot await its
// result and the parent will not trigger the pending-futures check on completion.
type SpawnWorkflowCommand struct {
	command

	Queue          core.Queue
	Instance       *core.WorkflowInstance
	Metadata       *metadata.WorkflowMetadata
	WorkflowSpanID [8]byte

	Name   string
	Inputs []payload.Payload
}

var _ Command = (*SpawnWorkflowCommand)(nil)

// NewSpawnWorkflowCommand creates a command that starts a new top-level workflow
// instance. instanceID must be non-empty; validation is expected to happen in the
// public workflow.SpawnWorkflow API before this command is created.
func NewSpawnWorkflowCommand(
	id int64, spawnQueue core.Queue, instanceID string,
	name string, inputs []payload.Payload, metadata *metadata.WorkflowMetadata, workflowSpanID [8]byte,
) *SpawnWorkflowCommand {
	return &SpawnWorkflowCommand{
		command: command{
			id:    id,
			name:  "SpawnWorkflow",
			state: CommandState_Pending,
		},

		Queue: spawnQueue,
		// Top-level instance: no parent, fresh execution id captured in history.
		Instance:       core.NewWorkflowInstance(instanceID, uuid.NewString()),
		Metadata:       metadata,
		WorkflowSpanID: workflowSpanID,

		Name:   name,
		Inputs: inputs,
	}
}

func (c *SpawnWorkflowCommand) Execute(clock clock.Clock) *CommandResult {
	switch c.state {
	case CommandState_Pending:
		c.state = CommandState_Committed

		return &CommandResult{
			// Intentionally no parent-side Events: the spawned workflow is detached and
			// top-level, so nothing is recorded on the calling workflow's history.
			WorkflowEvents: []*history.WorkflowEvent{
				{
					WorkflowInstance: c.Instance,
					HistoryEvent: history.NewPendingEvent(
						clock.Now(),
						history.EventType_WorkflowExecutionStarted,
						&history.ExecutionStartedAttributes{
							Queue:          c.Queue,
							Name:           c.Name,
							Inputs:         c.Inputs,
							Metadata:       c.Metadata,
							WorkflowSpanID: c.WorkflowSpanID,
						},
					),
				},
			},
		}
	}

	return nil
}
