package history

import (
	"github.com/denisaditya0/go-workflows/backend/metadata"
	"github.com/denisaditya0/go-workflows/backend/payload"
	"github.com/denisaditya0/go-workflows/core"
)

type SubWorkflowScheduledAttributes struct {
	SubWorkflowQueue core.Queue `json:"sub_workflow_queue,omitempty"`

	SubWorkflowInstance *core.WorkflowInstance `json:"sub_workflow_instance,omitempty"`

	Name string `json:"name,omitempty"`

	Inputs []payload.Payload `json:"inputs,omitempty"`

	Metadata *metadata.WorkflowMetadata `json:"metadata,omitempty"`
}
