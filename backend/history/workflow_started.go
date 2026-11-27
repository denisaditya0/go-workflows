package history

import (
	"github.com/denisaditya0/go-workflows/backend/metadata"
	"github.com/denisaditya0/go-workflows/backend/payload"
	"github.com/denisaditya0/go-workflows/core"
)

type ExecutionStartedAttributes struct {
	Queue core.Queue `json:"queue,omitempty"`

	Name string `json:"name,omitempty"`

	Metadata *metadata.WorkflowMetadata `json:"metadata,omitempty"`

	Inputs []payload.Payload `json:"inputs,omitempty"`
}
