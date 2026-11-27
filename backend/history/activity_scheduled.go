package history

import (
	"github.com/denisaditya0/go-workflows/backend/metadata"
	"github.com/denisaditya0/go-workflows/backend/payload"
	"github.com/denisaditya0/go-workflows/core"
)

type ActivityScheduledAttributes struct {
	Name string `json:"name,omitempty"`

	Attempt int `json:"attempt,omitempty"`

	Inputs []payload.Payload `json:"inputs,omitempty"`

	Metadata *metadata.WorkflowMetadata `json:"metadata,omitempty"`

	Queue core.Queue `json:"queue,omitempty"`
}
