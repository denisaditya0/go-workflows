package history

import (
	"github.com/denisaditya0/go-workflows/backend/payload"
	"github.com/denisaditya0/go-workflows/internal/workflowerrors"
)

type ExecutionCompletedAttributes struct {
	Result payload.Payload       `json:"result,omitempty"`
	Error  *workflowerrors.Error `json:"error,omitempty"`
}
