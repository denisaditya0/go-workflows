package history

import "github.com/denisaditya0/go-workflows/internal/workflowerrors"

type SubWorkflowFailedAttributes struct {
	Error *workflowerrors.Error `json:"error,omitempty"`
}
