package history

import "github.com/denisaditya0/go-workflows/internal/workflowerrors"

type ActivityFailedAttributes struct {
	Error *workflowerrors.Error `json:"error,omitempty"`
}
