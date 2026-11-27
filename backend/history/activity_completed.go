package history

import "github.com/denisaditya0/go-workflows/backend/payload"

type ActivityCompletedAttributes struct {
	Result payload.Payload `json:"result,omitempty"`
}
