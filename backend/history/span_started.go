package history

import "github.com/denisaditya0/go-workflows/backend/payload"

type TraceStartedAttributes struct {
	SpanID payload.Payload `json:"spanID"`
}
