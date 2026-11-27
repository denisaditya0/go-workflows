package history

import "github.com/denisaditya0/go-workflows/backend/payload"

type SideEffectResultAttributes struct {
	Result payload.Payload `json:"result,omitempty"`
}
