package converter

import (
	"bytes"
	"encoding/json"

	"github.com/cschleiden/go-workflows/backend/payload"
)

type jsonConverter struct{}

var _ Converter = (*jsonConverter)(nil)

func (jc *jsonConverter) To(v interface{}) (payload.Payload, error) {
	return json.Marshal(v)
}

func (jc *jsonConverter) From(data payload.Payload, vptr interface{}) error {
	// Use a Decoder with UseNumber so that JSON numbers decoded into an interface{}
	// target are kept as json.Number (a string) instead of being coerced to float64,
	// which would lose precision for int64 values larger than 2^53. Decoding into a
	// concrete numeric type (e.g. int64) is unaffected and remains lossless.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(vptr)
}
