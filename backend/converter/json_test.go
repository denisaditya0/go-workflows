package converter

import (
	"encoding/json"
	"testing"
)

// Test_JSON_RoundTrip_LargeInt64_Typed verifies that a large int64 (> 2^53)
// round-trips losslessly when decoded into a typed int64 target. This is the safe
// path (json.Unmarshal into an int target does not go through float64).
func Test_JSON_RoundTrip_LargeInt64_Typed(t *testing.T) {
	c := &jsonConverter{}

	const want int64 = 9_007_199_254_740_993 // 2^53 + 1, not representable as float64

	p, err := c.To(want)
	if err != nil {
		t.Fatal(err)
	}

	var got int64
	if err := c.From(p, &got); err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Fatalf("typed round-trip lost precision: want %d, got %d", want, got)
	}
}

// Test_JSON_RoundTrip_LargeInt64_Any verifies that decoding a large int64 (> 2^53)
// into an interface{} target preserves precision. The converter uses
// json.Decoder.UseNumber, so numbers decode into json.Number (not float64), keeping
// full int64 precision. This is the fix for cschleiden/go-workflows#471.
func Test_JSON_RoundTrip_LargeInt64_Any(t *testing.T) {
	c := &jsonConverter{}

	const want int64 = 9_007_199_254_740_993 // 2^53 + 1

	p, err := c.To(want)
	if err != nil {
		t.Fatal(err)
	}

	var got any
	if err := c.From(p, &got); err != nil {
		t.Fatal(err)
	}

	num, ok := got.(json.Number)
	if !ok {
		t.Fatalf("expected json.Number from any decode (UseNumber), got %T = %v", got, got)
	}

	gotInt, err := num.Int64()
	if err != nil {
		t.Fatalf("json.Number.Int64() failed: %v", err)
	}
	if gotInt != want {
		t.Fatalf("any round-trip lost precision: want %d, got %d", want, gotInt)
	}
}
