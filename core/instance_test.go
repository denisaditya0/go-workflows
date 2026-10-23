package core

import (
	"errors"
	"strings"
	"testing"
)

func Test_ValidateInstanceID(t *testing.T) {
	if err := ValidateInstanceID(""); err == nil {
		t.Fatal("expected error for empty instance ID")
	}

	if err := ValidateInstanceID("order-123"); err != nil {
		t.Fatalf("expected no error for normal instance ID, got %v", err)
	}

	atMax := strings.Repeat("a", MaxInstanceIDLength)
	if err := ValidateInstanceID(atMax); err != nil {
		t.Fatalf("expected no error at max length %d, got %v", MaxInstanceIDLength, err)
	}

	tooLong := strings.Repeat("a", MaxInstanceIDLength+1)
	err := ValidateInstanceID(tooLong)
	if err == nil {
		t.Fatal("expected error for over-length instance ID")
	}
	if !errors.Is(err, ErrInstanceIDTooLong) {
		t.Fatalf("expected ErrInstanceIDTooLong, got %v", err)
	}
}
