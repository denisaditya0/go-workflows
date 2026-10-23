package core

import "testing"

func Test_ValidQueue(t *testing.T) {
	valid := []Queue{
		QueueDefault, // "default"
		QueueSystem,  // "_system_"
		"custom",
		"custom-activity-queue",
		"abcd",                             // min length 4
		Queue(repeat("a", 31)),             // max length 31
	}
	for _, q := range valid {
		if err := ValidQueue(q); err != nil {
			t.Errorf("expected %q to be valid, got %v", q, err)
		}
	}

	invalid := []Queue{
		"",                       // empty
		"abc",                    // too short (<4)
		Queue(repeat("a", 32)),   // too long (>31)
		"has space",              // illegal char
		"has.dot",                // illegal char
	}
	for _, q := range invalid {
		if err := ValidQueue(q); err == nil {
			t.Errorf("expected %q to be invalid", q)
		}
	}
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}
