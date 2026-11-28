package scheduler

import (
	"testing"
	"time"
)

func mustParse(t *testing.T, expr string) *cronSchedule {
	t.Helper()
	c, err := parseCron(expr)
	if err != nil {
		t.Fatalf("parseCron(%q): %v", expr, err)
	}
	return c
}

func TestCronNext(t *testing.T) {
	utc := time.UTC

	tests := []struct {
		name string
		expr string
		from time.Time
		want time.Time
	}{
		{
			name: "every minute",
			expr: "* * * * *",
			from: time.Date(2024, 1, 1, 0, 0, 30, 0, utc),
			want: time.Date(2024, 1, 1, 0, 1, 0, 0, utc),
		},
		{
			name: "step",
			expr: "*/5 * * * *",
			from: time.Date(2024, 1, 1, 0, 2, 0, 0, utc),
			want: time.Date(2024, 1, 1, 0, 5, 0, 0, utc),
		},
		{
			name: "weekday range",
			expr: "0 9 * * mon-fri",
			from: time.Date(2024, 1, 6, 10, 0, 0, 0, utc), // Saturday
			want: time.Date(2024, 1, 8, 9, 0, 0, 0, utc),  // Monday
		},
		{
			name: "seconds field",
			expr: "*/15 * * * * *",
			from: time.Date(2024, 1, 1, 0, 0, 7, 0, utc),
			want: time.Date(2024, 1, 1, 0, 0, 15, 0, utc),
		},
		{
			name: "daily macro",
			expr: "@daily",
			from: time.Date(2024, 1, 1, 12, 0, 0, 0, utc),
			want: time.Date(2024, 1, 2, 0, 0, 0, 0, utc),
		},
		{
			name: "every macro",
			expr: "@every 5m",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, utc),
			want: time.Date(2024, 1, 1, 0, 5, 0, 0, utc),
		},
		{
			name: "day-of-month or day-of-week",
			expr: "0 0 13 * fri",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, utc),
			want: time.Date(2024, 1, 5, 0, 0, 0, 0, utc), // Friday before the 13th
		},
		{
			name: "month names",
			expr: "0 0 1 jan *",
			from: time.Date(2024, 6, 1, 0, 0, 0, 0, utc),
			want: time.Date(2025, 1, 1, 0, 0, 0, 0, utc),
		},
		{
			name: "sunday alias 7",
			expr: "0 0 * * 7",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, utc), // Monday
			want: time.Date(2024, 1, 7, 0, 0, 0, 0, utc), // Sunday
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := mustParse(t, tt.expr)
			got := c.next(tt.from)
			if !got.Equal(tt.want) {
				t.Fatalf("next() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCronNeverMatches(t *testing.T) {
	c := mustParse(t, "0 0 31 2 *")
	got := c.next(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	if !got.IsZero() {
		t.Fatalf("expected zero time for impossible expression, got %v", got)
	}
}

func TestCronInvalid(t *testing.T) {
	for _, expr := range []string{
		"",
		"* * * *",
		"* * * * * * *",
		"60 * * * *",
		"* 24 * * *",
		"* * 0 * *",
		"* * * 13 *",
		"*/0 * * * *",
		"@unknown",
		"@every",
		"@every nope",
	} {
		if err := validateCron(expr); err == nil {
			t.Errorf("expected error for %q", expr)
		}
	}
}

func TestCronLocale(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("timezone data unavailable: %v", err)
	}

	c := mustParse(t, "0 9 * * *")
	from := time.Date(2024, 1, 1, 10, 0, 0, 0, loc)
	got := c.next(from)
	want := time.Date(2024, 1, 2, 9, 0, 0, 0, loc)
	if !got.Equal(want) {
		t.Fatalf("next() = %v, want %v", got, want)
	}
}
