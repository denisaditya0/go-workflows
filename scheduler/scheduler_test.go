package scheduler

import (
	"testing"
	"time"

	"github.com/denisaditya0/go-workflows/backend"
)

func newTestScheduler() *Scheduler {
	return &Scheduler{
		crons: make(map[string]*cronSchedule),
		locs:  make(map[string]*time.Location),
	}
}

func TestNextRunCatchupNone(t *testing.T) {
	s := newTestScheduler()
	fire := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2024, 1, 1, 0, 5, 30, 0, time.UTC)

	schedule := &backend.Schedule{
		Cron:     "* * * * *",
		TimeZone: "UTC",
		Catchup:  backend.ScheduleCatchupNone,
	}

	got, err := s.nextRun(schedule, fire, now)
	if err != nil {
		t.Fatal(err)
	}

	want := time.Date(2024, 1, 1, 0, 6, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("next = %v, want %v", got, want)
	}
}

func TestNextRunCatchupAll(t *testing.T) {
	s := newTestScheduler()
	fire := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2024, 1, 1, 0, 5, 30, 0, time.UTC)

	schedule := &backend.Schedule{
		Cron:     "* * * * *",
		TimeZone: "UTC",
		Catchup:  backend.ScheduleCatchupAll,
	}

	got, err := s.nextRun(schedule, fire, now)
	if err != nil {
		t.Fatal(err)
	}

	want := time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("next = %v, want %v", got, want)
	}
}

func TestNextRunInterval(t *testing.T) {
	s := newTestScheduler()
	fire := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2024, 1, 1, 0, 3, 30, 0, time.UTC)

	none := &backend.Schedule{Interval: time.Minute, Catchup: backend.ScheduleCatchupNone}
	got, err := s.nextRun(none, fire, now)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2024, 1, 1, 0, 4, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("none: next = %v, want %v", got, want)
	}

	all := &backend.Schedule{Interval: time.Minute, Catchup: backend.ScheduleCatchupAll}
	got, err = s.nextRun(all, fire, now)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("all: next = %v, want %v", got, want)
	}
}

func TestScheduledInstanceID(t *testing.T) {
	fire := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	a := scheduledInstanceID("nightly", fire)
	b := scheduledInstanceID("nightly", fire)
	c := scheduledInstanceID("nightly", fire.Add(time.Minute))

	if a != b {
		t.Fatalf("instance ID is not deterministic: %q vs %q", a, b)
	}
	if a == c {
		t.Fatalf("instance ID should differ across fire times: %q", a)
	}

	longID := make([]byte, MaxScheduleIDLength)
	for i := range longID {
		longID[i] = 'a'
	}
	got := scheduledInstanceID(string(longID), fire)
	if len(got) > 256 {
		t.Fatalf("instance ID length %d exceeds 256", len(got))
	}
}

func TestFirstRun(t *testing.T) {
	s := newTestScheduler()
	now := time.Date(2024, 1, 1, 0, 2, 30, 0, time.UTC)

	got, err := s.firstRun("* * * * *", 0, "UTC", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2024, 1, 1, 0, 3, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("cron first run = %v, want %v", got, want)
	}

	got, err = s.firstRun("", 10*time.Minute, "UTC", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if want := now.Add(10 * time.Minute); !got.Equal(want) {
		t.Fatalf("interval first run = %v, want %v", got, want)
	}

	startAt := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	got, err = s.firstRun("0 0 * * *", 0, "UTC", &startAt, now)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(startAt) {
		t.Fatalf("startAt first run = %v, want %v", got, startAt)
	}
}
