package scheduler

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// maxSearchYears bounds how far ahead the cron matcher will look for the next
// occurrence. An expression that can never match (e.g. February 30th) is treated
// as invalid at runtime rather than looping forever.
const maxSearchYears = 5

// cronSchedule is a parsed cron expression. It supports the standard 5-field
// format (minute hour day-of-month month day-of-week), an optional leading
// seconds field (6 fields), and the common @macros. The parser is dependency-free
// so the engine keeps a zero-dependency footprint.
type cronSchedule struct {
	seconds  set
	minutes  set
	hours    set
	dom      set
	months   set
	dow      set
	domAny   bool
	dowAny   bool
	hasSecs  bool
	every    time.Duration // set for @every, disables field matching
	original string
}

// set is a bit set over small non-negative integers (max 63).
type set uint64

func (s set) has(v int) bool {
	if v < 0 || v > 63 {
		return false
	}
	return s&(set(1)<<uint(v)) != 0
}

func (s *set) add(v int) {
	*s |= set(1) << uint(v)
}

func (s *set) addRange(lo, hi, step int) {
	if step <= 0 {
		return
	}
	for v := lo; v <= hi; v += step {
		s.add(v)
	}
}

// parseCron parses a cron expression or @macro. When seconds are omitted, fires
// occur at second 0.
func parseCron(expr string) (*cronSchedule, error) {
	original := strings.TrimSpace(expr)
	if original == "" {
		return nil, fmt.Errorf("cron expression is empty")
	}

	lower := strings.ToLower(original)
	if strings.HasPrefix(lower, "@") {
		return parseMacro(original, lower)
	}

	fields := strings.Fields(original)
	if len(fields) != 5 && len(fields) != 6 {
		return nil, fmt.Errorf("cron expression %q must have 5 or 6 fields, got %d", original, len(fields))
	}

	c := &cronSchedule{original: original}

	idx := 0
	if len(fields) == 6 {
		c.hasSecs = true
		secs, err := parseField(fields[0], 0, 59, nil)
		if err != nil {
			return nil, fmt.Errorf("seconds: %w", err)
		}
		c.seconds = secs
		idx = 1
	} else {
		c.seconds.add(0)
	}

	var err error
	if c.minutes, err = parseField(fields[idx], 0, 59, nil); err != nil {
		return nil, fmt.Errorf("minutes: %w", err)
	}

	if c.hours, err = parseField(fields[idx+1], 0, 23, nil); err != nil {
		return nil, fmt.Errorf("hours: %w", err)
	}

	c.dom, c.domAny, err = parseFieldWithAny(fields[idx+2], 1, 31, nil)
	if err != nil {
		return nil, fmt.Errorf("day of month: %w", err)
	}

	if c.months, err = parseField(fields[idx+3], 1, 12, monthNames); err != nil {
		return nil, fmt.Errorf("month: %w", err)
	}

	c.dow, c.dowAny, err = parseFieldWithAny(fields[idx+4], 0, 7, dayNames)
	if err != nil {
		return nil, fmt.Errorf("day of week: %w", err)
	}

	// Alias 7 to 0 (both are Sunday).
	if c.dow.has(7) {
		c.dow.add(0)
	}

	return c, nil
}

func parseMacro(original, lower string) (*cronSchedule, error) {
	fields := strings.Fields(lower)
	macro := fields[0]

	if macro == "@every" {
		if len(fields) != 2 {
			return nil, fmt.Errorf("cron macro @every requires a duration, e.g. @every 5m")
		}
		d, err := time.ParseDuration(fields[1])
		if err != nil {
			return nil, fmt.Errorf("parsing @every duration: %w", err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("@every duration must be positive")
		}
		return &cronSchedule{every: d, original: original}, nil
	}

	var standard string
	switch macro {
	case "@yearly", "@annually":
		standard = "0 0 1 1 *"
	case "@monthly":
		standard = "0 0 1 * *"
	case "@weekly":
		standard = "0 0 * * 0"
	case "@daily", "@midnight":
		standard = "0 0 * * *"
	case "@hourly":
		standard = "0 * * * *"
	default:
		return nil, fmt.Errorf("unknown cron macro %q", macro)
	}

	c, err := parseCron(standard)
	if err != nil {
		return nil, err
	}
	c.original = original
	return c, nil
}

func parseField(field string, min, max int, names map[string]int) (set, error) {
	s, _, err := parseFieldWithAny(field, min, max, names)
	return s, err
}

// parseFieldWithAny parses one cron field. It reports whether the field was the
// wildcard "*" (or "?"), which drives the day-of-month / day-of-week OR rule.
func parseFieldWithAny(field string, min, max int, names map[string]int) (set, bool, error) {
	field = strings.ToLower(strings.TrimSpace(field))
	if field == "*" || field == "?" {
		var s set
		s.addRange(min, max, 1)
		return s, true, nil
	}

	var s set
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return 0, false, fmt.Errorf("empty list element")
		}

		step := 1
		base := part
		if slash := strings.IndexByte(part, '/'); slash >= 0 {
			base = part[:slash]
			rawStep := part[slash+1:]
			n, err := strconv.Atoi(rawStep)
			if err != nil || n <= 0 {
				return 0, false, fmt.Errorf("invalid step %q", rawStep)
			}
			step = n
		}

		var lo, hi int
		switch {
		case base == "*" || base == "?":
			lo, hi = min, max
		case strings.Contains(base, "-"):
			bounds := strings.SplitN(base, "-", 2)
			var err error
			if lo, err = parseValue(bounds[0], names); err != nil {
				return 0, false, err
			}
			if hi, err = parseValue(bounds[1], names); err != nil {
				return 0, false, err
			}
			if lo > hi {
				return 0, false, fmt.Errorf("invalid range %q", base)
			}
		default:
			v, err := parseValue(base, names)
			if err != nil {
				return 0, false, err
			}
			lo, hi = v, v
		}

		if lo < min || hi > max {
			return 0, false, fmt.Errorf("value out of range [%d,%d]: %q", min, max, part)
		}

		s.addRange(lo, hi, step)
	}

	return s, false, nil
}

func parseValue(raw string, names map[string]int) (int, error) {
	raw = strings.TrimSpace(raw)
	if names != nil {
		if v, ok := names[raw]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", raw)
	}
	return v, nil
}

var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var dayNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// validateCron reports whether expr is a valid cron expression.
func validateCron(expr string) error {
	_, err := parseCron(expr)
	return err
}

// next returns the first fire time strictly after t. For @every schedules it is
// simply t + interval. The returned time uses t's location. A zero time is
// returned if no occurrence exists within maxSearchYears.
func (c *cronSchedule) next(t time.Time) time.Time {
	if c.every > 0 {
		return t.Add(c.every)
	}

	loc := t.Location()
	var cursor time.Time
	if c.hasSecs {
		cursor = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, loc).Add(time.Second)
	} else {
		cursor = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, loc).Add(time.Minute)
	}

	limit := cursor.AddDate(maxSearchYears, 0, 0)
	for cursor.Before(limit) {
		if !c.months.has(int(cursor.Month())) {
			cursor = time.Date(cursor.Year(), cursor.Month()+1, 1, 0, 0, 0, 0, loc)
			continue
		}

		if !c.dayMatches(cursor) {
			cursor = time.Date(cursor.Year(), cursor.Month(), cursor.Day()+1, 0, 0, 0, 0, loc)
			continue
		}

		if !c.hours.has(cursor.Hour()) {
			cursor = time.Date(cursor.Year(), cursor.Month(), cursor.Day(), cursor.Hour()+1, 0, 0, 0, loc)
			continue
		}

		if !c.minutes.has(cursor.Minute()) {
			cursor = time.Date(cursor.Year(), cursor.Month(), cursor.Day(), cursor.Hour(), cursor.Minute()+1, 0, 0, loc)
			continue
		}

		if c.hasSecs && !c.seconds.has(cursor.Second()) {
			cursor = cursor.Add(time.Second)
			continue
		}

		return cursor
	}

	return time.Time{}
}

// dayMatches applies the standard cron rule: when both day-of-month and
// day-of-week are restricted, a day matches if either matches.
func (c *cronSchedule) dayMatches(t time.Time) bool {
	domOK := c.dom.has(t.Day())
	dowOK := c.dow.has(int(t.Weekday()))
	if !c.domAny && !c.dowAny {
		return domOK || dowOK
	}
	return domOK && dowOK
}
