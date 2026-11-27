// Package templ provides a server-rendered diagnostics UI built with Go's
// standard html/template and real .html template files (embedded via go:embed).
// diag.NewServeMux only exposes the JSON API; this package provides the UI.
//
// The UI renders the same data exposed by diag.Backend (instance list, instance
// detail + history, sub-workflow tree, and stats); the data layer in
// backend/postgres is untouched. This file holds the small, pure helpers used by
// the templates (payload decoding, state/event/schedule-id coloring) wired into
// the template FuncMap.
package templ

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/denisaditya0/go-workflows/core"
)

// StateBadge describes how a workflow instance state is rendered as a badge.
type StateBadge struct {
	Label string
	// Bg is a Bootstrap background class suffix (e.g. "info", "success").
	Bg string
	// Text is a Bootstrap text class suffix (e.g. "dark", "light").
	Text string
}

// EventColor is the text/background color pair for an event-type badge.
type EventColor struct {
	Text string
	Bg   string
}

// ScheduleColor is the text color suffix + background CSS color for a schedule
// event id badge.
type ScheduleColor struct {
	Text string
	BG   string
}

// instanceStateBadge renders the workflow instance state as a badge.
//
// Active and ContinuedAsNew are derived purely from the state value. For a
// finished instance the UI distinguishes Failed vs Completed based on whether
// the final event carried an error (hasError). This is the #469 work-around: the
// current diag SQL only sets Finished from completed_at and cannot by itself tell
// failed from succeeded, so callers pass hasError derived from the final history
// event.
func instanceStateBadge(state core.WorkflowInstanceState, hasError bool) StateBadge {
	switch state {
	case core.WorkflowInstanceStateActive:
		return StateBadge{Label: "Active", Bg: "info", Text: "light"}
	case core.WorkflowInstanceStateContinuedAsNew:
		return StateBadge{Label: "ContinuedAsNew", Bg: "light", Text: "dark"}
	default:
		if hasError {
			return StateBadge{Label: "Failed", Bg: "danger", Text: "light"}
		}
		return StateBadge{Label: "Completed", Bg: "success", Text: "light"}
	}
}

// eventColor returns the badge colors for an event type.
func eventColor(event string) EventColor {
	switch event {
	case "SubWorkflowScheduled", "SubWorkflowCancellationRequested",
		"SubWorkflowCompleted", "SubWorkflowFailed":
		return EventColor{Text: "light", Bg: "success"}
	case "ActivityScheduled", "ActivityCompleted", "ActivityFailed":
		return EventColor{Text: "dark", Bg: "warning"}
	case "TimerScheduled", "TimerFired", "TimerCanceled":
		return EventColor{Text: "light", Bg: "primary"}
	case "SignalReceived":
		return EventColor{Text: "light", Bg: "dark"}
	case "SideEffectResult":
		return EventColor{Text: "dark", Bg: "secondary"}
	case "WorkflowTaskStarted":
		return EventColor{Text: "dark", Bg: "light"}
	case "WorkflowExecutionStarted":
		return EventColor{Text: "dark", Bg: "info"}
	case "WorkflowExecutionFinished":
		return EventColor{Text: "light", Bg: "success"}
	case "WorkflowExecutionContinuedAsNew":
		return EventColor{Text: "dark", Bg: "light"}
	default:
		return EventColor{Text: "dark", Bg: "light"}
	}
}

// scheduleEventIDColors is the palette used for schedule event ids.
var scheduleEventIDColors = []ScheduleColor{
	{"light", "#0d6efd"},
	{"light", "#6f42c1"},
	{"light", "#d63384"},
	{"light", "#ffc107"},
	{"light", "#dc3545"},
	{"light", "#6610f2"},
	{"light", "#fd7e14"},
	{"light", "#198754"},
	{"light", "#20c997"},
	{"light", "#0dcaf0"},
}

// scheduleEventIDColor cycles through the event id palette.
func scheduleEventIDColor(id int64) ScheduleColor {
	if id <= 0 {
		return scheduleEventIDColors[0]
	}
	return scheduleEventIDColors[(id-1)%int64(len(scheduleEventIDColors))]
}

// decodePayloadBytes renders already-decoded payload bytes: pretty-print if the
// bytes are valid JSON, otherwise return them verbatim as a string. The engine's
// payloads are JSON bytes (converter.DefaultConverter), so this is the common case.
func decodePayloadBytes(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}

	var js any
	if err := json.Unmarshal(raw, &js); err == nil {
		pretty, err := json.MarshalIndent(js, "", "  ")
		if err == nil {
			return string(pretty)
		}
	}

	return string(raw)
}

// formatAttributes renders an event's attributes (a history.*Attributes struct)
// as indented JSON. Payload fields appear base64-encoded; the dedicated
// Input/Result panels decode them explicitly.
func formatAttributes(attributes any) string {
	if attributes == nil {
		return ""
	}

	pretty, err := json.MarshalIndent(attributes, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", attributes)
	}

	return strings.ReplaceAll(string(pretty), "\\t", "    ")
}

// instanceLink builds an absolute link to an instance detail page from the mount
// base path (e.g. "/templ") + instance/execution ids.
func instanceLink(basePath, instanceID, executionID string) string {
	return basePath + "/" + instanceID + "/" + executionID
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func fmtTimePtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return fmtTime(*t)
}

// dict builds a map from alternating key/value pairs for passing multiple named
// values into a sub-template (html/template templates take a single arg).
func dict(values ...any) (map[string]any, error) {
	if len(values)%2 != 0 {
		return nil, fmt.Errorf("dict: odd number of arguments (%d)", len(values))
	}
	m := make(map[string]any, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			return nil, fmt.Errorf("dict: key %d is not a string", i)
		}
		m[key] = values[i+1]
	}
	return m, nil
}
