package templ

import (
	"embed"
	"html/template"
	"io"

	"github.com/denisaditya0/go-workflows/backend"
	"github.com/denisaditya0/go-workflows/backend/history"
	"github.com/denisaditya0/go-workflows/diag"
)

//go:embed templates/*.html
var templatesFS embed.FS

// funcMap exposes the pure helpers to the templates.
var funcMap = template.FuncMap{
	"dict":                 dict,
	"instanceLink":         instanceLink,
	"fmtTime":              fmtTime,
	"fmtTimePtr":           fmtTimePtr,
	"stateBadge":           instanceStateBadge,
	"eventColor":           eventColor,
	"scheduleEventIDColor": scheduleEventIDColor,
	"formatAttributes":     formatAttributes,
	"add":                  func(a, b int) int { return a + b },
}

// homeTmpl and instanceTmpl are two independent template sets. Each contains the
// shared layout plus its own "content" definition (html/template requires unique
// template names within a set, so the two "content" definitions must live in
// separate sets).
var (
	homeTmpl     = template.Must(template.New("home").Funcs(funcMap).ParseFS(templatesFS, "templates/layout.html", "templates/home.html"))
	instanceTmpl = template.Must(template.New("instance").Funcs(funcMap).ParseFS(templatesFS, "templates/layout.html", "templates/instance.html"))
)

// ---- View models ----

type homeData struct {
	Title     string
	BasePath  string
	Instances []*diag.WorkflowInstanceRef
	Stats     *backend.Stats
	Count     int
	Page      int
	NextAfter string // "instanceID:executionID" or "" if no next page

	// Active filter values (for pre-filling the form) and their URL-encoded form
	// (for carrying through pagination links).
	FilterState string
	FilterQueue string
	FilterName  string
	FilterQuery string
}

type instanceData struct {
	Title        string
	BasePath     string
	Info         *diag.WorkflowInstanceInfo
	History      []*diag.Event
	Tree         *diag.WorkflowInstanceTree
	WorkflowName string
	Inputs       []string // decoded payloads
	Result       string   // decoded result payload, if any
	ErrorJSON    string   // pretty-printed error, if any
	HasError     bool
}

// renderHome executes the home template set against the layout.
func renderHome(w io.Writer, d homeData) error {
	return homeTmpl.ExecuteTemplate(w, "layout", d)
}

// renderInstance executes the instance template set against the layout.
func renderInstance(w io.Writer, d instanceData) error {
	return instanceTmpl.ExecuteTemplate(w, "layout", d)
}

// extractInstanceDetails scans history for the started/finished events and
// decodes input/result/error, mirroring Instance.tsx. It also returns the real
// outcome (hasError) which fixes the #469 "Failed shown as Completed" gap for
// the detail view.
func extractInstanceDetails(h []*diag.Event) (name string, inputs []string, result string, errJSON string, hasError bool) {
	for _, ev := range h {
		switch ev.Type {
		case history.EventType_WorkflowExecutionStarted.String():
			if a, ok := ev.Attributes.(*history.ExecutionStartedAttributes); ok {
				name = a.Name
				for _, in := range a.Inputs {
					inputs = append(inputs, decodePayloadBytes(in))
				}
			}
		case history.EventType_WorkflowExecutionFinished.String():
			if a, ok := ev.Attributes.(*history.ExecutionCompletedAttributes); ok {
				if a.Error != nil {
					hasError = true
					errJSON = formatAttributes(a.Error)
				} else if len(a.Result) > 0 {
					result = decodePayloadBytes(a.Result)
				}
			}
		case history.EventType_WorkflowExecutionContinuedAsNew.String():
			if a, ok := ev.Attributes.(*history.ExecutionContinuedAsNewAttributes); ok && len(a.Result) > 0 {
				result = decodePayloadBytes(a.Result)
			}
		}
	}
	return
}
