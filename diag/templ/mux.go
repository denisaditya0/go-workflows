package templ

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/diag"
)

// NewServeMux returns an *http.ServeMux that serves a server-rendered diagnostics
// UI built with Go's html/template and real .html files (see templates/). It
// reads from the diag.Backend data source; no JSON API is exposed because pages
// are rendered on the server. Use diag.NewServeMux if you need the JSON API.
//
// Routes:
//
//	GET /                              instance list + queue stats (paginated)
//	GET /{instanceID}/{executionID}    instance detail + history + tree
//
// Usage:
//
//	mux := http.NewServeMux()
//	mux.Handle("/diag/", http.StripPrefix("/diag", templ.NewServeMux(b)))
func NewServeMux(backend diag.Backend) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		path := strings.Trim(r.URL.Path, "/")
		if path == "" {
			handleHome(w, r, backend)
			return
		}

		segments := strings.Split(path, "/")
		if len(segments) == 2 {
			handleInstance(w, r, backend, segments[0], segments[1])
			return
		}

		http.NotFound(w, r)
	})

	return mux
}

func handleHome(w http.ResponseWriter, r *http.Request, b diag.Backend) {
	q := r.URL.Query()

	count := 20
	if c := q.Get("count"); c != "" {
		if n, err := strconv.Atoi(c); err == nil && n > 0 {
			count = n
		}
	}

	page := 1
	if p := q.Get("page"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			page = n
		}
	}

	var afterInstanceID, afterExecutionID string
	if after := q.Get("after"); after != "" {
		if parts := strings.Split(after, ":"); len(parts) == 2 {
			afterInstanceID, afterExecutionID = parts[0], parts[1]
		}
	}

	// Parse filters from query params.
	filter := diag.InstanceFilter{
		Queue:        q.Get("queue"),
		WorkflowName: q.Get("name"),
	}
	stateParam := q.Get("state")
	switch stateParam {
	case "active":
		s := core.WorkflowInstanceStateActive
		filter.State = &s
	case "continued":
		s := core.WorkflowInstanceStateContinuedAsNew
		filter.State = &s
	case "finished":
		s := core.WorkflowInstanceStateFinished
		filter.State = &s
	case "failed":
		s := core.WorkflowInstanceStateFinished
		filter.State = &s
		filter.FailedOnly = true
	}

	instances, err := b.GetWorkflowInstancesFiltered(r.Context(), filter, afterInstanceID, afterExecutionID, count)
	if err != nil {
		http.Error(w, "failed to load instances: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Stats are best-effort; a failure shouldn't blank the whole page.
	stats, _ := b.GetStats(r.Context())

	nextAfter := ""
	if len(instances) == count {
		last := instances[len(instances)-1].Instance
		nextAfter = last.InstanceID + ":" + last.ExecutionID
	}

	// Encode the active filters so pagination/links preserve them.
	fq := url.Values{}
	if stateParam != "" {
		fq.Set("state", stateParam)
	}
	if filter.Queue != "" {
		fq.Set("queue", filter.Queue)
	}
	if filter.WorkflowName != "" {
		fq.Set("name", filter.WorkflowName)
	}

	d := homeData{
		Title:       "Instances",
		BasePath:    basePath(r),
		Instances:   instances,
		Stats:       stats,
		Count:       count,
		Page:        page,
		NextAfter:   nextAfter,
		FilterState: stateParam,
		FilterQueue: filter.Queue,
		FilterName:  filter.WorkflowName,
		FilterQuery: fq.Encode(),
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := renderHome(w, d); err != nil {
		http.Error(w, "render error: "+err.Error(), http.StatusInternalServerError)
	}
}

func handleInstance(w http.ResponseWriter, r *http.Request, b diag.Backend, instanceID, executionID string) {
	instance := core.NewWorkflowInstance(instanceID, executionID)

	ref, err := b.GetWorkflowInstance(r.Context(), instance)
	if err != nil || ref == nil {
		http.NotFound(w, r)
		return
	}

	hist, err := b.GetWorkflowInstanceHistory(r.Context(), ref.Instance, nil)
	if err != nil {
		http.Error(w, "failed to load history: "+err.Error(), http.StatusInternalServerError)
		return
	}

	events := make([]*diag.Event, 0, len(hist))
	for _, e := range hist {
		events = append(events, &diag.Event{
			ID:              e.ID,
			SequenceID:      e.SequenceID,
			Type:            e.Type.String(),
			Timestamp:       e.Timestamp,
			ScheduleEventID: e.ScheduleEventID,
			Attributes:      e.Attributes,
			VisibleAt:       e.VisibleAt,
		})
	}

	info := &diag.WorkflowInstanceInfo{WorkflowInstanceRef: ref, History: events}

	// Tree is best-effort.
	tree, _ := b.GetWorkflowTree(r.Context(), ref.Instance)

	// Prefer the promoted columns for the headline values; fall back to walking
	// history for rows written before the promotion migration.
	histName, histInputs, histResult, histErrJSON, histHasError := extractInstanceDetails(events)

	name := ref.WorkflowName
	if name == "" {
		name = histName
	}

	// Outcome: the state/error columns are authoritative. HasError only matters for
	// a finished instance (the badge logic ignores it for Active/ContinuedAsNew).
	hasError := ref.HasError || histHasError

	// Payloads from the promoted columns (best-effort); fall back to history-decoded.
	inputs := histInputs
	result := histResult
	errJSON := histErrJSON
	if pin, pres, perr, perr2 := b.GetWorkflowInstancePayloads(r.Context(), ref.Instance); perr2 == nil {
		info.Input, info.Result, info.Error = pin, pres, perr
		if len(pin) > 0 {
			inputs = []string{decodePayloadBytes(pin)}
		}
		if len(perr) > 0 {
			errJSON = decodePayloadBytes(perr)
			hasError = true
		} else if len(pres) > 0 {
			result = decodePayloadBytes(pres)
		}
	}

	d := instanceData{
		Title:        "Instance " + instanceID,
		BasePath:     basePath(r),
		Info:         info,
		History:      events,
		Tree:         tree,
		WorkflowName: name,
		Inputs:       inputs,
		Result:       result,
		ErrorJSON:    errJSON,
		HasError:     hasError,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := renderInstance(w, d); err != nil {
		http.Error(w, "render error: "+err.Error(), http.StatusInternalServerError)
	}
}

// basePath reconstructs the external mount prefix for building absolute links.
// When the handler is mounted via http.StripPrefix("/templ", ...), r.URL.Path is
// stripped but r.RequestURI retains the full path, so the prefix is the part of
// RequestURI preceding the (stripped) URL path. Returns a prefix WITHOUT a
// trailing slash (e.g. "/templ"), or "" when mounted at root.
func basePath(r *http.Request) string {
	reqPath := r.RequestURI
	if i := strings.IndexByte(reqPath, '?'); i >= 0 {
		reqPath = reqPath[:i]
	}

	urlPath := r.URL.Path
	if strings.HasSuffix(reqPath, urlPath) {
		prefix := strings.TrimSuffix(reqPath, urlPath)
		return strings.TrimRight(prefix, "/")
	}

	return ""
}
