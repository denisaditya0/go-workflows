package templ

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/denisaditya0/go-workflows/backend"
	"github.com/denisaditya0/go-workflows/backend/history"
	"github.com/denisaditya0/go-workflows/backend/payload"
	"github.com/denisaditya0/go-workflows/core"
	"github.com/denisaditya0/go-workflows/diag"
	"github.com/denisaditya0/go-workflows/internal/workflowerrors"
	"github.com/stretchr/testify/require"
)

// fakeDiagBackend embeds the generated MockBackend (to satisfy backend.Backend)
// and overrides the three diag.Backend methods plus the history read used by the
// instance page, serving fixed in-memory data. No database is required.
type fakeDiagBackend struct {
	*backend.MockBackend

	instances []*diag.WorkflowInstanceRef
	histories map[string][]*history.Event
	payloads  map[string][3][]byte // instanceID -> {input, result, error}
	tree      *diag.WorkflowInstanceTree
	stats     *backend.Stats
}

var _ diag.Backend = (*fakeDiagBackend)(nil)

func (f *fakeDiagBackend) GetWorkflowInstances(_ context.Context, _, _ string, count int) ([]*diag.WorkflowInstanceRef, error) {
	if count < len(f.instances) {
		return f.instances[:count], nil
	}
	return f.instances, nil
}

func (f *fakeDiagBackend) GetWorkflowInstancesFiltered(_ context.Context, filter diag.InstanceFilter, _, _ string, count int) ([]*diag.WorkflowInstanceRef, error) {
	var out []*diag.WorkflowInstanceRef
	for _, i := range f.instances {
		if filter.State != nil && i.State != *filter.State {
			continue
		}
		if filter.FailedOnly && !i.HasError {
			continue
		}
		if filter.Queue != "" && i.Queue != filter.Queue {
			continue
		}
		if filter.WorkflowName != "" &&
			!strings.Contains(strings.ToLower(i.WorkflowName), strings.ToLower(filter.WorkflowName)) {
			continue
		}
		out = append(out, i)
		if len(out) == count {
			break
		}
	}
	return out, nil
}

func (f *fakeDiagBackend) GetWorkflowInstance(_ context.Context, instance *core.WorkflowInstance) (*diag.WorkflowInstanceRef, error) {
	for _, i := range f.instances {
		if i.Instance.InstanceID == instance.InstanceID && i.Instance.ExecutionID == instance.ExecutionID {
			return i, nil
		}
	}
	return nil, nil
}

func (f *fakeDiagBackend) GetWorkflowTree(_ context.Context, _ *core.WorkflowInstance) (*diag.WorkflowInstanceTree, error) {
	return f.tree, nil
}

func (f *fakeDiagBackend) GetWorkflowInstanceHistory(_ context.Context, instance *core.WorkflowInstance, _ *int64) ([]*history.Event, error) {
	return f.histories[instance.InstanceID], nil
}

func (f *fakeDiagBackend) GetWorkflowInstancePayloads(_ context.Context, instance *core.WorkflowInstance) (input, result, errPayload []byte, err error) {
	if p, ok := f.payloads[instance.InstanceID]; ok {
		return p[0], p[1], p[2], nil
	}
	return nil, nil, nil, nil
}

func (f *fakeDiagBackend) GetStats(_ context.Context) (*backend.Stats, error) {
	return f.stats, nil
}

func newFakeBackend() *fakeDiagBackend {
	created := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	completed := created.Add(time.Minute)

	inst := core.NewWorkflowInstance("wf-1", "exec-1")
	failedInst := core.NewWorkflowInstance("wf-2", "exec-2")

	errPayload, _ := json.Marshal(workflowerrors.FromError(context.DeadlineExceeded))

	return &fakeDiagBackend{
		MockBackend: &backend.MockBackend{},
		instances: []*diag.WorkflowInstanceRef{
			// Promoted columns populated: workflow_name + (no) error for wf-1,
			// has_error for wf-2. This exercises the #469 fix in the LIST.
			{Instance: inst, CreatedAt: created, CompletedAt: &completed, State: core.WorkflowInstanceStateFinished, Queue: "default", WorkflowName: "MyWorkflow", HasError: false},
			{Instance: failedInst, CreatedAt: created, CompletedAt: &completed, State: core.WorkflowInstanceStateFinished, Queue: "default", WorkflowName: "FailingWorkflow", HasError: true},
		},
		payloads: map[string][3][]byte{
			"wf-1": {[]byte(`"hello"`), []byte(`42`), nil},
			"wf-2": {nil, nil, errPayload},
		},
		histories: map[string][]*history.Event{
			"wf-1": {
				history.NewHistoryEvent(1, created, history.EventType_WorkflowExecutionStarted,
					&history.ExecutionStartedAttributes{Name: "MyWorkflow", Queue: "default", Inputs: []payload.Payload{payload.Payload(`"hello"`)}}),
				history.NewHistoryEvent(2, completed, history.EventType_WorkflowExecutionFinished,
					&history.ExecutionCompletedAttributes{Result: payload.Payload(`42`)}),
			},
			"wf-2": {
				history.NewHistoryEvent(1, created, history.EventType_WorkflowExecutionStarted,
					&history.ExecutionStartedAttributes{Name: "FailingWorkflow", Queue: "default"}),
				history.NewHistoryEvent(2, completed, history.EventType_WorkflowExecutionFinished,
					&history.ExecutionCompletedAttributes{Error: workflowerrors.FromError(context.DeadlineExceeded)}),
			},
		},
		tree: &diag.WorkflowInstanceTree{
			WorkflowInstanceRef: &diag.WorkflowInstanceRef{Instance: inst, State: core.WorkflowInstanceStateFinished, WorkflowName: "MyWorkflow"},
			WorkflowName:        "MyWorkflow",
		},
		stats: &backend.Stats{
			ActiveWorkflowInstances: 3,
			PendingWorkflowTasks:    map[core.Queue]int64{"default": 2},
			PendingActivityTasks:    map[core.Queue]int64{"default": 1},
		},
	}
}

func TestServeMux_Home(t *testing.T) {
	mux := NewServeMux(newFakeBackend())

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "go-workflows diagnostics")
	require.Contains(t, body, "wf-1")
	require.Contains(t, body, "wf-2")
	// Queue stats rendered
	require.Contains(t, body, "Workflow Queues")
	require.Contains(t, body, "Active workflow instances: 3")
	// Instances are linked with absolute paths (basePath is empty at root mount)
	require.Contains(t, body, `href="/wf-1/exec-1"`)
	// Bootstrap JS bundle must be present so the history accordion is clickable
	require.Contains(t, body, "bootstrap.bundle.min.js")
}

func TestServeMux_InstanceDetail_Completed(t *testing.T) {
	mux := NewServeMux(newFakeBackend())

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/wf-1/exec-1", nil)
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "MyWorkflow")
	require.Contains(t, body, "Completed") // state badge
	require.Contains(t, body, "WorkflowExecutionStarted")
	require.Contains(t, body, "WorkflowExecutionFinished")
	// decoded result payload
	require.Contains(t, body, "42")
	// decoded input payload
	require.Contains(t, body, "hello")
}

func TestServeMux_InstanceDetail_FailedShowsFailed(t *testing.T) {
	// This is the #469 fix: a finished-with-error instance must render as Failed,
	// not Completed, in the detail view.
	mux := NewServeMux(newFakeBackend())

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/wf-2/exec-2", nil)
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	// The instance's own State row must show the Failed badge. Collapse
	// whitespace so the assertion is tolerant of template indentation.
	collapsed := regexp.MustCompile(`\s+`).ReplaceAllString(body, " ")
	require.Contains(t, collapsed, `>State</dt> <dd class="col-sm-9"><span class="badge text-bg-danger">Failed</span></dd>`)
	require.Contains(t, body, "FailingWorkflow")
}

func TestServeMux_InstanceNotFound(t *testing.T) {
	mux := NewServeMux(newFakeBackend())

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nope/nope", nil)
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusNotFound, rr.Code)
}

func TestServeMux_HistoryAccordionIsCollapsible(t *testing.T) {
	// The accordion needs Bootstrap's collapse toggles to be clickable.
	mux := NewServeMux(newFakeBackend())

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/wf-1/exec-1", nil)
	mux.ServeHTTP(rr, req)

	body := rr.Body.String()
	require.Contains(t, body, `data-bs-toggle="collapse"`)
	require.Contains(t, body, `class="accordion-collapse collapse"`)
	require.Contains(t, body, "bootstrap.bundle.min.js")
}

func TestServeMux_LinksAbsoluteUnderStripPrefix(t *testing.T) {
	// When mounted under /templ via http.StripPrefix, all internal links must be
	// absolute and prefixed with /templ so sub-workflow (e.g. RunStep) links in
	// the tree resolve correctly instead of 404-ing.
	inner := NewServeMux(newFakeBackend())
	mux := http.NewServeMux()
	mux.Handle("/templ/", http.StripPrefix("/templ", inner))

	// Instance detail page (has the tree with child links).
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/templ/wf-1/exec-1", nil)
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	// Tree/back links must carry the /templ prefix and NOT use fragile ./../../
	require.Contains(t, body, `href="/templ/wf-1/exec-1"`)
	require.Contains(t, body, `href="/templ/"`) // back-to-list link
	require.NotContains(t, body, "./../../")

	// List page links must also be prefixed.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/templ/", nil)
	mux.ServeHTTP(rr2, req2)
	require.Equal(t, http.StatusOK, rr2.Code)
	require.Contains(t, rr2.Body.String(), `href="/templ/wf-1/exec-1"`)
}

func TestServeMux_Home_ShowsWorkflowNameAndStatusFromPromotedColumns(t *testing.T) {
	// The list now uses the promoted columns: it shows the workflow name and the
	// real Failed/Completed status (#469 fixed in the LIST, not just detail).
	mux := NewServeMux(newFakeBackend())

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()

	// Workflow names appear in the list.
	require.Contains(t, body, "MyWorkflow")
	require.Contains(t, body, "FailingWorkflow")

	// wf-2 has_error=true → its row must render the Failed (danger) badge, and the
	// list must contain a Completed (success) badge for wf-1. Both present proves
	// the list distinguishes outcomes.
	require.Contains(t, body, `text-bg-danger">Failed`)
	require.Contains(t, body, `text-bg-success">Completed`)
}

func TestServeMux_InstanceDetail_UsesPromotedPayloads(t *testing.T) {
	// The detail page prefers the promoted input/result/error columns over decoding
	// history. wf-1 has promoted input "hello" and result 42.
	mux := NewServeMux(newFakeBackend())

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/wf-1/exec-1", nil)
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "hello") // promoted input
	require.Contains(t, body, "42")    // promoted result

	// wf-2 has a promoted error payload → detail shows Failed + the error message.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/wf-2/exec-2", nil)
	mux.ServeHTTP(rr2, req2)
	require.Equal(t, http.StatusOK, rr2.Code)
	body2 := rr2.Body.String()
	require.Contains(t, body2, "context deadline exceeded")
	collapsed := regexp.MustCompile(`\s+`).ReplaceAllString(body2, " ")
	require.Contains(t, collapsed, `>State</dt> <dd class="col-sm-9"><span class="badge text-bg-danger">Failed</span></dd>`)
}

func TestServeMux_Home_FilterByName(t *testing.T) {
	mux := NewServeMux(newFakeBackend())

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/?name=Failing", nil)
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "FailingWorkflow")
	require.NotContains(t, body, "MyWorkflow")
	// The filter form should reflect the active value.
	require.Contains(t, body, `value="Failing"`)
}

func TestServeMux_Home_FilterByFailed(t *testing.T) {
	mux := NewServeMux(newFakeBackend())

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/?state=failed", nil)
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	// Only the failed instance (wf-2) should remain.
	require.Contains(t, body, "wf-2")
	require.NotContains(t, body, "wf-1")
	// Selected option echoed in the form.
	require.Contains(t, body, `value="failed" selected`)
}

func TestServeMux_Home_FilterFormAndClearPresent(t *testing.T) {
	mux := NewServeMux(newFakeBackend())

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	mux.ServeHTTP(rr, req)

	body := rr.Body.String()
	require.Contains(t, body, `name="state"`)
	require.Contains(t, body, `name="queue"`)
	require.Contains(t, body, `name="name"`)
	require.Contains(t, body, ">Filter<")
	require.Contains(t, body, ">Clear<")
}
