package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/diag"
	diagtempl "github.com/cschleiden/go-workflows/diag/templ"
	"github.com/cschleiden/go-workflows/samples"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/google/uuid"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Dedicated DB "dummyworker" (dropped+recreated each run). Register the
	// SourcePropagator like the real worker does, so metadata carries "source".
	b := samples.GetBackend("dummyworker", true, backend.WithContextPropagator(&SourcePropagator{}))

	db, ok := b.(diag.Backend)
	if !ok {
		panic("backend does not implement diag.Backend")
	}

	// Serve the server-rendered diagnostics UI.
	m := http.NewServeMux()
	m.Handle("/diag/", http.StripPrefix("/diag", diagtempl.NewServeMux(db)))
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/diag/", http.StatusFound)
	})
	go func() {
		log.Println("Diagnostics UI: http://localhost:3002/diag/")
		_ = http.ListenAndServe(":3002", m)
	}()

	// Worker.
	w := runWorker(ctx, b)

	// Seed a variety of instances so the UI has realistic data.
	c := client.New(b)
	seed(ctx, c)

	log.Println("Dummy worker running. Browse http://localhost:3002/diag/  (Ctrl+C to stop)")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	cancel()
	_ = w.WaitForCompletion()
}

func runWorker(ctx context.Context, b backend.Backend) *worker.Worker {
	w := worker.New(b, nil)

	w.RegisterWorkflow(RootWorkflow)
	w.RegisterWorkflow(ChildWorkflow)
	w.RegisterWorkflow(FailingWorkflow)
	w.RegisterWorkflow(LoopWorkflow)
	w.RegisterActivity(CallService)
	w.RegisterActivity(FlakyActivity)

	if err := w.Start(ctx); err != nil {
		panic("could not start worker: " + err.Error())
	}
	return w
}

func seed(ctx context.Context, c *client.Client) {
	start := func(id string, source string, wf any, args ...any) {
		// Carry the run source on the Go context so SourcePropagator stamps it.
		sctx := withSource(ctx, source)
		_, err := c.CreateWorkflowInstance(sctx, client.WorkflowInstanceOptions{
			InstanceID: id,
		}, wf, args...)
		if err != nil {
			log.Printf("seed %s: %v", id, err)
		}
	}

	// A few successful trees (manual + scheduled sources).
	start("root-"+uuid.NewString(), "manual", RootWorkflow, "manual")
	start("root-"+uuid.NewString(), "scheduled", RootWorkflow, "scheduled")

	// A failing workflow (shows Failed badge + promoted error).
	start("failing-"+uuid.NewString(), "manual", FailingWorkflow)

	// A continue-as-new loop (shows previous_execution_id lineage).
	start("loop-"+uuid.NewString(), "scheduled", LoopWorkflow, 0)

	// Give the worker a moment to produce history before you browse.
	time.Sleep(2 * time.Second)
}
