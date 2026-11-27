package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/denisaditya0/go-workflows/backend"
	"github.com/denisaditya0/go-workflows/client"
	"github.com/denisaditya0/go-workflows/diag"
	diagtempl "github.com/denisaditya0/go-workflows/diag/templ"
	"github.com/denisaditya0/go-workflows/samples"

	"github.com/denisaditya0/go-workflows/worker"

	"github.com/google/uuid"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	b := samples.GetBackend("web", true)

	db, ok := b.(diag.Backend)
	if !ok {
		panic("backend does not implement diag.Backend")
	}

	// Serve the server-rendered templ diagnostics UI at http://localhost:3001/diag/.
	//
	// Note: samples.GetBackend already starts a diagnostics server on :3000; this
	// sample uses :3001 to avoid the port clash.
	m := http.NewServeMux()
	m.Handle("/diag/", http.StripPrefix("/diag", diagtempl.NewServeMux(db)))
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!doctype html><html><body style="font-family:sans-serif;padding:2rem">` +
			`<h1>go-workflows diagnostics</h1><ul>` +
			`<li><a href="/diag/">Diagnostics UI (server-rendered templ)</a></li>` +
			`</ul></body></html>`))
	})
	log.Println("Diagnostics UI: http://localhost:3001/diag/")
	go http.ListenAndServe(":3001", m)

	// Run worker
	w := RunWorker(ctx, b)

	// Start workflow via client
	c := client.New(b)

	runWorkflow(ctx, c)

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint

	cancel()

	if err := w.WaitForCompletion(); err != nil {
		panic("could not stop worker" + err.Error())
	}
}

func runWorkflow(ctx context.Context, c *client.Client) {
	wf, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: uuid.NewString(),
	}, Workflow1, "Hello world"+uuid.NewString(), 2, Inputs{
		Msg:   "",
		Times: 2,
	})
	if err != nil {
		log.Println("could not start workflow:", err)
		return
	}

	log.Printf("Started workflow %s; browse the diag UI while it runs.", wf.InstanceID)

	// Wait for the result in the background so the HTTP server (diag UI) stays
	// up regardless of how long the workflow takes. The sample's Activity1 sleeps
	// ~50s, so a short timeout would otherwise kill the process before you can
	// open a browser.
	go func() {
		result, err := client.GetWorkflowResult[int](ctx, c, wf, 2*time.Minute)
		if err != nil {
			log.Println("workflow did not finish:", err)
			return
		}
		log.Println("Workflow finished. Result:", result)
	}()
}

func RunWorker(ctx context.Context, mb backend.Backend) *worker.Worker {
	w := worker.New(mb, nil)

	w.RegisterWorkflow(Workflow1)
	w.RegisterWorkflow(RunJob)
	w.RegisterWorkflow(RunStep)

	w.RegisterActivity(Activity1)

	if err := w.Start(ctx); err != nil {
		panic("could not start worker")
	}

	return w
}
