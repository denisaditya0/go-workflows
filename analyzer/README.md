## Custom analyzer

This package implements a static analyzer for checking various common workflow
error conditions, such as non-deterministic operations or use of native Go
primitives that are not safe inside workflows.

It is deliberately decoupled from any single driver: the rules live in a plain
[`analysis.Analyzer`](https://pkg.go.dev/golang.org/x/tools/go/analysis) and can
be run from `go vet`, a standalone binary, `golangci-lint`, or `analysistest`.

### Standalone binary / `go vet`

```bash
# Build the standalone tool
go build -o goworkflows ./analyzer/cmd/goworkflows

# Run it directly
./goworkflows ./...

# Or use it as a go vet tool
go vet -vettool=./goworkflows ./...
```

### golangci-lint

The package also registers itself as a golangci-lint plugin named
`goworkflows`. In your own `.golangci.yml` configuration file you can enable it
like this:

```yaml
version: "2"

linters:
  enable:
    - goworkflows

  settings:
    custom:
      goworkflows:
        type: module
        original-url: github.com/denisaditya0/go-workflows/analyzer
        # Optional. When true, return-signature checks also apply to unexported
        # workflows. Workflows are detected by registration, so this does not
        # flag helpers that merely take a workflow.Context.
        # settings:
        #   checkprivatereturnvalues: true
```

### How workflows are detected

A workflow is identified by its registration, not merely by its signature:

- `*worker.Worker.RegisterWorkflow(...)`, `*tester.Registry().RegisterWorkflow(...)`,
  or any method named `RegisterWorkflow`
- `tester.NewWorkflowTester[T](wf)`

Registration is resolved for named functions and for function literals, including
package-level variables initialized with a literal and literals registered inline
(e.g. inside `init` or a test). This means helper functions that happen to take a
`workflow.Context` are not treated as workflows.

If a package contains **no** registration site at all (for example when the
analyzer is run standalone on a snippet), it falls back to the heuristic that any
function whose first parameter is a `workflow.Context` is a workflow.

### Checks

The analyzer only inspects functions that are workflows. It reports:

- missing `error` return value (or more than two return values)
- ranging over a `map` (non-deterministic)
- use of `select` (use `workflow.Select`)
- use of `go` (use `workflow.Go`)
- use of native channels (use `workflow.Channel`)
- use of `sync.WaitGroup` (use `workflow.WaitGroup`), `sync.Mutex`, `sync.RWMutex`, `sync.Once`, or `sync.Cond`
- writes to package-level variables (not deterministic across replays)
- use of `context.Background` or `context.TODO` (use the `workflow.Context` passed in)
- use of `time.Now`, `time.Sleep`, `time.After`, `time.Tick`, `time.NewTimer`,
  `time.NewTicker`, and `time.Since` (use the `workflow` equivalents)
- use of `math/rand` (non-deterministic)
