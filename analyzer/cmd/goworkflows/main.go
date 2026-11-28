// Command goworkflows runs the go-workflows analyzer as a standalone tool.
//
// Usage:
//
//	goworkflows ./...
//
// It can also be used as a `go vet` tool:
//
//	go build -o goworkflows ./analyzer/cmd/goworkflows
//	go vet -vettool=./goworkflows ./...
package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/denisaditya0/go-workflows/analyzer"
)

func main() {
	singlechecker.Main(analyzer.Analyzer)
}
