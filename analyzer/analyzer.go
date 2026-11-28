package analyzer

import (
	"go/ast"
	"go/types"
	"strings"

	"github.com/golangci/plugin-module-register/register"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the default analyzer with no settings. It can be used by any
// go/analysis driver, for example singlechecker (`cmd/goworkflows`),
// `go vet -vettool`, or analysistest.
var Analyzer = NewAnalyzer(Settings{})

// NewAnalyzer returns an analyzer configured with the given settings.
func NewAnalyzer(settings Settings) *analysis.Analyzer {
	return &analysis.Analyzer{
		Name:      "goworkflows",
		Doc:       "Checks for common errors when writing workflows",
		Run:       func(pass *analysis.Pass) (interface{}, error) { return run(pass, settings) },
		Requires:  []*analysis.Analyzer{inspect.Analyzer},
		FactTypes: []analysis.Fact{new(IsWorkflowFact)},
	}
}

func init() {
	register.Plugin("goworkflows", New)
}

func New(settings any) (register.LinterPlugin, error) {
	// The configuration type will be map[string]any or []interface, it depends on your configuration.
	// You can use https://github.com/go-viper/mapstructure to convert map to struct.
	s, err := register.DecodeSettings[Settings](settings)
	if err != nil {
		return nil, err
	}

	return &GoWorkflowsPlugin{Settings: s}, nil
}

type GoWorkflowsPlugin struct {
	Settings Settings
}

type Settings struct {
	CheckPrivateReturnValues bool `json:"checkprivatereturnvalues"`
}

func (w *GoWorkflowsPlugin) BuildAnalyzers() ([]*analysis.Analyzer, error) {
	return []*analysis.Analyzer{NewAnalyzer(w.Settings)}, nil
}

func (w *GoWorkflowsPlugin) GetLoadMode() string {
	// The analyzer inspects type information (e.g. `pass.TypesInfo`), so it must
	// load types; `register.LoadModeSyntax` would leave `TypesInfo` empty.
	return register.LoadModeTypesInfo
}

func run(pass *analysis.Pass, settings Settings) (interface{}, error) {
	inspector := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// inWorkflow tracks whether the node currently being visited is lexically
	// part of a workflow. Workflows are functions (named or literals) whose first
	// parameter is a workflow.Context. Nested function literals inherit the state
	// of their parent, so helper closures inside a workflow are still checked,
	// while package-level literals are not.
	inWorkflow := false

	// litStack records the inWorkflow value before entering each function literal
	// so that it can be restored when leaving the literal.
	var litStack []bool

	workflowImportName := "workflow"

	// Registration is the reliable signal that a function is a workflow. When a
	// package registers at least one workflow, only registered functions and
	// literals are analyzed; helpers that merely take a workflow.Context are
	// ignored. Packages that never register anything (for example when the
	// analyzer is used standalone) fall back to the parameter heuristic.
	regs := findRegistrations(pass, inspector)

	inspector.Nodes([]ast.Node{
		(*ast.File)(nil),
		(*ast.FuncDecl)(nil),
		(*ast.FuncLit)(nil),
		(*ast.ImportSpec)(nil),
		(*ast.RangeStmt)(nil),
		(*ast.SelectStmt)(nil),
		(*ast.GoStmt)(nil),
		(*ast.CallExpr)(nil),
		(*ast.AssignStmt)(nil),
		(*ast.IncDecStmt)(nil),
	}, func(node ast.Node, push bool) bool {
		switch n := node.(type) {
		case *ast.File:
			if push {
				// New file, reset state
				inWorkflow = false
				workflowImportName = "workflow"
			}
			return true

		case *ast.ImportSpec:
			if !push {
				return false
			}

			if isWorkflowPackage(importPath(n)) {
				if n.Name != nil {
					workflowImportName = n.Name.Name
				} else {
					workflowImportName = "workflow"
				}
			}
			return true

		case *ast.FuncDecl:
			if !push {
				// Finished with the current workflow
				inWorkflow = false
				return false
			}

			// Only check functions that are workflows. Non-workflow functions
			// cannot contain workflow code, but their bodies still have to be
			// traversed so that workflow literals defined and registered inside
			// them (for example in `init` or `main`) are discovered.
			if !isWorkflowDecl(pass, regs, workflowImportName, n) {
				return true
			}

			inWorkflow = true

			// Check return types
			if n.Name.IsExported() || settings.CheckPrivateReturnValues {
				if n.Type.Results == nil || len(n.Type.Results.List) == 0 {
					pass.Reportf(n.Pos(), "workflow `%v` doesn't return anything. needs to return at least `error`", n.Name.Name)
				} else {
					if len(n.Type.Results.List) > 2 {
						pass.Reportf(n.Pos(), "workflow `%v` returns more than two values", n.Name.Name)
						return true
					}

					lastResult := n.Type.Results.List[len(n.Type.Results.List)-1]
					if types.ExprString(lastResult.Type) != "error" {
						pass.Reportf(n.Pos(), "workflow `%v` doesn't return `error` as last return value", n.Name.Name)
					}
				}
			}

			funcScope := pass.TypesInfo.Scopes[n.Type]
			if funcScope != nil {
				checkVarsInScope(pass, funcScope)
			}

			// Continue with the function's children
			return true

		case *ast.FuncLit:
			if !push {
				if len(litStack) > 0 {
					inWorkflow = litStack[len(litStack)-1]
					litStack = litStack[:len(litStack)-1]
				}
				return false
			}

			litStack = append(litStack, inWorkflow)

			// Package-level literals are not workflows; literals nested inside a
			// workflow inherit its state. A registered literal, or (in heuristic
			// mode) a literal whose first parameter is a workflow.Context, is
			// treated as a workflow itself.
			if isWorkflowLit(regs, workflowImportName, n) {
				parentInWorkflow := inWorkflow
				inWorkflow = true

				// If the literal is nested inside a workflow, its variables are
				// already covered by the enclosing function's scope walk.
				if !parentInWorkflow {
					funcScope := pass.TypesInfo.Scopes[n.Type]
					if funcScope != nil {
						checkVarsInScope(pass, funcScope)
					}
				}
			}

			return true
		}

		if !push {
			// Only check nodes while descending
			return false
		}

		if !inWorkflow {
			// Keep traversing so workflow function literals are still discovered,
			// but skip the workflow-specific checks for this node.
			return true
		}

		switch n := node.(type) {
		case *ast.RangeStmt:
			{
				t := pass.TypesInfo.TypeOf(n.X)
				if t == nil {
					break
				}

				switch t.(type) {
				case *types.Map:
					pass.Reportf(n.Pos(), "iterating over a `map` is not deterministic and not allowed in workflows")

				case *types.Chan:
					pass.Reportf(n.Pos(), "using native channels is not allowed in workflows, use `workflow.Channel` instead")
				}
			}

		case *ast.SelectStmt:
			pass.Reportf(n.Pos(), "`select` statements are not allowed in workflows, use `workflow.Select` instead")

		case *ast.GoStmt:
			pass.Reportf(n.Pos(), "use `workflow.Go` instead of `go` in workflows")

		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				checkPackageLevelWrite(pass, lhs)
			}

		case *ast.IncDecStmt:
			checkPackageLevelWrite(pass, n.X)

		case *ast.CallExpr:
			var pkg *ast.Ident
			var id *ast.Ident
			switch fun := n.Fun.(type) {
			case *ast.SelectorExpr:
				pkg, _ = fun.X.(*ast.Ident)
				id = fun.Sel
			}

			if pkg == nil || id == nil {
				break
			}

			pkgInfo := pass.TypesInfo.Uses[pkg]
			pkgName, _ := pkgInfo.(*types.PkgName)
			if pkgName == nil {
				break
			}

			switch pkgName.Imported().Path() {
			case "time":
				switch id.Name {
				case "Now":
					pass.Reportf(n.Pos(), "`time.Now` is not allowed in workflows, use `workflow.Now` instead")
				case "Sleep":
					pass.Reportf(n.Pos(), "`time.Sleep` is not allowed in workflows, use `workflow.Sleep` instead")
				case "After", "Tick", "NewTimer", "NewTicker", "Since":
					pass.Reportf(n.Pos(), "`time.%v` is not allowed in workflows, use `workflow.ScheduleTimer` or `workflow.Sleep` instead", id.Name)
				}

			case "math/rand", "math/rand/v2":
				pass.Reportf(n.Pos(), "`%v.%v` is not deterministic and not allowed in workflows", pkgName.Imported().Name(), id.Name)

			case "context":
				switch id.Name {
				case "Background", "TODO":
					pass.Reportf(n.Pos(), "`context.%v` is not allowed in workflows, use the `workflow.Context` passed into the workflow", id.Name)
				}
			}
		}

		// Continue with the children
		return true
	})

	return nil, nil
}

func checkVarsInScope(pass *analysis.Pass, scope *types.Scope) {
	for _, name := range scope.Names() {
		obj := scope.Lookup(name)
		switch t := obj.Type().(type) {
		case *types.Chan:
			pass.Reportf(obj.Pos(), "using native channels is not allowed in workflows, use `workflow.Channel` instead")

		case *types.Named:
			checkNamed(pass, obj, t)

		case *types.Pointer:
			if named, ok := t.Elem().(*types.Named); ok {
				checkNamed(pass, obj, named)
			}
		}
	}

	for i := 0; i < scope.NumChildren(); i++ {
		checkVarsInScope(pass, scope.Child(i))
	}
}

func checkNamed(pass *analysis.Pass, ref types.Object, named *types.Named) {
	if obj := named.Obj(); obj != nil {
		if pkg := obj.Pkg(); pkg != nil {
			switch pkg.Path() {
			case "sync":
				switch obj.Name() {
				case "WaitGroup":
					pass.Reportf(ref.Pos(), "using `sync.WaitGroup` is not allowed in workflows, use `workflow.WaitGroup` instead")
				case "Mutex", "RWMutex", "Once", "Cond":
					pass.Reportf(ref.Pos(), "using `sync.%v` is not allowed in workflows", obj.Name())
				}
			}
		}
	}

}

// checkPackageLevelWrite reports writes to package-level variables from within
// a workflow. Such writes are shared across executions and replays and are not
// deterministic.
func checkPackageLevelWrite(pass *analysis.Pass, expr ast.Expr) {
	var id *ast.Ident
	switch e := expr.(type) {
	case *ast.Ident:
		id = e
	case *ast.SelectorExpr:
		id = e.Sel
	default:
		return
	}

	obj := pass.TypesInfo.ObjectOf(id)
	if obj == nil {
		return
	}

	if _, ok := obj.(*types.Var); !ok {
		return
	}

	if obj.Pkg() != nil && obj.Parent() == obj.Pkg().Scope() {
		pass.Reportf(expr.Pos(), "writing to package-level variable `%v` from a workflow is not deterministic", obj.Name())
	}
}

func isWorkflow(workflowImportName string, funcType *ast.FuncType) bool {
	if funcType.Params == nil {
		return false
	}

	params := funcType.Params.List

	// Need at least workflow.Context
	if len(params) < 1 {
		return false
	}

	firstParam, ok := params[0].Type.(*ast.SelectorExpr)
	if !ok { // first param type isn't identificator so it can't be of type "string"
		return false
	}

	xname, ok := firstParam.X.(*ast.Ident)
	if !ok {
		return false
	}

	selname := firstParam.Sel.Name
	if xname.Name+"."+selname != workflowImportName+".Context" {
		return false
	}

	return true
}

// isWorkflowDecl reports whether the function declaration is a workflow. When
// the package registers workflows anywhere, only registered functions count;
// otherwise the workflow.Context parameter heuristic is used.
func isWorkflowDecl(pass *analysis.Pass, regs registrations, workflowImportName string, n *ast.FuncDecl) bool {
	if regs.hasAny() {
		fn, _ := pass.TypesInfo.ObjectOf(n.Name).(*types.Func)
		return regs.isRegisteredFunc(pass, fn)
	}

	return isWorkflow(workflowImportName, n.Type)
}

// isWorkflowLit reports whether the function literal is a workflow, following
// the same registration-first rule as isWorkflowDecl.
func isWorkflowLit(regs registrations, workflowImportName string, n *ast.FuncLit) bool {
	if regs.hasAny() {
		return regs.isRegisteredLit(n)
	}

	return isWorkflow(workflowImportName, n.Type)
}

// workflowPackageSuffix is used instead of a fully-qualified import path so that
// the analyzer keeps working when the module is forked, vendored, or imported
// through a different path.
const workflowPackageSuffix = "/workflow"

func isWorkflowPackage(path string) bool {
	return path == "workflow" || strings.HasSuffix(path, workflowPackageSuffix)
}

// importPath returns the unquoted import path of an ImportSpec.
func importPath(spec *ast.ImportSpec) string {
	return strings.Trim(spec.Path.Value, `"`)
}
