package analyzer

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/ast/inspector"
)

// IsWorkflowFact records that a function was registered as a workflow.
//
// Not every function that takes a workflow.Context is a workflow: helper
// functions share the same first parameter. Registration (via
// RegisterWorkflow or tester.NewWorkflowTester) is the reliable signal, so the
// analyzer records it as a fact.
type IsWorkflowFact struct{}

func (*IsWorkflowFact) AFact() {}

func (*IsWorkflowFact) String() string { return "workflow" }

// registrations describes the workflows registered within the package that is
// currently being analyzed.
type registrations struct {
	funcs map[*types.Func]struct{}
	lits  map[*ast.FuncLit]struct{}

	// found is true if the package contains at least one registration call,
	// even if its arguments could not be resolved to a concrete function.
	found bool
}

func (r registrations) hasAny() bool { return r.found }

func (r registrations) isRegisteredLit(lit *ast.FuncLit) bool {
	_, ok := r.lits[lit]
	return ok
}

func (r registrations) isRegisteredFunc(pass *analysis.Pass, fn *types.Func) bool {
	if fn == nil {
		return false
	}

	if _, ok := r.funcs[fn]; ok {
		return true
	}

	if fn.Pkg() == pass.Pkg {
		return false
	}

	// Workflows registered in the package where they are defined export a
	// fact; consult it for functions imported from other packages.
	return pass.ImportObjectFact(fn, new(IsWorkflowFact))
}

// findRegistrations scans the package for workflow registration calls and
// collects the registered functions and function literals.
func findRegistrations(pass *analysis.Pass, insp *inspector.Inspector) registrations {
	regs := registrations{
		funcs: map[*types.Func]struct{}{},
		lits:  map[*ast.FuncLit]struct{}{},
	}

	// Package-level workflow literals are usually registered by reference, so
	// resolve identifiers to the literal that initializes the variable.
	varLits := collectVarLits(pass, insp)

	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 || !isRegistrationCall(pass, call) {
			return
		}

		regs.found = true

		fn, lit := resolveFunc(pass, varLits, call.Args[0])
		if fn != nil {
			if _, seen := regs.funcs[fn]; seen {
				return
			}

			regs.funcs[fn] = struct{}{}

			if fn.Pkg() == pass.Pkg {
				pass.ExportObjectFact(fn, new(IsWorkflowFact))
			}
		}

		if lit != nil {
			regs.lits[lit] = struct{}{}
		}
	})

	return regs
}

// collectVarLits maps package-level variables to the function literal that
// initializes them, so registrations by reference can be resolved.
func collectVarLits(pass *analysis.Pass, insp *inspector.Inspector) map[*types.Var]*ast.FuncLit {
	varLits := map[*types.Var]*ast.FuncLit{}

	insp.Preorder([]ast.Node{(*ast.ValueSpec)(nil), (*ast.AssignStmt)(nil)}, func(n ast.Node) {
		switch n := n.(type) {
		case *ast.ValueSpec:
			for i, name := range n.Names {
				if i >= len(n.Values) {
					break
				}

				lit, ok := n.Values[i].(*ast.FuncLit)
				if !ok {
					continue
				}

				if v, ok := pass.TypesInfo.ObjectOf(name).(*types.Var); ok {
					varLits[v] = lit
				}
			}

		case *ast.AssignStmt:
			if len(n.Lhs) != len(n.Rhs) {
				break
			}

			for i, lhs := range n.Lhs {
				lit, ok := n.Rhs[i].(*ast.FuncLit)
				if !ok {
					continue
				}

				id, ok := lhs.(*ast.Ident)
				if !ok {
					continue
				}

				if v, ok := pass.TypesInfo.ObjectOf(id).(*types.Var); ok {
					varLits[v] = lit
				}
			}
		}
	})

	return varLits
}

// isRegistrationCall reports whether call is a workflow registration site.
func isRegistrationCall(pass *analysis.Pass, call *ast.CallExpr) bool {
	fn := calleeFunc(pass, call.Fun)
	if fn == nil {
		return false
	}

	switch fn.Name() {
	case "RegisterWorkflow":
		return true
	case "NewWorkflowTester":
		return fn.Pkg() != nil && strings.HasSuffix(fn.Pkg().Path(), "/tester")
	}

	return false
}

// calleeFunc resolves the function being called, unwrapping generic
// instantiations and qualified selectors.
func calleeFunc(pass *analysis.Pass, expr ast.Expr) *types.Func {
	switch e := unwrapIndex(expr).(type) {
	case *ast.Ident:
		fn, _ := pass.TypesInfo.ObjectOf(e).(*types.Func)
		return fn
	case *ast.SelectorExpr:
		fn, _ := pass.TypesInfo.ObjectOf(e.Sel).(*types.Func)
		return fn
	}

	return nil
}

// resolveFunc resolves a registered workflow argument to either a named
// function or a function literal. Identifiers that refer to package-level
// variables initialized with a literal are resolved through varLits.
func resolveFunc(pass *analysis.Pass, varLits map[*types.Var]*ast.FuncLit, expr ast.Expr) (*types.Func, *ast.FuncLit) {
	switch e := unwrapIndex(expr).(type) {
	case *ast.Ident:
		switch obj := pass.TypesInfo.ObjectOf(e).(type) {
		case *types.Func:
			return obj, nil
		case *types.Var:
			return nil, varLits[obj]
		}
	case *ast.SelectorExpr:
		fn, _ := pass.TypesInfo.ObjectOf(e.Sel).(*types.Func)
		return fn, nil
	case *ast.FuncLit:
		return nil, e
	}

	return nil, nil
}

func unwrapIndex(expr ast.Expr) ast.Expr {
	switch e := expr.(type) {
	case *ast.IndexExpr:
		return unwrapIndex(e.X)
	case *ast.IndexListExpr:
		return unwrapIndex(e.X)
	}

	return expr
}
