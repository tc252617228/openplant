package calc

import (
	"sort"

	"github.com/tc252617228/openplant/model"
)

type DependencyGraph struct {
	Nodes map[model.GN]DependencyNode
}

type DependencyNode struct {
	GN         model.GN
	Config     model.PointConfig
	References []model.GN
	Internal   []model.GN
	External   []model.GN
}

type DependencyCycle struct {
	Path []model.GN
}

type OrderIssue struct {
	GN              model.GN
	Dependency      model.GN
	CalcOrder       int8
	DependencyOrder int8
	Message         string
}

func BuildDependencyGraph(configs []model.PointConfig) DependencyGraph {
	nodes := make(map[model.GN]DependencyNode, len(configs))
	for _, cfg := range configs {
		if cfg.GN == "" {
			continue
		}
		if cfg.Source != model.SourceCalc || cfg.Expression == "" {
			continue
		}
		refs := FormulaReferences(cfg.Expression)
		nodes[cfg.GN] = DependencyNode{
			GN:         cfg.GN,
			Config:     cfg,
			References: refs,
		}
	}
	for gn, node := range nodes {
		internal := make([]model.GN, 0, len(node.References))
		external := make([]model.GN, 0, len(node.References))
		for _, ref := range node.References {
			if _, ok := nodes[ref]; ok {
				internal = append(internal, ref)
			} else {
				external = append(external, ref)
			}
		}
		node.Internal = internal
		node.External = external
		nodes[gn] = node
	}
	return DependencyGraph{Nodes: nodes}
}

func (g DependencyGraph) EvaluationOrder() ([]model.GN, []DependencyCycle) {
	visiting := make(map[model.GN]int, len(g.Nodes))
	stack := make([]model.GN, 0, len(g.Nodes))
	order := make([]model.GN, 0, len(g.Nodes))
	cycles := make([]DependencyCycle, 0)

	var visit func(model.GN)
	visit = func(gn model.GN) {
		switch visiting[gn] {
		case 2:
			return
		case 1:
			cycles = append(cycles, DependencyCycle{Path: cyclePath(stack, gn)})
			return
		}
		node, ok := g.Nodes[gn]
		if !ok {
			return
		}
		visiting[gn] = 1
		stack = append(stack, gn)
		for _, dep := range node.Internal {
			visit(dep)
		}
		stack = stack[:len(stack)-1]
		visiting[gn] = 2
		order = append(order, gn)
	}

	for _, gn := range g.sortedGNs() {
		visit(gn)
	}
	if len(cycles) > 0 {
		return nil, dedupeCycles(cycles)
	}
	return order, nil
}

func (g DependencyGraph) Cycles() []DependencyCycle {
	_, cycles := g.EvaluationOrder()
	return cycles
}

func (g DependencyGraph) OrderIssues() []OrderIssue {
	issues := make([]OrderIssue, 0)
	for _, gn := range g.sortedGNs() {
		node := g.Nodes[gn]
		for _, depGN := range node.Internal {
			dep := g.Nodes[depGN]
			if dep.Config.CalcOrder >= node.Config.CalcOrder {
				issues = append(issues, OrderIssue{
					GN:              node.GN,
					Dependency:      dep.GN,
					CalcOrder:       node.Config.CalcOrder,
					DependencyOrder: dep.Config.CalcOrder,
					Message:         "dependency calculation order must be lower than dependent calculation order",
				})
			}
		}
	}
	return issues
}

func (g DependencyGraph) sortedGNs() []model.GN {
	gns := make([]model.GN, 0, len(g.Nodes))
	for gn := range g.Nodes {
		gns = append(gns, gn)
	}
	sort.Slice(gns, func(i, j int) bool { return gns[i] < gns[j] })
	return gns
}

func cyclePath(stack []model.GN, gn model.GN) []model.GN {
	start := 0
	for i, item := range stack {
		if item == gn {
			start = i
			break
		}
	}
	path := append([]model.GN(nil), stack[start:]...)
	path = append(path, gn)
	return path
}

func dedupeCycles(cycles []DependencyCycle) []DependencyCycle {
	seen := make(map[string]struct{}, len(cycles))
	out := make([]DependencyCycle, 0, len(cycles))
	for _, cycle := range cycles {
		key := cycleKey(cycle.Path)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, cycle)
	}
	return out
}

func cycleKey(path []model.GN) string {
	if len(path) == 0 {
		return ""
	}
	min := 0
	for i := 1; i < len(path)-1; i++ {
		if path[i] < path[min] {
			min = i
		}
	}
	key := ""
	for i := 0; i < len(path)-1; i++ {
		if i > 0 {
			key += ">"
		}
		key += string(path[(min+i)%(len(path)-1)])
	}
	return key
}
