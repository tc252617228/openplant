package calc

import (
	"testing"

	"github.com/tc252617228/openplant/model"
)

func TestBuildDependencyGraphSplitsInternalAndExternalRefs(t *testing.T) {
	graph := BuildDependencyGraph([]model.PointConfig{
		calcPoint("W3.C.A", 2, `return op.value("W3.C.B") + op.value("W3.R.P1")`),
		calcPoint("W3.C.B", 1, `return 1`),
		{GN: "W3.R.P1", Source: model.SourceDAS, Expression: `return 2`},
	})
	node := graph.Nodes["W3.C.A"]
	if len(node.Internal) != 1 || node.Internal[0] != "W3.C.B" {
		t.Fatalf("internal=%#v", node.Internal)
	}
	if len(node.External) != 1 || node.External[0] != "W3.R.P1" {
		t.Fatalf("external=%#v", node.External)
	}
	if _, ok := graph.Nodes["W3.R.P1"]; ok {
		t.Fatalf("DAS point should not be a calc graph node")
	}
}

func TestEvaluationOrderReturnsDependenciesFirst(t *testing.T) {
	graph := BuildDependencyGraph([]model.PointConfig{
		calcPoint("W3.C.A", 3, `return op.value("W3.C.B")`),
		calcPoint("W3.C.B", 2, `return op.value("W3.C.C")`),
		calcPoint("W3.C.C", 1, `return 1`),
	})
	order, cycles := graph.EvaluationOrder()
	if len(cycles) != 0 {
		t.Fatalf("unexpected cycles: %#v", cycles)
	}
	want := []model.GN{"W3.C.C", "W3.C.B", "W3.C.A"}
	if len(order) != len(want) {
		t.Fatalf("order=%#v want %#v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order=%#v want %#v", order, want)
		}
	}
}

func TestEvaluationOrderDetectsCycles(t *testing.T) {
	graph := BuildDependencyGraph([]model.PointConfig{
		calcPoint("W3.C.A", 1, `return op.value("W3.C.B")`),
		calcPoint("W3.C.B", 2, `return op.value("W3.C.A")`),
	})
	order, cycles := graph.EvaluationOrder()
	if order != nil || len(cycles) != 1 {
		t.Fatalf("order=%#v cycles=%#v", order, cycles)
	}
	if len(cycles[0].Path) != 3 || cycles[0].Path[0] != cycles[0].Path[2] {
		t.Fatalf("unexpected cycle path: %#v", cycles[0].Path)
	}
}

func TestOrderIssues(t *testing.T) {
	graph := BuildDependencyGraph([]model.PointConfig{
		calcPoint("W3.C.A", 1, `return op.value("W3.C.B")`),
		calcPoint("W3.C.B", 2, `return 1`),
	})
	issues := graph.OrderIssues()
	if len(issues) != 1 {
		t.Fatalf("issues=%#v", issues)
	}
	if issues[0].GN != "W3.C.A" || issues[0].Dependency != "W3.C.B" || issues[0].DependencyOrder != 2 {
		t.Fatalf("unexpected issue: %#v", issues[0])
	}
}

func calcPoint(gn model.GN, order int8, expression string) model.PointConfig {
	return model.PointConfig{
		GN:         gn,
		Source:     model.SourceCalc,
		CalcOrder:  order,
		Expression: expression,
	}
}
