package main

import (
	"fmt"

	openplant "github.com/tc252617228/openplant"
)

func main() {
	configs := []openplant.PointConfig{
		{
			GN:         "W3.CALC.TOTAL",
			Source:     openplant.SourceCalc,
			CalcOrder:  2,
			Expression: `return op.value("W3.CALC.RATE") * 60`,
		},
		{
			GN:         "W3.CALC.RATE",
			Source:     openplant.SourceCalc,
			CalcOrder:  1,
			Expression: `return op.rate("W3.SYS.EVENT", 5)`,
		},
		{
			GN:         "W3.MIRROR.AR",
			Source:     openplant.SourceCalc,
			CalcOrder:  1,
			Expression: `return op.ar_sync_time("W3.NODE.P1")`,
		},
	}

	graph := openplant.BuildCalcDependencyGraph(configs)
	order, cycles := graph.EvaluationOrder()
	fmt.Printf("calc order: %v cycles: %d order issues: %d\n", order, len(cycles), len(graph.OrderIssues()))

	monitors := openplant.MirrorSyncMonitors(configs)
	fmt.Printf("mirror sync monitors: %d\n", len(monitors))

	issues := openplant.MirrorDiagnose(openplant.MirrorConfig{
		Replicators: []openplant.Replicator{{
			Name:            "R1",
			SyncMode:        openplant.ReplicationSyncPreserveID,
			ArchiveBackfill: true,
			TimeLimitDays:   openplant.ReplicationBackfillMaxDays,
		}},
		Items: []openplant.RepItem{{
			PointName:  "W3.NODE.P1",
			TargetName: "REMOTE.NODE.P1",
			Transform:  openplant.ReplicationTransformPreserveRole,
		}},
	})
	fmt.Printf("mirror config issues: %d\n", len(issues))

	mutation, err := openplant.BuildDefaultSystemPointTemplateInsert("W3", 1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("system template mutation rows: %d\n", len(mutation.Rows))

	nodeMutation, err := openplant.BuildNodeInsert("W3", []openplant.Node{{
		Name:        "SDK_NODE",
		Description: "example node",
	}})
	if err != nil {
		panic(err)
	}
	fmt.Printf("node mutation rows: %d\n", len(nodeMutation.Rows))
}
