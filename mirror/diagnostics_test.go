package mirror

import (
	"testing"

	"github.com/tc252617228/openplant/model"
)

func TestDiagnoseReportsReplicatorAndItemIssues(t *testing.T) {
	issues := Diagnose(Config{
		Replicators: []model.Replicator{
			{Name: "R1", SyncMode: model.ReplicationSyncPreserveID, ArchiveBackfill: true},
			{Name: "R1", SyncMode: 9, TimeLimitDays: 31},
		},
		Items: []model.RepItem{
			{PointName: "W3.P1", TargetName: "REMOTE.P1", Transform: model.ReplicationTransformPreserveRole},
			{PointName: "W3.P1", TargetName: "REMOTE.P2", Transform: 9},
			{PointName: "W3.P2", TargetName: "REMOTE.P1", Transform: model.ReplicationTransformCalcAsDAS},
		},
	})
	if !hasIssue(issues, SeverityWarning, "Replicator", "TL") {
		t.Fatalf("missing TL warning: %#v", issues)
	}
	if !hasIssue(issues, SeverityError, "Replicator", "RN") {
		t.Fatalf("missing duplicate RN error: %#v", issues)
	}
	if !hasIssue(issues, SeverityError, "RepItem", "XF") {
		t.Fatalf("missing XF error: %#v", issues)
	}
	if !hasIssue(issues, SeverityError, "RepItem", "PN") {
		t.Fatalf("missing duplicate PN error: %#v", issues)
	}
	if !hasIssue(issues, SeverityWarning, "RepItem", "TN") {
		t.Fatalf("missing duplicate target warning: %#v", issues)
	}
}

func TestDiagnoseWarnsItemsWithoutReplicators(t *testing.T) {
	issues := Diagnose(Config{
		Items: []model.RepItem{{PointName: "W3.P1", TargetName: "REMOTE.P1", Transform: model.ReplicationTransformPreserveRole}},
	})
	if !hasIssue(issues, SeverityWarning, "RepItem", "RN") {
		t.Fatalf("missing no-replicator warning: %#v", issues)
	}
}

func TestSyncMonitorsFindsMirrorFunctions(t *testing.T) {
	monitors := SyncMonitors([]model.PointConfig{
		{
			GN:         "W3.M.AR",
			Expression: `return op.ar_sync_time("W3.N.P1")`,
		},
		{
			GN:         "W3.M.RT",
			Expression: `return op.rt_sync_time("W3.N.P2")`,
		},
		{
			GN:         "W3.M.BOTH",
			Expression: `return op.ar_sync_time("W3.N.P3") + op.rt_sync_time("W3.N.P3")`,
		},
	})
	if len(monitors) != 4 {
		t.Fatalf("monitors=%#v", monitors)
	}
	if monitors[0].GN != "W3.M.AR" || monitors[0].Kind != SyncArchive || monitors[0].References[0] != "W3.N.P1" {
		t.Fatalf("unexpected first monitor: %#v", monitors[0])
	}
}

func hasIssue(issues []Issue, severity Severity, scope, field string) bool {
	for _, issue := range issues {
		if issue.Severity == severity && issue.Scope == scope && issue.Field == field {
			return true
		}
	}
	return false
}
