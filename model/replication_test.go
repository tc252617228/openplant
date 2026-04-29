package model

import (
	"strings"
	"testing"
)

func TestReplicationEnums(t *testing.T) {
	if !ReplicationSyncLoose.Valid() || !ReplicationSyncPreserveID.Valid() || ReplicationSyncMode(9).Valid() {
		t.Fatalf("unexpected sync mode validity")
	}
	if ReplicationSyncPreserveID.String() != "preserve_id" {
		t.Fatalf("sync string=%s", ReplicationSyncPreserveID)
	}
	if !ReplicationTransformPreserveRole.Valid() || !ReplicationTransformCalcAsDAS.Valid() || ReplicationTransform(9).Valid() {
		t.Fatalf("unexpected transform validity")
	}
	if ReplicationTransformCalcAsDAS.String() != "calc_as_das" {
		t.Fatalf("transform string=%s", ReplicationTransformCalcAsDAS)
	}
}

func TestReplicatorValidation(t *testing.T) {
	valid := Replicator{
		Name:            "R1",
		Port:            8200,
		SourcePort:      8200,
		SyncMode:        ReplicationSyncPreserveID,
		FilterUnchanged: true,
		ArchiveBackfill: true,
		TimeLimitDays:   ReplicationBackfillMaxDays,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid replicator failed: %v", err)
	}
	invalid := valid
	invalid.Name = ""
	invalid.SyncMode = 7
	invalid.TimeLimitDays = 31
	err := invalid.Validate()
	if err == nil {
		t.Fatalf("expected invalid replicator error")
	}
	text := err.Error()
	for _, want := range []string{"RN:", "SY:", "TL:"} {
		if !strings.Contains(text, want) {
			t.Fatalf("error missing %q: %s", want, text)
		}
	}
}

func TestRepItemValidation(t *testing.T) {
	valid := RepItem{PointName: "W3.N.P1", TargetName: "W3.N.P1", Transform: ReplicationTransformPreserveRole}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid rep item failed: %v", err)
	}
	invalid := RepItem{Transform: 2}
	err := invalid.Validate()
	if err == nil {
		t.Fatalf("expected invalid rep item error")
	}
	text := err.Error()
	for _, want := range []string{"PN:", "TN:", "XF:"} {
		if !strings.Contains(text, want) {
			t.Fatalf("error missing %q: %s", want, text)
		}
	}
}
