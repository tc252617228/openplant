package protocol

import "testing"

func TestTableSelectRejectsAmbiguousIndexes(t *testing.T) {
	req := TableSelectRequest{
		Table:   "W3.Archive",
		Columns: []string{"ID"},
		Indexes: &Indexes{
			Key:     "ID",
			Int32:   []int32{1001},
			Strings: []string{"W3.N.P1"},
		},
	}
	if _, err := req.Encode(); err == nil {
		t.Fatalf("expected ambiguous indexes to be rejected")
	}
}

func TestTableMutationRejectsAmbiguousIndexes(t *testing.T) {
	req := TableMutationRequest{
		Table:  "Point",
		Action: ActionDelete,
		Key:    "ID",
		Indexes: &Indexes{
			Int32:   []int32{1001},
			Strings: []string{"W3.N.P1"},
		},
	}
	if _, err := req.Encode(); err == nil {
		t.Fatalf("expected ambiguous indexes to be rejected")
	}
}
