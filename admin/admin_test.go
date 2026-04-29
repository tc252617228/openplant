package admin

import (
	"testing"

	"github.com/tc252617228/openplant/operror"
)

func TestTableMutationRequiresBoundedUpdateAndDelete(t *testing.T) {
	for _, req := range []TableMutation{
		{
			Table:   "Point",
			Action:  MutationUpdate,
			Columns: []Column{{Name: "ED", Type: ColumnString}},
			Rows:    []Row{{"ED": "x"}},
		},
		{
			Table:  "Point",
			Action: MutationDelete,
		},
	} {
		if err := req.Validate(); err == nil || !operror.IsKind(err, operror.KindValidation) {
			t.Fatalf("expected validation error, got %v", err)
		}
	}
}

func TestTableMutationRejectsTimeSeriesTables(t *testing.T) {
	for _, table := range []string{"W3.Realtime", "W3.Dynamic", "W3.Archive", "W3.Stat", "W3.Alarm", "W3.AAlarm"} {
		req := TableMutation{
			Table:  table,
			Action: MutationDelete,
			Filters: []Filter{{
				Left:     "ID",
				Operator: FilterEQ,
				Right:    "1001",
				Relation: FilterAnd,
			}},
		}
		if err := req.Validate(); err == nil || !operror.IsKind(err, operror.KindValidation) {
			t.Fatalf("expected validation error for %s, got %v", table, err)
		}
	}
}

func TestTableMutationAcceptsBoundedConfigUpdate(t *testing.T) {
	req := TableMutation{
		DB:     "W3",
		Table:  "Point",
		Action: MutationUpdate,
		Filters: []Filter{{
			Left:     "GN",
			Operator: FilterEQ,
			Right:    "W3.TEST.P1",
			Relation: FilterAnd,
		}},
		Columns: []Column{{Name: "ED", Type: ColumnString}},
		Rows:    []Row{{"ED": "x"}},
	}
	if err := req.Validate(); err != nil {
		t.Fatalf("valid mutation rejected: %v", err)
	}
}

func TestTableMutationIndexesRequireKeyAndSingleValueType(t *testing.T) {
	base := TableMutation{
		DB:      "W3",
		Table:   "Point",
		Action:  MutationUpdate,
		Columns: []Column{{Name: "ED", Type: ColumnString}},
		Rows:    []Row{{"ED": "x"}},
	}
	withoutKey := base
	withoutKey.Indexes = &Indexes{Strings: []string{"W3.TEST.P1"}}
	if err := withoutKey.Validate(); err == nil || !operror.IsKind(err, operror.KindValidation) {
		t.Fatalf("expected missing index key validation error, got %v", err)
	}

	multipleValueTypes := base
	multipleValueTypes.Indexes = &Indexes{Key: "ID", Int32: []int32{1001}, Strings: []string{"W3.TEST.P1"}}
	if err := multipleValueTypes.Validate(); err == nil || !operror.IsKind(err, operror.KindValidation) {
		t.Fatalf("expected multiple index value type validation error, got %v", err)
	}

	valid := base
	valid.Indexes = &Indexes{Key: "GN", Strings: []string{"W3.TEST.P1"}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid indexed mutation rejected: %v", err)
	}
}
