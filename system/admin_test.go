package system

import (
	"testing"

	"github.com/tc252617228/openplant/admin"
	"github.com/tc252617228/openplant/model"
)

func TestBuildPointTemplateInsert(t *testing.T) {
	template, ok := LookupPointTemplate(MetricLoad, "W3")
	if !ok {
		t.Fatalf("LOAD template not found")
	}
	req, err := BuildPointTemplateInsert("W3", 12, []PointTemplate{template})
	if err != nil {
		t.Fatalf("BuildPointTemplateInsert failed: %v", err)
	}
	if req.DB != "W3" || req.Table != "Point" || req.Action != admin.MutationInsert {
		t.Fatalf("unexpected mutation identity: %#v", req)
	}
	if len(req.Columns) == 0 || len(req.Rows) != 1 {
		t.Fatalf("unexpected mutation shape: %#v", req)
	}
	row := req.Rows[0]
	if row["PN"] != "LOAD" || row["ND"] != int32(12) || row["PT"] != int8(model.SourceCalc) || row["RT"] != int8(model.TypeAX) {
		t.Fatalf("unexpected row identity: %#v", row)
	}
	if row["LC"] != int16(model.AlarmLimitMask) || row["EX"] != "return op.load()" {
		t.Fatalf("unexpected alarm/formula row: %#v", row)
	}
	if pointTemplateColumnType(req.Columns, "BV") != admin.ColumnFloat32 || pointTemplateColumnType(req.Columns, "FK") != admin.ColumnFloat32 {
		t.Fatalf("point template float columns should use config-table float32 types: %#v", req.Columns)
	}
}

func TestBuildDefaultPointTemplateInsert(t *testing.T) {
	req, err := BuildDefaultPointTemplateInsert("W3", 1)
	if err != nil {
		t.Fatalf("BuildDefaultPointTemplateInsert failed: %v", err)
	}
	if len(req.Rows) != len(Metrics()) {
		t.Fatalf("rows=%d metrics=%d", len(req.Rows), len(Metrics()))
	}
	if err := req.Validate(); err != nil {
		t.Fatalf("mutation validation failed: %v", err)
	}
}

func TestBuildPointTemplateMutationRejectsUnsafeInput(t *testing.T) {
	template, _ := LookupPointTemplate(MetricLoad, "W3")
	if _, err := BuildPointTemplateInsert("W3", -1, []PointTemplate{template}); err == nil {
		t.Fatalf("expected negative node error")
	}
	if _, err := BuildPointTemplateInsert("W3", 1, nil); err == nil {
		t.Fatalf("expected empty template error")
	}
	template.GN = "OTHER.SYS.LOAD"
	if _, err := BuildPointTemplateInsert("W3", 1, []PointTemplate{template}); err == nil {
		t.Fatalf("expected wrong database error")
	}
	template.GN = "W3.SYS.LOAD"
	if _, err := BuildPointTemplateInsert("W3", 1, []PointTemplate{template, template}); err == nil {
		t.Fatalf("expected duplicate template error")
	}
}

func pointTemplateColumnType(columns []admin.Column, name string) admin.ColumnType {
	for _, column := range columns {
		if column.Name == name {
			return column.Type
		}
	}
	return admin.ColumnNull
}
