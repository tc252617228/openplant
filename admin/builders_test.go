package admin

import (
	"testing"

	"github.com/tc252617228/openplant/model"
)

func TestBuildNodeInsert(t *testing.T) {
	req, err := BuildNodeInsert("W3", []model.Node{{
		ParentID:    0,
		Name:        "SDK_NODE",
		Description: "node",
		Resolution:  5,
		AlarmCode:   model.AlarmLL,
		Archived:    true,
		Internal:    true,
	}})
	if err != nil {
		t.Fatalf("BuildNodeInsert failed: %v", err)
	}
	if req.DB != "W3" || req.Table != "Node" || req.Action != MutationInsert {
		t.Fatalf("unexpected request identity: %#v", req)
	}
	if len(req.Rows) != 1 || req.Rows[0]["PN"] != "SDK_NODE" || req.Rows[0]["ND"] != int32(0) || req.Rows[0]["LO"] != true {
		t.Fatalf("unexpected row: %#v", req.Rows)
	}
	if err := req.Validate(); err != nil {
		t.Fatalf("mutation validation failed: %v", err)
	}
}

func TestBuildNodeDeleteUsesPositiveIDFilter(t *testing.T) {
	req, err := BuildNodeDelete("W3", []model.Node{{ID: 12, Name: "SDK_NODE"}})
	if err != nil {
		t.Fatalf("BuildNodeDelete failed: %v", err)
	}
	if req.Table != "Node" || req.Action != MutationDelete || len(req.Filters) != 1 {
		t.Fatalf("unexpected node delete: %#v", req)
	}
	if req.Filters[0].Left != "ID" || req.Filters[0].Operator != FilterEQ || req.Filters[0].Right != "12" {
		t.Fatalf("unexpected node delete filter: %#v", req.Filters)
	}
}

func TestBuildNodeRejectsInvalidInput(t *testing.T) {
	if _, err := BuildNodeInsert("W3", nil); err == nil {
		t.Fatalf("expected empty nodes error")
	}
	if _, err := BuildNodeInsert("W3", []model.Node{{Name: ""}}); err == nil {
		t.Fatalf("expected empty name error")
	}
	if _, err := BuildNodeInsert("W3", []model.Node{{Name: "SDK_NODE", ParentID: -1}}); err == nil {
		t.Fatalf("expected negative parent error")
	}
	if _, err := BuildNodeInsert("W3", []model.Node{{Name: "SDK_NODE"}, {Name: "SDK_NODE"}}); err == nil {
		t.Fatalf("expected duplicate node error")
	}
	if _, err := BuildNodeDelete("W3", []model.Node{{ID: 0}}); err == nil {
		t.Fatalf("expected missing node ID delete error")
	}
	if _, err := BuildNodeDelete("W3", []model.Node{{ID: 12}, {ID: 12}}); err == nil {
		t.Fatalf("expected duplicate node ID delete error")
	}
}

func TestBuildPointConfigInsert(t *testing.T) {
	security, _ := model.SecurityGroups{}.With(1)
	req, err := BuildPointConfigInsert("W3", []model.PointConfig{{
		NodeID:      12,
		Source:      model.SourceCalc,
		Type:        model.TypeR8,
		Name:        "SDK_POINT",
		Alias:       "P",
		Description: "point",
		Keyword:     "K",
		Security:    security,
		Resolution:  1,
		AlarmCode:   model.AlarmHL,
		AlarmLevel:  model.AlarmPriorityYellow,
		Archived:    true,
		Unit:        "MW",
		Format:      2,
		RangeLower:  0,
		RangeUpper:  100,
		Limits:      model.AlarmLimits{HL: 90},
		Deadband:    0.1,
		Compression: model.PointCompressionDeadband,
		CalcType:    1,
		CalcOrder:   2,
		ScaleFactor: 1,
		Expression:  `return op.value("W3.NODE.P1")`,
	}})
	if err != nil {
		t.Fatalf("BuildPointConfigInsert failed: %v", err)
	}
	if req.DB != "W3" || req.Table != "Point" || req.Action != MutationInsert {
		t.Fatalf("unexpected request identity: %#v", req)
	}
	row := req.Rows[0]
	if row["PN"] != "SDK_POINT" || row["ND"] != int32(12) || row["PT"] != int8(model.SourceCalc) || row["RT"] != int8(model.TypeR8) {
		t.Fatalf("unexpected row identity: %#v", row)
	}
	if got := row["SG"].([]byte); len(got) != 4 || got[0] != 2 {
		t.Fatalf("unexpected security bytes: %#v", got)
	}
	if row["LC"] != int16(model.AlarmHL) || row["EX"] == "" {
		t.Fatalf("unexpected alarm/expression row: %#v", row)
	}
	if columnType(req.Columns, "BV") != ColumnFloat32 || columnType(req.Columns, "FK") != ColumnFloat32 {
		t.Fatalf("point config float columns should use config-table float32 types: %#v", req.Columns)
	}
	if err := req.Validate(); err != nil {
		t.Fatalf("mutation validation failed: %v", err)
	}
}

func TestBuildPointConfigDeleteUsesPositiveIDFilter(t *testing.T) {
	req, err := BuildPointConfigDelete("W3", []model.PointConfig{{ID: 1001}})
	if err != nil {
		t.Fatalf("BuildPointConfigDelete failed: %v", err)
	}
	if req.Table != "Point" || req.Action != MutationDelete || len(req.Filters) != 1 {
		t.Fatalf("unexpected point delete: %#v", req)
	}
	if req.Filters[0].Left != "ID" || req.Filters[0].Operator != FilterEQ || req.Filters[0].Right != "1001" {
		t.Fatalf("unexpected point delete filter: %#v", req.Filters)
	}
}

func TestBuildPointConfigRejectsInvalidInput(t *testing.T) {
	valid := model.PointConfig{
		NodeID:      1,
		Source:      model.SourceDAS,
		Type:        model.TypeR8,
		Name:        "P1",
		AlarmLevel:  model.AlarmPriorityUnset,
		Compression: model.PointCompressionDeadband,
	}
	if _, err := BuildPointConfigInsert("W3", nil); err == nil {
		t.Fatalf("expected empty point error")
	}
	invalid := valid
	invalid.Type = model.TypeUnknown
	if _, err := BuildPointConfigInsert("W3", []model.PointConfig{invalid}); err == nil {
		t.Fatalf("expected invalid type error")
	}
	invalid = valid
	invalid.Source = model.SourceCalc
	if _, err := BuildPointConfigInsert("W3", []model.PointConfig{invalid}); err == nil {
		t.Fatalf("expected missing expression error")
	}
	invalid = valid
	invalid.Type = model.TypeDX
	invalid.AlarmCode = model.AlarmL3
	if _, err := BuildPointConfigInsert("W3", []model.PointConfig{invalid}); err == nil {
		t.Fatalf("expected invalid alarm for DX error")
	}
	if _, err := BuildPointConfigInsert("W3", []model.PointConfig{valid, valid}); err == nil {
		t.Fatalf("expected duplicate point error")
	}
	if _, err := BuildPointConfigDelete("W3", []model.PointConfig{{ID: 0}}); err == nil {
		t.Fatalf("expected missing point ID delete error")
	}
	if _, err := BuildPointConfigDelete("W3", []model.PointConfig{{ID: 1001}, {ID: 1001}}); err == nil {
		t.Fatalf("expected duplicate point ID delete error")
	}
}

func TestBuildReplicatorInsert(t *testing.T) {
	req, err := BuildReplicatorInsert("W3", []model.Replicator{{
		Name:            "R1",
		IP:              "127.0.0.1",
		Port:            8200,
		SourcePort:      8200,
		SyncMode:        model.ReplicationSyncPreserveID,
		FilterUnchanged: true,
		ArchiveBackfill: true,
		TimeLimitDays:   model.ReplicationBackfillMaxDays,
	}})
	if err != nil {
		t.Fatalf("BuildReplicatorInsert failed: %v", err)
	}
	if req.Table != "Replicator" || req.Action != MutationInsert || len(req.Rows) != 1 {
		t.Fatalf("unexpected request: %#v", req)
	}
	row := req.Rows[0]
	if row["RN"] != "R1" || row["SY"] != int32(model.ReplicationSyncPreserveID) || row["FL"] != true {
		t.Fatalf("unexpected row: %#v", row)
	}
}

func TestBuildReplicatorRejectsInvalidInput(t *testing.T) {
	valid := model.Replicator{Name: "R1", SyncMode: model.ReplicationSyncLoose}
	if _, err := BuildReplicatorInsert("W3", nil); err == nil {
		t.Fatalf("expected empty replicator error")
	}
	if _, err := BuildReplicatorInsert("W3", []model.Replicator{{Name: "", SyncMode: model.ReplicationSyncLoose}}); err == nil {
		t.Fatalf("expected empty name error")
	}
	invalid := valid
	invalid.TimeLimitDays = model.ReplicationBackfillMaxDays + 1
	if _, err := BuildReplicatorInsert("W3", []model.Replicator{invalid}); err == nil {
		t.Fatalf("expected TL validation error")
	}
	if _, err := BuildReplicatorInsert("W3", []model.Replicator{valid, valid}); err == nil {
		t.Fatalf("expected duplicate replicator error")
	}
}

func TestBuildRepItemInsert(t *testing.T) {
	req, err := BuildRepItemInsert("W3", []model.RepItem{{
		PointName:  "W3.NODE.P1",
		TargetName: "REMOTE.NODE.P1",
		Transform:  model.ReplicationTransformCalcAsDAS,
	}})
	if err != nil {
		t.Fatalf("BuildRepItemInsert failed: %v", err)
	}
	row := req.Rows[0]
	if req.Table != "RepItem" || row["PN"] != "W3.NODE.P1" || row["XF"] != int32(model.ReplicationTransformCalcAsDAS) {
		t.Fatalf("unexpected request: %#v", req)
	}
}

func TestBuildRepItemRejectsInvalidInput(t *testing.T) {
	valid := model.RepItem{PointName: "W3.NODE.P1", TargetName: "REMOTE.NODE.P1", Transform: model.ReplicationTransformPreserveRole}
	if _, err := BuildRepItemInsert("W3", nil); err == nil {
		t.Fatalf("expected empty item error")
	}
	invalid := valid
	invalid.Transform = 9
	if _, err := BuildRepItemInsert("W3", []model.RepItem{invalid}); err == nil {
		t.Fatalf("expected invalid transform error")
	}
	if _, err := BuildRepItemInsert("W3", []model.RepItem{valid, valid}); err == nil {
		t.Fatalf("expected duplicate item error")
	}
}

func TestBuildUserBuilders(t *testing.T) {
	password := string([]byte{112, 119})
	req, err := BuildUserInsert("W3", []UserCredential{{Name: "sdk_user", Password: password}})
	if err != nil {
		t.Fatalf("BuildUserInsert failed: %v", err)
	}
	if req.Table != "User" || req.Rows[0]["US"] != "sdk_user" || req.Rows[0]["PW"] != password {
		t.Fatalf("unexpected user insert: %#v", req)
	}
	deleteReq, err := BuildUserDelete("W3", []model.User{{Name: "sdk_user"}})
	if err != nil {
		t.Fatalf("BuildUserDelete failed: %v", err)
	}
	if deleteReq.Action != MutationDelete || len(deleteReq.Filters) != 1 || deleteReq.Filters[0].Right != "('sdk_user')" {
		t.Fatalf("unexpected user delete: %#v", deleteReq)
	}
}

func TestBuildUserRejectsInvalidInput(t *testing.T) {
	password := string([]byte{112, 119})
	if _, err := BuildUserInsert("W3", nil); err == nil {
		t.Fatalf("expected empty user error")
	}
	if _, err := BuildUserInsert("W3", []UserCredential{{Name: "sdk_user"}}); err == nil {
		t.Fatalf("expected missing password error")
	}
	if _, err := BuildUserInsert("W3", []UserCredential{{Name: "sdk_user", Password: password}, {Name: "sdk_user", Password: password}}); err == nil {
		t.Fatalf("expected duplicate user error")
	}
	if _, err := BuildUserDelete("W3", []model.User{{Name: ""}}); err == nil {
		t.Fatalf("expected empty delete user error")
	}
}

func TestBuildGroupAndAccessBuilders(t *testing.T) {
	groupReq, err := BuildGroupInsert("W3", []model.Group{{ID: 10, Name: "operators"}})
	if err != nil {
		t.Fatalf("BuildGroupInsert failed: %v", err)
	}
	if groupReq.Table != "Groups" || groupReq.Rows[0]["ID"] != int32(10) || groupReq.Rows[0]["GP"] != "operators" {
		t.Fatalf("unexpected group request: %#v", groupReq)
	}
	accessReq, err := BuildAccessInsert("W3", []model.Access{{User: "sdk_user", Group: "operators", Privilege: "read"}})
	if err != nil {
		t.Fatalf("BuildAccessInsert failed: %v", err)
	}
	if accessReq.Table != "Access" || accessReq.Rows[0]["US"] != "sdk_user" || accessReq.Rows[0]["PL"] != "read" {
		t.Fatalf("unexpected access request: %#v", accessReq)
	}
}

func TestBuildGroupAndAccessRejectInvalidInput(t *testing.T) {
	if _, err := BuildGroupInsert("W3", []model.Group{{ID: -1, Name: "system"}}); err == nil {
		t.Fatalf("expected negative group error")
	}
	if _, err := BuildGroupInsert("W3", []model.Group{{ID: 1, Name: "ops"}, {ID: 1, Name: "other"}}); err == nil {
		t.Fatalf("expected duplicate group ID error")
	}
	if _, err := BuildAccessInsert("W3", nil); err == nil {
		t.Fatalf("expected empty access error")
	}
	if _, err := BuildAccessInsert("W3", []model.Access{{User: "sdk_user", Group: "", Privilege: "read"}}); err == nil {
		t.Fatalf("expected missing group error")
	}
	if _, err := BuildAccessInsert("W3", []model.Access{{User: "sdk_user", Group: "ops", Privilege: "read"}, {User: "sdk_user", Group: "ops", Privilege: "read"}}); err == nil {
		t.Fatalf("expected duplicate access error")
	}
}

func columnType(columns []Column, name string) ColumnType {
	for _, column := range columns {
		if column.Name == name {
			return column.Type
		}
	}
	return ColumnNull
}
