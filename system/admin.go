package system

import (
	"fmt"

	"github.com/tc252617228/openplant/admin"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
)

func BuildPointTemplateInsert(db model.DatabaseName, nodeID model.NodeID, templates []PointTemplate) (admin.TableMutation, error) {
	return buildPointTemplateMutation(db, nodeID, admin.MutationInsert, templates)
}

func BuildPointTemplateReplace(db model.DatabaseName, nodeID model.NodeID, templates []PointTemplate) (admin.TableMutation, error) {
	return buildPointTemplateMutation(db, nodeID, admin.MutationReplace, templates)
}

func BuildDefaultPointTemplateInsert(db model.DatabaseName, nodeID model.NodeID) (admin.TableMutation, error) {
	templates, err := PointTemplates(db)
	if err != nil {
		return admin.TableMutation{}, err
	}
	return BuildPointTemplateInsert(db, nodeID, templates)
}

func buildPointTemplateMutation(db model.DatabaseName, nodeID model.NodeID, action admin.MutationAction, templates []PointTemplate) (admin.TableMutation, error) {
	if err := db.Validate(); err != nil {
		return admin.TableMutation{}, err
	}
	if nodeID < 0 {
		return admin.TableMutation{}, operror.Validation("system.buildPointTemplateMutation", "node ID cannot be negative")
	}
	if len(templates) == 0 {
		return admin.TableMutation{}, operror.Validation("system.buildPointTemplateMutation", "at least one point template is required")
	}
	if action != admin.MutationInsert && action != admin.MutationReplace {
		return admin.TableMutation{}, operror.Validation("system.buildPointTemplateMutation", "system point templates support insert or replace mutations only")
	}
	rows := make([]admin.Row, 0, len(templates))
	seen := make(map[model.GN]struct{}, len(templates))
	for _, template := range templates {
		if template.GN == "" {
			template.GN = template.Metric.GN(db)
		}
		if err := template.GN.Validate(); err != nil {
			return admin.TableMutation{}, err
		}
		if template.GN.Database() != db {
			return admin.TableMutation{}, operror.Validation("system.buildPointTemplateMutation", fmt.Sprintf("template GN %s does not belong to database %s", template.GN, db))
		}
		if _, ok := seen[template.GN]; ok {
			return admin.TableMutation{}, operror.Validation("system.buildPointTemplateMutation", "duplicate point template GN: "+string(template.GN))
		}
		seen[template.GN] = struct{}{}
		rows = append(rows, pointTemplateRow(nodeID, template))
	}
	req := admin.TableMutation{
		DB:      db,
		Table:   "Point",
		Action:  action,
		Columns: pointTemplateColumns(),
		Rows:    rows,
	}
	if err := req.Validate(); err != nil {
		return admin.TableMutation{}, err
	}
	return req, nil
}

func pointTemplateColumns() []admin.Column {
	return []admin.Column{
		{Name: "PN", Type: admin.ColumnString},
		{Name: "ND", Type: admin.ColumnInt32},
		{Name: "PT", Type: admin.ColumnInt8},
		{Name: "RT", Type: admin.ColumnInt8},
		{Name: "ED", Type: admin.ColumnString},
		{Name: "LC", Type: admin.ColumnInt16},
		{Name: "AP", Type: admin.ColumnInt8},
		{Name: "AR", Type: admin.ColumnBool},
		{Name: "EU", Type: admin.ColumnString},
		{Name: "FM", Type: admin.ColumnInt16},
		{Name: "BV", Type: admin.ColumnFloat32},
		{Name: "TV", Type: admin.ColumnFloat32},
		{Name: "LL", Type: admin.ColumnFloat32},
		{Name: "HL", Type: admin.ColumnFloat32},
		{Name: "ZL", Type: admin.ColumnFloat32},
		{Name: "ZH", Type: admin.ColumnFloat32},
		{Name: "L3", Type: admin.ColumnFloat32},
		{Name: "H3", Type: admin.ColumnFloat32},
		{Name: "L4", Type: admin.ColumnFloat32},
		{Name: "H4", Type: admin.ColumnFloat32},
		{Name: "DB", Type: admin.ColumnFloat32},
		{Name: "DT", Type: admin.ColumnInt8},
		{Name: "KZ", Type: admin.ColumnInt8},
		{Name: "KT", Type: admin.ColumnInt8},
		{Name: "KO", Type: admin.ColumnInt8},
		{Name: "FK", Type: admin.ColumnFloat32},
		{Name: "FB", Type: admin.ColumnFloat32},
		{Name: "EX", Type: admin.ColumnString},
	}
}

func pointTemplateRow(nodeID model.NodeID, template PointTemplate) admin.Row {
	return admin.Row{
		"PN": template.Name,
		"ND": int32(nodeID),
		"PT": int8(template.Source),
		"RT": int8(template.Type),
		"ED": template.Description,
		"LC": int16(template.AlarmCode),
		"AP": int8(template.AlarmLevel),
		"AR": template.Archived,
		"EU": template.Unit,
		"FM": template.Format,
		"BV": template.RangeLower,
		"TV": template.RangeUpper,
		"LL": template.Limits.LL,
		"HL": template.Limits.HL,
		"ZL": template.Limits.ZL,
		"ZH": template.Limits.ZH,
		"L3": template.Limits.L3,
		"H3": template.Limits.H3,
		"L4": template.Limits.L4,
		"H4": template.Limits.H4,
		"DB": template.Deadband,
		"DT": int8(template.DeadbandType),
		"KZ": int8(template.Compression),
		"KT": template.CalcType,
		"KO": template.CalcOrder,
		"FK": template.ScaleFactor,
		"FB": template.Offset,
		"EX": template.Expression,
	}
}
