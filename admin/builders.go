package admin

import (
	"fmt"
	"strings"

	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
)

const (
	nodeNameMaxBytes  = 24
	pointNameMaxBytes = 32
)

type UserCredential struct {
	Name     string
	Password string
}

func BuildNodeInsert(db model.DatabaseName, nodes []model.Node) (TableMutation, error) {
	return buildNodeMutation(db, MutationInsert, nodes)
}

func BuildNodeReplace(db model.DatabaseName, nodes []model.Node) (TableMutation, error) {
	return buildNodeMutation(db, MutationReplace, nodes)
}

func BuildNodeDelete(db model.DatabaseName, nodes []model.Node) (TableMutation, error) {
	if err := db.Validate(); err != nil {
		return TableMutation{}, err
	}
	if len(nodes) == 0 {
		return TableMutation{}, operror.Validation("admin.BuildNodeDelete", "at least one node is required")
	}
	ids := make([]int32, 0, len(nodes))
	seen := make(map[model.NodeID]struct{}, len(nodes))
	for _, node := range nodes {
		if node.ID <= 0 {
			return TableMutation{}, operror.Validation("admin.BuildNodeDelete", "node delete requires a positive node ID")
		}
		if _, ok := seen[node.ID]; ok {
			return TableMutation{}, operror.Validation("admin.BuildNodeDelete", fmt.Sprintf("duplicate node ID: %d", node.ID))
		}
		seen[node.ID] = struct{}{}
		ids = append(ids, int32(node.ID))
	}
	req := TableMutation{
		DB:      db,
		Table:   "Node",
		Action:  MutationDelete,
		Filters: int32DeleteFilters("ID", ids),
	}
	if err := req.Validate(); err != nil {
		return TableMutation{}, err
	}
	return req, nil
}

func BuildPointConfigInsert(db model.DatabaseName, points []model.PointConfig) (TableMutation, error) {
	return buildPointConfigMutation(db, MutationInsert, points)
}

func BuildPointConfigReplace(db model.DatabaseName, points []model.PointConfig) (TableMutation, error) {
	return buildPointConfigMutation(db, MutationReplace, points)
}

func BuildPointConfigDelete(db model.DatabaseName, points []model.PointConfig) (TableMutation, error) {
	if err := db.Validate(); err != nil {
		return TableMutation{}, err
	}
	if len(points) == 0 {
		return TableMutation{}, operror.Validation("admin.BuildPointConfigDelete", "at least one point config is required")
	}
	ids := make([]int32, 0, len(points))
	seen := make(map[model.PointID]struct{}, len(points))
	for _, point := range points {
		if point.ID <= 0 {
			return TableMutation{}, operror.Validation("admin.BuildPointConfigDelete", "point delete requires a positive point ID")
		}
		if _, ok := seen[point.ID]; ok {
			return TableMutation{}, operror.Validation("admin.BuildPointConfigDelete", fmt.Sprintf("duplicate point ID: %d", point.ID))
		}
		seen[point.ID] = struct{}{}
		ids = append(ids, int32(point.ID))
	}
	req := TableMutation{
		DB:      db,
		Table:   "Point",
		Action:  MutationDelete,
		Filters: int32DeleteFilters("ID", ids),
	}
	if err := req.Validate(); err != nil {
		return TableMutation{}, err
	}
	return req, nil
}

func BuildReplicatorInsert(db model.DatabaseName, replicators []model.Replicator) (TableMutation, error) {
	return buildReplicatorMutation(db, MutationInsert, replicators)
}

func BuildReplicatorReplace(db model.DatabaseName, replicators []model.Replicator) (TableMutation, error) {
	return buildReplicatorMutation(db, MutationReplace, replicators)
}

func BuildRepItemInsert(db model.DatabaseName, items []model.RepItem) (TableMutation, error) {
	return buildRepItemMutation(db, MutationInsert, items)
}

func BuildRepItemReplace(db model.DatabaseName, items []model.RepItem) (TableMutation, error) {
	return buildRepItemMutation(db, MutationReplace, items)
}

func BuildUserInsert(db model.DatabaseName, users []UserCredential) (TableMutation, error) {
	return buildUserMutation(db, MutationInsert, users)
}

func BuildUserReplace(db model.DatabaseName, users []UserCredential) (TableMutation, error) {
	return buildUserMutation(db, MutationReplace, users)
}

func BuildUserDelete(db model.DatabaseName, users []model.User) (TableMutation, error) {
	if err := db.Validate(); err != nil {
		return TableMutation{}, err
	}
	if len(users) == 0 {
		return TableMutation{}, operror.Validation("admin.BuildUserDelete", "at least one user is required")
	}
	names := make([]string, 0, len(users))
	seen := make(map[string]struct{}, len(users))
	for _, user := range users {
		if err := validateRequiredText("admin.BuildUserDelete", "user name", user.Name); err != nil {
			return TableMutation{}, err
		}
		if _, ok := seen[user.Name]; ok {
			return TableMutation{}, operror.Validation("admin.BuildUserDelete", "duplicate user: "+user.Name)
		}
		seen[user.Name] = struct{}{}
		names = append(names, user.Name)
	}
	req := TableMutation{
		DB:     db,
		Table:  "User",
		Action: MutationDelete,
		Filters: []Filter{{
			Left:     "US",
			Operator: FilterIn,
			Right:    stringFilterList(names),
			Relation: FilterAnd,
		}},
	}
	if err := req.Validate(); err != nil {
		return TableMutation{}, err
	}
	return req, nil
}

func BuildGroupInsert(db model.DatabaseName, groups []model.Group) (TableMutation, error) {
	return buildGroupMutation(db, MutationInsert, groups)
}

func BuildGroupReplace(db model.DatabaseName, groups []model.Group) (TableMutation, error) {
	return buildGroupMutation(db, MutationReplace, groups)
}

func BuildAccessInsert(db model.DatabaseName, entries []model.Access) (TableMutation, error) {
	return buildAccessMutation(db, MutationInsert, entries)
}

func BuildAccessReplace(db model.DatabaseName, entries []model.Access) (TableMutation, error) {
	return buildAccessMutation(db, MutationReplace, entries)
}

func buildNodeMutation(db model.DatabaseName, action MutationAction, nodes []model.Node) (TableMutation, error) {
	if err := db.Validate(); err != nil {
		return TableMutation{}, err
	}
	if action != MutationInsert && action != MutationReplace {
		return TableMutation{}, operror.Validation("admin.buildNodeMutation", "node builders support insert or replace only")
	}
	if len(nodes) == 0 {
		return TableMutation{}, operror.Validation("admin.buildNodeMutation", "at least one node is required")
	}
	rows := make([]Row, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if err := validateNodeConfig(node); err != nil {
			return TableMutation{}, err
		}
		key := fmt.Sprintf("%d/%s", node.ParentID, node.Name)
		if _, ok := seen[key]; ok {
			return TableMutation{}, operror.Validation("admin.buildNodeMutation", "duplicate node name under parent: "+node.Name)
		}
		seen[key] = struct{}{}
		rows = append(rows, nodeRow(node))
	}
	req := TableMutation{
		DB:      db,
		Table:   "Node",
		Action:  action,
		Columns: nodeColumns(),
		Rows:    rows,
	}
	if err := req.Validate(); err != nil {
		return TableMutation{}, err
	}
	return req, nil
}

func buildPointConfigMutation(db model.DatabaseName, action MutationAction, points []model.PointConfig) (TableMutation, error) {
	if err := db.Validate(); err != nil {
		return TableMutation{}, err
	}
	if action != MutationInsert && action != MutationReplace {
		return TableMutation{}, operror.Validation("admin.buildPointConfigMutation", "point builders support insert or replace only")
	}
	if len(points) == 0 {
		return TableMutation{}, operror.Validation("admin.buildPointConfigMutation", "at least one point config is required")
	}
	rows := make([]Row, 0, len(points))
	seen := make(map[string]struct{}, len(points))
	for _, point := range points {
		if err := validatePointConfig(point); err != nil {
			return TableMutation{}, err
		}
		key := fmt.Sprintf("%d/%s", point.NodeID, point.Name)
		if _, ok := seen[key]; ok {
			return TableMutation{}, operror.Validation("admin.buildPointConfigMutation", "duplicate point name under node: "+point.Name)
		}
		seen[key] = struct{}{}
		rows = append(rows, pointConfigRow(point))
	}
	req := TableMutation{
		DB:      db,
		Table:   "Point",
		Action:  action,
		Columns: pointConfigColumns(),
		Rows:    rows,
	}
	if err := req.Validate(); err != nil {
		return TableMutation{}, err
	}
	return req, nil
}

func buildReplicatorMutation(db model.DatabaseName, action MutationAction, replicators []model.Replicator) (TableMutation, error) {
	if err := db.Validate(); err != nil {
		return TableMutation{}, err
	}
	if action != MutationInsert && action != MutationReplace {
		return TableMutation{}, operror.Validation("admin.buildReplicatorMutation", "replicator builders support insert or replace only")
	}
	if len(replicators) == 0 {
		return TableMutation{}, operror.Validation("admin.buildReplicatorMutation", "at least one replicator is required")
	}
	rows := make([]Row, 0, len(replicators))
	seen := make(map[string]struct{}, len(replicators))
	for _, replicator := range replicators {
		if err := replicator.Validate(); err != nil {
			return TableMutation{}, operror.Validation("admin.buildReplicatorMutation", err.Error())
		}
		if _, ok := seen[replicator.Name]; ok {
			return TableMutation{}, operror.Validation("admin.buildReplicatorMutation", "duplicate replicator: "+replicator.Name)
		}
		seen[replicator.Name] = struct{}{}
		rows = append(rows, replicatorRow(replicator))
	}
	req := TableMutation{
		DB:      db,
		Table:   "Replicator",
		Action:  action,
		Columns: replicatorColumns(),
		Rows:    rows,
	}
	if err := req.Validate(); err != nil {
		return TableMutation{}, err
	}
	return req, nil
}

func buildRepItemMutation(db model.DatabaseName, action MutationAction, items []model.RepItem) (TableMutation, error) {
	if err := db.Validate(); err != nil {
		return TableMutation{}, err
	}
	if action != MutationInsert && action != MutationReplace {
		return TableMutation{}, operror.Validation("admin.buildRepItemMutation", "replication item builders support insert or replace only")
	}
	if len(items) == 0 {
		return TableMutation{}, operror.Validation("admin.buildRepItemMutation", "at least one replication item is required")
	}
	rows := make([]Row, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if err := item.Validate(); err != nil {
			return TableMutation{}, operror.Validation("admin.buildRepItemMutation", err.Error())
		}
		if _, ok := seen[item.PointName]; ok {
			return TableMutation{}, operror.Validation("admin.buildRepItemMutation", "duplicate replication source point: "+item.PointName)
		}
		seen[item.PointName] = struct{}{}
		rows = append(rows, repItemRow(item))
	}
	req := TableMutation{
		DB:      db,
		Table:   "RepItem",
		Action:  action,
		Columns: repItemColumns(),
		Rows:    rows,
	}
	if err := req.Validate(); err != nil {
		return TableMutation{}, err
	}
	return req, nil
}

func buildUserMutation(db model.DatabaseName, action MutationAction, users []UserCredential) (TableMutation, error) {
	if err := db.Validate(); err != nil {
		return TableMutation{}, err
	}
	if action != MutationInsert && action != MutationReplace {
		return TableMutation{}, operror.Validation("admin.buildUserMutation", "user builders support insert or replace only")
	}
	if len(users) == 0 {
		return TableMutation{}, operror.Validation("admin.buildUserMutation", "at least one user is required")
	}
	rows := make([]Row, 0, len(users))
	seen := make(map[string]struct{}, len(users))
	for _, user := range users {
		if err := validateRequiredText("admin.buildUserMutation", "user name", user.Name); err != nil {
			return TableMutation{}, err
		}
		if strings.TrimSpace(user.Password) == "" {
			return TableMutation{}, operror.Validation("admin.buildUserMutation", "password is required")
		}
		if _, ok := seen[user.Name]; ok {
			return TableMutation{}, operror.Validation("admin.buildUserMutation", "duplicate user: "+user.Name)
		}
		seen[user.Name] = struct{}{}
		rows = append(rows, Row{"US": user.Name, "PW": user.Password})
	}
	req := TableMutation{
		DB:      db,
		Table:   "User",
		Action:  action,
		Columns: []Column{{Name: "US", Type: ColumnString}, {Name: "PW", Type: ColumnString}},
		Rows:    rows,
	}
	if err := req.Validate(); err != nil {
		return TableMutation{}, err
	}
	return req, nil
}

func buildGroupMutation(db model.DatabaseName, action MutationAction, groups []model.Group) (TableMutation, error) {
	if err := db.Validate(); err != nil {
		return TableMutation{}, err
	}
	if action != MutationInsert && action != MutationReplace {
		return TableMutation{}, operror.Validation("admin.buildGroupMutation", "group builders support insert or replace only")
	}
	if len(groups) == 0 {
		return TableMutation{}, operror.Validation("admin.buildGroupMutation", "at least one group is required")
	}
	rows := make([]Row, 0, len(groups))
	seenID := make(map[int32]struct{}, len(groups))
	seenName := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		if group.ID < 0 {
			return TableMutation{}, operror.Validation("admin.buildGroupMutation", "negative group IDs are system-owned and cannot be changed")
		}
		if err := validateRequiredText("admin.buildGroupMutation", "group name", group.Name); err != nil {
			return TableMutation{}, err
		}
		if _, ok := seenID[group.ID]; ok {
			return TableMutation{}, operror.Validation("admin.buildGroupMutation", fmt.Sprintf("duplicate group ID: %d", group.ID))
		}
		if _, ok := seenName[group.Name]; ok {
			return TableMutation{}, operror.Validation("admin.buildGroupMutation", "duplicate group name: "+group.Name)
		}
		seenID[group.ID] = struct{}{}
		seenName[group.Name] = struct{}{}
		rows = append(rows, Row{"ID": group.ID, "GP": group.Name})
	}
	req := TableMutation{
		DB:      db,
		Table:   "Groups",
		Action:  action,
		Columns: []Column{{Name: "ID", Type: ColumnInt32}, {Name: "GP", Type: ColumnString}},
		Rows:    rows,
	}
	if err := req.Validate(); err != nil {
		return TableMutation{}, err
	}
	return req, nil
}

func buildAccessMutation(db model.DatabaseName, action MutationAction, entries []model.Access) (TableMutation, error) {
	if err := db.Validate(); err != nil {
		return TableMutation{}, err
	}
	if action != MutationInsert && action != MutationReplace {
		return TableMutation{}, operror.Validation("admin.buildAccessMutation", "access builders support insert or replace only")
	}
	if len(entries) == 0 {
		return TableMutation{}, operror.Validation("admin.buildAccessMutation", "at least one access entry is required")
	}
	rows := make([]Row, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if err := validateRequiredText("admin.buildAccessMutation", "user name", entry.User); err != nil {
			return TableMutation{}, err
		}
		if err := validateRequiredText("admin.buildAccessMutation", "group", entry.Group); err != nil {
			return TableMutation{}, err
		}
		if err := validateRequiredText("admin.buildAccessMutation", "privilege", entry.Privilege); err != nil {
			return TableMutation{}, err
		}
		key := entry.User + "\x00" + entry.Group + "\x00" + entry.Privilege
		if _, ok := seen[key]; ok {
			return TableMutation{}, operror.Validation("admin.buildAccessMutation", "duplicate access entry")
		}
		seen[key] = struct{}{}
		rows = append(rows, Row{"US": entry.User, "GP": entry.Group, "PL": entry.Privilege})
	}
	req := TableMutation{
		DB:      db,
		Table:   "Access",
		Action:  action,
		Columns: []Column{{Name: "US", Type: ColumnString}, {Name: "GP", Type: ColumnString}, {Name: "PL", Type: ColumnString}},
		Rows:    rows,
	}
	if err := req.Validate(); err != nil {
		return TableMutation{}, err
	}
	return req, nil
}

func validateNodeConfig(node model.Node) error {
	if err := validateRequiredName("admin.validateNodeConfig", "node", node.Name, nodeNameMaxBytes); err != nil {
		return err
	}
	if node.ParentID < 0 {
		return operror.Validation("admin.validateNodeConfig", "parent node ID cannot be negative")
	}
	if !node.AlarmCode.ValidForPointType(model.TypeAX) {
		return operror.Validation("admin.validateNodeConfig", "node alarm code is invalid")
	}
	return nil
}

func validatePointConfig(point model.PointConfig) error {
	if err := validateRequiredName("admin.validatePointConfig", "point", point.Name, pointNameMaxBytes); err != nil {
		return err
	}
	if point.NodeID < 0 {
		return operror.Validation("admin.validatePointConfig", "node ID cannot be negative")
	}
	if point.DeviceID < 0 {
		return operror.Validation("admin.validatePointConfig", "device ID cannot be negative")
	}
	if !point.Source.Valid() {
		return operror.Validation("admin.validatePointConfig", "point source is invalid")
	}
	if !point.Type.Valid() {
		return operror.Validation("admin.validatePointConfig", "point type is invalid")
	}
	if point.Source == model.SourceCalc && strings.TrimSpace(point.Expression) == "" {
		return operror.Validation("admin.validatePointConfig", "calculation point requires expression")
	}
	if !point.AlarmCode.ValidForPointType(point.Type) {
		return operror.Validation("admin.validatePointConfig", "alarm code is invalid for point type")
	}
	if point.Type.SupportsAnalogAlarms() {
		if err := model.ValidateAnalogAlarmLimits(point.AlarmCode, point.Limits); err != nil {
			return operror.Validation("admin.validatePointConfig", err.Error())
		}
	}
	if !point.AlarmLevel.Valid() {
		return operror.Validation("admin.validatePointConfig", "alarm priority is invalid")
	}
	if !point.DeadbandType.Valid() {
		return operror.Validation("admin.validatePointConfig", "deadband type is invalid")
	}
	if !point.Compression.Valid() {
		return operror.Validation("admin.validatePointConfig", "compression mode is invalid")
	}
	return nil
}

func validateRequiredName(op, label, name string, maxBytes int) error {
	if err := validateRequiredText(op, label+" name", name); err != nil {
		return err
	}
	if len([]byte(name)) > maxBytes {
		return operror.Validation(op, fmt.Sprintf("%s name exceeds %d bytes", label, maxBytes))
	}
	return nil
}

func validateRequiredText(op, label, value string) error {
	if strings.TrimSpace(value) == "" {
		return operror.Validation(op, label+" is required")
	}
	return nil
}

func int32FilterList(values []int32) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d", value))
	}
	return "(" + strings.Join(parts, ",") + ")"
}

func int32DeleteFilters(column string, values []int32) []Filter {
	if len(values) == 1 {
		return []Filter{{
			Left:     column,
			Operator: FilterEQ,
			Right:    fmt.Sprintf("%d", values[0]),
			Relation: FilterAnd,
		}}
	}
	return []Filter{{
		Left:     column,
		Operator: FilterIn,
		Right:    int32FilterList(values),
		Relation: FilterAnd,
	}}
}

func stringFilterList(values []string) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, quoteFilterString(value))
	}
	return "(" + strings.Join(parts, ",") + ")"
}

func quoteFilterString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func replicatorColumns() []Column {
	return []Column{
		{Name: "RN", Type: ColumnString},
		{Name: "IP", Type: ColumnString},
		{Name: "PO", Type: ColumnInt32},
		{Name: "SP", Type: ColumnInt32},
		{Name: "SY", Type: ColumnInt32},
		{Name: "FL", Type: ColumnBool},
		{Name: "AR", Type: ColumnBool},
		{Name: "TL", Type: ColumnInt32},
	}
}

func replicatorRow(replicator model.Replicator) Row {
	return Row{
		"RN": replicator.Name,
		"IP": replicator.IP,
		"PO": replicator.Port,
		"SP": replicator.SourcePort,
		"SY": int32(replicator.SyncMode),
		"FL": replicator.FilterUnchanged,
		"AR": replicator.ArchiveBackfill,
		"TL": replicator.TimeLimitDays,
	}
}

func repItemColumns() []Column {
	return []Column{
		{Name: "PN", Type: ColumnString},
		{Name: "TN", Type: ColumnString},
		{Name: "XF", Type: ColumnInt32},
	}
}

func repItemRow(item model.RepItem) Row {
	return Row{
		"PN": item.PointName,
		"TN": item.TargetName,
		"XF": int32(item.Transform),
	}
}

func nodeColumns() []Column {
	return []Column{
		{Name: "PN", Type: ColumnString},
		{Name: "ND", Type: ColumnInt32},
		{Name: "ED", Type: ColumnString},
		{Name: "FQ", Type: ColumnInt32},
		{Name: "LC", Type: ColumnInt32},
		{Name: "AR", Type: ColumnBool},
		{Name: "OF", Type: ColumnBool},
		{Name: "LO", Type: ColumnBool},
	}
}

func nodeRow(node model.Node) Row {
	return Row{
		"PN": node.Name,
		"ND": int32(node.ParentID),
		"ED": node.Description,
		"FQ": node.Resolution,
		"LC": int32(node.AlarmCode),
		"AR": node.Archived,
		"OF": node.Offline,
		"LO": node.Internal,
	}
}

func pointConfigColumns() []Column {
	return []Column{
		{Name: "PN", Type: ColumnString},
		{Name: "ND", Type: ColumnInt32},
		{Name: "CD", Type: ColumnInt32},
		{Name: "PT", Type: ColumnInt8},
		{Name: "RT", Type: ColumnInt8},
		{Name: "AN", Type: ColumnString},
		{Name: "ED", Type: ColumnString},
		{Name: "KR", Type: ColumnString},
		{Name: "SG", Type: ColumnBinary, Length: 4},
		{Name: "FQ", Type: ColumnInt16},
		{Name: "CP", Type: ColumnInt16},
		{Name: "HW", Type: ColumnInt32},
		{Name: "BP", Type: ColumnInt16},
		{Name: "SR", Type: ColumnString},
		{Name: "AD", Type: ColumnString},
		{Name: "LC", Type: ColumnInt16},
		{Name: "AP", Type: ColumnInt8},
		{Name: "AR", Type: ColumnBool},
		{Name: "OF", Type: ColumnBool},
		{Name: "FL", Type: ColumnInt32},
		{Name: "ST", Type: ColumnString},
		{Name: "RS", Type: ColumnString},
		{Name: "EU", Type: ColumnString},
		{Name: "FM", Type: ColumnInt16},
		{Name: "IV", Type: ColumnFloat32},
		{Name: "BV", Type: ColumnFloat32},
		{Name: "TV", Type: ColumnFloat32},
		{Name: "LL", Type: ColumnFloat32},
		{Name: "HL", Type: ColumnFloat32},
		{Name: "ZL", Type: ColumnFloat32},
		{Name: "ZH", Type: ColumnFloat32},
		{Name: "L3", Type: ColumnFloat32},
		{Name: "H3", Type: ColumnFloat32},
		{Name: "L4", Type: ColumnFloat32},
		{Name: "H4", Type: ColumnFloat32},
		{Name: "C1", Type: ColumnInt32},
		{Name: "C2", Type: ColumnInt32},
		{Name: "C3", Type: ColumnInt32},
		{Name: "C4", Type: ColumnInt32},
		{Name: "C5", Type: ColumnInt32},
		{Name: "C6", Type: ColumnInt32},
		{Name: "C7", Type: ColumnInt32},
		{Name: "C8", Type: ColumnInt32},
		{Name: "DB", Type: ColumnFloat32},
		{Name: "DT", Type: ColumnInt8},
		{Name: "KZ", Type: ColumnInt8},
		{Name: "TT", Type: ColumnInt8},
		{Name: "TP", Type: ColumnInt16},
		{Name: "OT", Type: ColumnInt16},
		{Name: "KT", Type: ColumnInt8},
		{Name: "KO", Type: ColumnInt8},
		{Name: "FK", Type: ColumnFloat32},
		{Name: "FB", Type: ColumnFloat32},
		{Name: "EX", Type: ColumnString},
	}
}

func pointConfigRow(point model.PointConfig) Row {
	return Row{
		"PN": point.Name,
		"ND": int32(point.NodeID),
		"CD": int32(point.DeviceID),
		"PT": int8(point.Source),
		"RT": int8(point.Type),
		"AN": point.Alias,
		"ED": point.Description,
		"KR": point.Keyword,
		"SG": point.Security.Bytes(),
		"FQ": point.Resolution,
		"CP": point.Processor,
		"HW": point.HardwareAddress,
		"BP": point.Channel,
		"SR": point.SignalType,
		"AD": point.SignalAddress,
		"LC": int16(point.AlarmCode),
		"AP": int8(point.AlarmLevel),
		"AR": point.Archived,
		"OF": point.Offline,
		"FL": point.Flags,
		"ST": point.SetDescription,
		"RS": point.ResetDescription,
		"EU": point.Unit,
		"FM": point.Format,
		"IV": point.InitialValue,
		"BV": point.RangeLower,
		"TV": point.RangeUpper,
		"LL": point.Limits.LL,
		"HL": point.Limits.HL,
		"ZL": point.Limits.ZL,
		"ZH": point.Limits.ZH,
		"L3": point.Limits.L3,
		"H3": point.Limits.H3,
		"L4": point.Limits.L4,
		"H4": point.Limits.H4,
		"C1": point.Colors.LL,
		"C2": point.Colors.ZL,
		"C3": point.Colors.L3,
		"C4": point.Colors.L4,
		"C5": point.Colors.HL,
		"C6": point.Colors.ZH,
		"C7": point.Colors.H3,
		"C8": point.Colors.H4,
		"DB": point.Deadband,
		"DT": int8(point.DeadbandType),
		"KZ": int8(point.Compression),
		"TT": point.StatType,
		"TP": point.StatPeriod,
		"OT": point.StatOffset,
		"KT": point.CalcType,
		"KO": point.CalcOrder,
		"FK": point.ScaleFactor,
		"FB": point.Offset,
		"EX": point.Expression,
	}
}
