package alarm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tc252617228/openplant/internal/rowconv"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	sqlapi "github.com/tc252617228/openplant/sql"
)

const defaultActiveAlarmLimit = 1000

type Queryer interface {
	Query(ctx context.Context, query string) (sqlapi.Result, error)
}

type Options struct {
	Queryer Queryer
}

type HistoryQuery struct {
	DB    model.DatabaseName
	IDs   []model.PointID
	GNs   []model.GN
	Range model.TimeRange
	Limit int
}

func (q HistoryQuery) Validate() error {
	if err := q.DB.Validate(); err != nil {
		return err
	}
	if err := (model.PointSelector{IDs: q.IDs, GNs: q.GNs}).ValidateBounded(); err != nil {
		return err
	}
	if err := q.Range.Validate(); err != nil {
		return err
	}
	if q.Limit < 0 {
		return operror.Validation("alarm.HistoryQuery.Validate", "limit cannot be negative")
	}
	return nil
}

type Service struct {
	closed  error
	queryer Queryer
}

func NewService(opts ...Options) *Service {
	s := &Service{}
	if len(opts) > 0 {
		s.queryer = opts[0].Queryer
	}
	return s
}

func NewClosedService(err error) *Service {
	return &Service{closed: err}
}

func (s *Service) Active(ctx context.Context, db model.DatabaseName, limit int) ([]model.Alarm, error) {
	return s.ActiveSQL(ctx, db, limit)
}

func (s *Service) ActiveSQL(ctx context.Context, db model.DatabaseName, limit int) ([]model.Alarm, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := db.Validate(); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, operror.Validation("alarm.Service.Active", "limit cannot be negative")
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("alarm.Service.ActiveSQL", "SQL queryer is not configured")
	}
	query, err := buildActiveSQL(db, limit)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return alarmsFromRows(result.Rows), nil
}

func (s *Service) History(ctx context.Context, q HistoryQuery) ([]model.Alarm, error) {
	return s.HistorySQL(ctx, q)
}

func (s *Service) HistorySQL(ctx context.Context, q HistoryQuery) ([]model.Alarm, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("alarm.Service.HistorySQL", "SQL queryer is not configured")
	}
	query, err := buildHistorySQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return alarmsFromRows(result.Rows), nil
}

func buildActiveSQL(db model.DatabaseName, limit int) (string, error) {
	table, err := sqlapi.QualifiedTable(string(db), "Alarm")
	if err != nil {
		return "", err
	}
	columns, err := quoteColumns(alarmColumns())
	if err != nil {
		return "", err
	}
	if limit <= 0 {
		limit = defaultActiveAlarmLimit
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
		strings.Join(columns, ","),
		table,
		`"ID" >= 0`,
		`"TM" DESC,"ID" ASC`,
		limit,
	), nil
}

func buildHistorySQL(q HistoryQuery) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "AAlarm")
	if err != nil {
		return "", err
	}
	columns, err := quoteColumns(alarmColumns())
	if err != nil {
		return "", err
	}
	pointScope := pointScopeSQL(q.IDs, q.GNs)
	if pointScope == "" {
		return "", operror.Validation("alarm.buildHistorySQL", "point scope is required")
	}
	conditions := []string{
		pointScope,
		fmt.Sprintf(`"TM" BETWEEN %s AND %s`, timeLiteral(q.Range.Begin), timeLiteral(q.Range.End)),
	}
	limit := ""
	if q.Limit > 0 {
		limit = fmt.Sprintf(" LIMIT %d", q.Limit)
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s%s",
		strings.Join(columns, ","),
		table,
		strings.Join(conditions, " AND "),
		`"ID" ASC,"TM" ASC`,
		limit,
	), nil
}

func alarmsFromRows(rows []sqlapi.Row) []model.Alarm {
	alarms := make([]model.Alarm, 0, len(rows))
	for _, row := range rows {
		alarms = append(alarms, alarmFromRow(row))
	}
	return alarms
}

func alarmFromRow(row sqlapi.Row) model.Alarm {
	value, typ := rowconv.Value(row["AV"])
	if rt := rowconv.Int64(row["RT"]); rt != 0 || row["RT"] != nil {
		if typedValue, ok := rowconv.ValueForType(model.PointType(rt), row["AV"]); ok {
			value = typedValue
			typ = model.PointType(rt)
		}
	}
	status := model.DSFromInt16(rowconv.Int16(row["DS"]))
	configCode := model.AlarmCode(rowconv.Int64(row["LC"]))
	colors := alarmColorsFromRow(row)
	alarm := model.Alarm{
		ID:          model.PointID(rowconv.Int32(row["ID"])),
		GN:          model.GN(rowconv.String(row["GN"])),
		Type:        typ,
		Name:        rowconv.String(row["PN"]),
		Alias:       rowconv.String(row["AN"]),
		Description: rowconv.String(row["ED"]),
		Unit:        rowconv.String(row["EU"]),
		Level:       int8(rowconv.Int64(row["AL"])),
		Color:       rowconv.Int32(row["AC"]),
		Priority:    model.AlarmPriority(rowconv.Int64(row["AP"])),
		ConfigCode:  configCode,
		Colors:      colors,
		ActiveCode:  status.ActiveAlarm(typ, configCode),
		FirstTime:   rowconv.Time(row["TF"]),
		AlarmTime:   rowconv.Time(row["TA"]),
		UpdateTime:  rowconv.Time(row["TM"]),
		Status:      status,
		Value:       value,
	}
	if alarm.Color == 0 {
		if color, ok := alarm.DisplayColor(); ok {
			alarm.Color = color
		}
	}
	return alarm
}

func alarmColumns() []string {
	return []string{
		"ID", "GN", "PN", "AN", "ED", "EU",
		"TM", "TA", "TF", "AV", "DS", "RT",
		"AL", "AC", "AP", "LC",
		"C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8",
	}
}

func alarmColorsFromRow(row sqlapi.Row) model.AlarmColors {
	if !rowHasAny(row, "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8") {
		return model.AlarmColors{}
	}
	return model.AlarmColors{
		LL: rowconv.Int32(row["C1"]),
		ZL: rowconv.Int32(row["C2"]),
		L3: rowconv.Int32(row["C3"]),
		L4: rowconv.Int32(row["C4"]),
		HL: rowconv.Int32(row["C5"]),
		ZH: rowconv.Int32(row["C6"]),
		H3: rowconv.Int32(row["C7"]),
		H4: rowconv.Int32(row["C8"]),
	}
}

func rowHasAny(row sqlapi.Row, names ...string) bool {
	for _, name := range names {
		if _, ok := row[name]; ok {
			return true
		}
	}
	return false
}

func pointScopeSQL(ids []model.PointID, gns []model.GN) string {
	parts := make([]string, 0, 2)
	if len(ids) > 0 {
		items := make([]string, 0, len(ids))
		for _, id := range ids {
			items = append(items, fmt.Sprintf("%d", id))
		}
		parts = append(parts, fmt.Sprintf(`"ID" IN (%s)`, strings.Join(items, ",")))
	}
	if len(gns) > 0 {
		items := make([]string, 0, len(gns))
		for _, gn := range gns {
			items = append(items, sqlapi.LiteralString(string(gn)))
		}
		parts = append(parts, fmt.Sprintf(`"GN" IN (%s)`, strings.Join(items, ",")))
	}
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

func quoteColumns(columns []string) ([]string, error) {
	out := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted, err := sqlapi.QuoteIdentifier(column)
		if err != nil {
			return nil, err
		}
		out = append(out, quoted)
	}
	return out, nil
}

func timeLiteral(tm time.Time) string {
	tm = tm.Truncate(time.Millisecond)
	layout := "2006-01-02 15:04:05"
	if tm.Nanosecond() != 0 {
		layout = "2006-01-02 15:04:05.000"
	}
	return sqlapi.LiteralString(tm.Format(layout))
}
