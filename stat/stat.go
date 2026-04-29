package stat

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

type Query struct {
	DB        model.DatabaseName
	IDs       []model.PointID
	GNs       []model.GN
	Range     model.TimeRange
	Mode      model.ArchiveMode
	Interval  model.Interval
	Quality   model.QualityFilter
	Limit     int
	ChunkSize int
}

type Queryer interface {
	Query(ctx context.Context, query string) (sqlapi.Result, error)
}

type Requester interface {
	QueryStatByRequest(ctx context.Context, q Query) ([]model.StatSample, error)
}

type NativeQuerier interface {
	QueryStatNative(ctx context.Context, q Query) ([]model.StatSample, error)
}

type NativeStreamer interface {
	StreamStatNative(ctx context.Context, q Query, emit func(model.StatSample) bool) error
}

type Options struct {
	Queryer   Queryer
	Requester Requester
	Native    NativeQuerier
	Streamer  NativeStreamer
}

func (q Query) Validate() error {
	if err := q.DB.Validate(); err != nil {
		return err
	}
	if err := (model.PointSelector{IDs: q.IDs, GNs: q.GNs}).ValidateBounded(); err != nil {
		return err
	}
	if err := q.Range.Validate(); err != nil {
		return err
	}
	mode := q.Mode
	if mode == "" {
		mode = model.ModeAvg
	}
	if err := mode.Validate(); err != nil {
		return err
	}
	if err := q.Interval.ValidateRequired(); err != nil {
		return err
	}
	if err := q.Quality.Validate(); err != nil {
		return err
	}
	if q.Limit < 0 {
		return operror.Validation("stat.Query.Validate", "limit cannot be negative")
	}
	if q.ChunkSize < 0 {
		return operror.Validation("stat.Query.Validate", "chunk size cannot be negative")
	}
	return nil
}

func (q Query) ValidateNative() error {
	if err := q.Validate(); err != nil {
		return err
	}
	if len(q.GNs) > 0 {
		return operror.Unsupported("stat.Query.ValidateNative", "native stat query requires point IDs; resolve GNs explicitly before calling native APIs")
	}
	return nil
}

type Service struct {
	closed    error
	queryer   Queryer
	requester Requester
	native    NativeQuerier
	streamer  NativeStreamer
}

func NewService(opts ...Options) *Service {
	s := &Service{}
	if len(opts) > 0 {
		s.queryer = opts[0].Queryer
		s.requester = opts[0].Requester
		s.native = opts[0].Native
		s.streamer = opts[0].Streamer
	}
	return s
}

func NewClosedService(err error) *Service {
	return &Service{closed: err}
}

func (s *Service) Query(ctx context.Context, q Query) ([]model.StatSample, error) {
	return s.QuerySQL(ctx, q)
}

func (s *Service) QuerySQL(ctx context.Context, q Query) ([]model.StatSample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	return s.querySQL(ctx, q)
}

func (s *Service) QueryRequest(ctx context.Context, q Query) ([]model.StatSample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.requester == nil {
		return nil, operror.Unsupported("stat.Service.QueryRequest", "request stat query is not configured")
	}
	return s.requester.QueryStatByRequest(ctx, q)
}

func (s *Service) QueryNative(ctx context.Context, q Query) ([]model.StatSample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.ValidateNative(); err != nil {
		return nil, err
	}
	if s.native == nil {
		return nil, operror.Unsupported("stat.Service.QueryNative", "native stat query is not configured")
	}
	return s.native.QueryStatNative(ctx, q)
}

func (s *Service) StreamNative(ctx context.Context, q Query, emit func(model.StatSample) bool) error {
	if s.closed != nil {
		return s.closed
	}
	if err := q.ValidateNative(); err != nil {
		return err
	}
	if emit == nil {
		return operror.Validation("stat.Service.StreamNative", "emit callback is required")
	}
	if s.streamer == nil {
		return operror.Unsupported("stat.Service.StreamNative", "native stat stream is not configured")
	}
	return s.streamer.StreamStatNative(ctx, q, emit)
}

func (s *Service) querySQL(ctx context.Context, q Query) ([]model.StatSample, error) {
	if s.queryer == nil {
		return nil, operror.Unsupported("stat.Service.Query.sql", "SQL queryer is not configured")
	}
	query, err := buildSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	samples := make([]model.StatSample, 0, len(result.Rows))
	for _, row := range result.Rows {
		samples = append(samples, sampleFromRow(row))
	}
	return samples, nil
}

func buildSQL(q Query) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "Stat")
	if err != nil {
		return "", err
	}
	columns, err := quoteColumns([]string{"ID", "GN", "TM", "DS", "FLOW", "AVGV", "MAXV", "MINV", "MAXTIME", "MINTIME"})
	if err != nil {
		return "", err
	}
	conditions := make([]string, 0, 5)
	pointScope := pointScopeSQL(q.IDs, q.GNs)
	if pointScope == "" {
		return "", operror.Validation("stat.buildSQL", "point scope is required")
	}
	conditions = append(conditions, pointScope)
	conditions = append(conditions, fmt.Sprintf(`"TM" BETWEEN %s AND %s`, timeLiteral(q.Range.Begin), timeLiteral(q.Range.End)))
	conditions = append(conditions, fmt.Sprintf(`"INTERVAL" = %s`, sqlapi.LiteralString(string(q.Interval))))
	conditions = append(conditions, fmt.Sprintf(`"QTYPE" = %d`, q.Quality))
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

func sampleFromRow(row sqlapi.Row) model.StatSample {
	return model.StatSample{
		ID:      model.PointID(rowconv.Int32(row["ID"])),
		GN:      model.GN(rowconv.String(row["GN"])),
		Time:    rowconv.Time(row["TM"]),
		Status:  model.DSFromInt16(rowconv.Int16(row["DS"])),
		Flow:    rowconv.Float64(row["FLOW"]),
		Avg:     rowconv.Float64(row["AVGV"]),
		Max:     rowconv.Float64(row["MAXV"]),
		Min:     rowconv.Float64(row["MINV"]),
		MaxTime: rowconv.Time(row["MAXTIME"]),
		MinTime: rowconv.Time(row["MINTIME"]),
		Mean:    rowconv.Float64(row["MEAN"]),
		Sum:     rowconv.Float64(row["SUM"]),
	}
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
