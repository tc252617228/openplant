package archive

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

type SnapshotQuery struct {
	DB       model.DatabaseName
	IDs      []model.PointID
	GNs      []model.GN
	Range    model.TimeRange
	Interval model.Interval
	Limit    int
}

type WriteRequest struct {
	DB        model.DatabaseName
	Samples   []model.Sample
	Cache     bool
	ChunkSize int
}

func (r WriteRequest) Validate() error {
	if err := r.DB.Validate(); err != nil {
		return err
	}
	if len(r.Samples) == 0 {
		return operror.Validation("archive.WriteRequest.Validate", "write requires at least one sample")
	}
	for _, sample := range r.Samples {
		if sample.ID <= 0 && sample.GN == "" {
			return operror.Validation("archive.WriteRequest.Validate", "sample requires ID or GN")
		}
		if sample.GN != "" {
			if err := sample.GN.Validate(); err != nil {
				return err
			}
		}
		if !sample.Type.Valid() {
			return operror.Validation("archive.WriteRequest.Validate", "sample requires explicit point type")
		}
		if sample.Value.Type() != sample.Type {
			return operror.Validation("archive.WriteRequest.Validate", "sample value type does not match explicit point type")
		}
		if sample.Time.IsZero() {
			return operror.Validation("archive.WriteRequest.Validate", "sample requires explicit timestamp")
		}
	}
	if r.ChunkSize < 0 {
		return operror.Validation("archive.WriteRequest.Validate", "chunk size cannot be negative")
	}
	return nil
}

func (r WriteRequest) ValidateNative() error {
	if err := r.Validate(); err != nil {
		return err
	}
	for _, sample := range r.Samples {
		if sample.GN != "" {
			return operror.Unsupported("archive.WriteRequest.ValidateNative", "native archive write requires point IDs; resolve GNs explicitly before calling native APIs")
		}
	}
	return nil
}

type DeleteRequest struct {
	DB        model.DatabaseName
	IDs       []model.PointID
	GNs       []model.GN
	Range     model.TimeRange
	ChunkSize int
}

func (r DeleteRequest) Validate() error {
	if err := r.DB.Validate(); err != nil {
		return err
	}
	if err := (model.PointSelector{IDs: r.IDs, GNs: r.GNs}).ValidateBounded(); err != nil {
		return err
	}
	if err := r.Range.Validate(); err != nil {
		return err
	}
	if r.ChunkSize < 0 {
		return operror.Validation("archive.DeleteRequest.Validate", "chunk size cannot be negative")
	}
	return nil
}

func (r DeleteRequest) ValidateNative() error {
	if err := r.Validate(); err != nil {
		return err
	}
	if len(r.GNs) > 0 {
		return operror.Unsupported("archive.DeleteRequest.ValidateNative", "native archive delete requires point IDs; resolve GNs explicitly before calling native APIs")
	}
	return nil
}

type Queryer interface {
	Query(ctx context.Context, query string) (sqlapi.Result, error)
}

type Requester interface {
	QueryArchiveByRequest(ctx context.Context, q Query) ([]model.Sample, error)
}

type NativeQuerier interface {
	QueryArchiveNative(ctx context.Context, q Query) ([]model.Sample, error)
}

type NativeStreamer interface {
	StreamArchiveNative(ctx context.Context, q Query, emit func(model.Sample) bool) error
}

type NativeWriter interface {
	WriteArchiveNative(ctx context.Context, req WriteRequest) error
	DeleteArchiveNative(ctx context.Context, req DeleteRequest) error
}

type Options struct {
	Queryer   Queryer
	Requester Requester
	Native    NativeQuerier
	Streamer  NativeStreamer
	Writer    NativeWriter
	ReadOnly  bool
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
		mode = model.ModeRaw
	}
	if err := mode.Validate(); err != nil {
		return err
	}
	if mode.RequiresInterval() {
		if err := q.Interval.ValidateRequired(); err != nil {
			return err
		}
	} else if err := q.Interval.ValidateOptional(); err != nil {
		return err
	}
	if err := q.Quality.Validate(); err != nil {
		return err
	}
	if q.Limit < 0 {
		return operror.Validation("archive.Query.Validate", "limit cannot be negative")
	}
	if q.ChunkSize < 0 {
		return operror.Validation("archive.Query.Validate", "chunk size cannot be negative")
	}
	return nil
}

func (q Query) ValidateNative() error {
	if err := q.Validate(); err != nil {
		return err
	}
	if len(q.GNs) > 0 {
		return operror.Unsupported("archive.Query.ValidateNative", "native archive query requires point IDs; resolve GNs explicitly before calling native APIs")
	}
	return nil
}

func (q SnapshotQuery) Validate() error {
	if err := q.DB.Validate(); err != nil {
		return err
	}
	if err := (model.PointSelector{IDs: q.IDs, GNs: q.GNs}).ValidateBounded(); err != nil {
		return err
	}
	if err := q.Range.Validate(); err != nil {
		return err
	}
	if err := q.Interval.ValidateRequired(); err != nil {
		return err
	}
	if q.Limit < 0 {
		return operror.Validation("archive.SnapshotQuery.Validate", "limit cannot be negative")
	}
	return nil
}

type Service struct {
	closed    error
	readOnly  bool
	queryer   Queryer
	requester Requester
	native    NativeQuerier
	streamer  NativeStreamer
	writer    NativeWriter
}

func NewService(opts ...Options) *Service {
	s := &Service{}
	if len(opts) > 0 {
		s.readOnly = opts[0].ReadOnly
		s.queryer = opts[0].Queryer
		s.requester = opts[0].Requester
		s.native = opts[0].Native
		s.streamer = opts[0].Streamer
		s.writer = opts[0].Writer
	}
	return s
}

func NewClosedService(err error) *Service {
	return &Service{closed: err}
}

func (s *Service) Query(ctx context.Context, q Query) ([]model.Sample, error) {
	return s.QuerySQL(ctx, q)
}

func (s *Service) QuerySQL(ctx context.Context, q Query) ([]model.Sample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	return s.querySQL(ctx, q)
}

func (s *Service) SnapshotSQL(ctx context.Context, q SnapshotQuery) ([]model.Sample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("archive.Service.SnapshotSQL", "SQL queryer is not configured")
	}
	query, err := buildSnapshotSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	samples := make([]model.Sample, 0, len(result.Rows))
	for _, row := range result.Rows {
		samples = append(samples, sampleFromRow(row))
	}
	return samples, nil
}

func (s *Service) QueryRequest(ctx context.Context, q Query) ([]model.Sample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.requester == nil {
		return nil, operror.Unsupported("archive.Service.QueryRequest", "request archive query is not configured")
	}
	return s.requester.QueryArchiveByRequest(ctx, q)
}

func (s *Service) QueryNative(ctx context.Context, q Query) ([]model.Sample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.ValidateNative(); err != nil {
		return nil, err
	}
	if s.native == nil {
		return nil, operror.Unsupported("archive.Service.QueryNative", "native archive query is not configured")
	}
	return s.native.QueryArchiveNative(ctx, q)
}

func (s *Service) StreamNative(ctx context.Context, q Query, emit func(model.Sample) bool) error {
	if s.closed != nil {
		return s.closed
	}
	if err := q.ValidateNative(); err != nil {
		return err
	}
	if emit == nil {
		return operror.Validation("archive.Service.StreamNative", "emit callback is required")
	}
	if s.streamer == nil {
		return operror.Unsupported("archive.Service.StreamNative", "native archive stream is not configured")
	}
	return s.streamer.StreamArchiveNative(ctx, q, emit)
}

func (s *Service) WriteNative(ctx context.Context, req WriteRequest) error {
	if s.closed != nil {
		return s.closed
	}
	if s.readOnly {
		return operror.ReadOnly("archive.Service.WriteNative", "archive write blocked in readonly mode")
	}
	if err := req.ValidateNative(); err != nil {
		return err
	}
	if s.writer == nil {
		return operror.Unsupported("archive.Service.WriteNative", "native archive writer is not configured")
	}
	return s.writer.WriteArchiveNative(ctx, req)
}

func (s *Service) DeleteNative(ctx context.Context, req DeleteRequest) error {
	if s.closed != nil {
		return s.closed
	}
	if s.readOnly {
		return operror.ReadOnly("archive.Service.DeleteNative", "archive delete blocked in readonly mode")
	}
	if err := req.ValidateNative(); err != nil {
		return err
	}
	if s.writer == nil {
		return operror.Unsupported("archive.Service.DeleteNative", "native archive writer is not configured")
	}
	return s.writer.DeleteArchiveNative(ctx, req)
}

func (s *Service) querySQL(ctx context.Context, q Query) ([]model.Sample, error) {
	if s.queryer == nil {
		return nil, operror.Unsupported("archive.Service.Query.sql", "SQL queryer is not configured")
	}
	query, err := buildSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	samples := make([]model.Sample, 0, len(result.Rows))
	for _, row := range result.Rows {
		samples = append(samples, sampleFromRow(row))
	}
	return samples, nil
}

func buildSQL(q Query) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "Archive")
	if err != nil {
		return "", err
	}
	columns, err := quoteColumns([]string{"ID", "GN", "TM", "DS", "AV"})
	if err != nil {
		return "", err
	}
	conditions := make([]string, 0, 6)
	pointScope := pointScopeSQL(q.IDs, q.GNs)
	if pointScope == "" {
		return "", operror.Validation("archive.buildSQL", "point scope is required")
	}
	conditions = append(conditions, pointScope)
	conditions = append(conditions, fmt.Sprintf(`"TM" BETWEEN %s AND %s`, timeLiteral(q.Range.Begin), timeLiteral(q.Range.End)))
	mode := q.Mode
	if mode == "" {
		mode = model.ModeRaw
	}
	conditions = append(conditions, fmt.Sprintf(`"MODE" = %s`, sqlapi.LiteralString(string(mode))))
	if q.Interval != "" {
		conditions = append(conditions, fmt.Sprintf(`"INTERVAL" = %s`, sqlapi.LiteralString(string(q.Interval))))
	}
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

func buildSnapshotSQL(q SnapshotQuery) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "Archive")
	if err != nil {
		return "", err
	}
	columns, err := quoteColumns([]string{"ID", "GN", "TM", "DS", "AV", "RT", "FM"})
	if err != nil {
		return "", err
	}
	pointScope := pointScopeSQL(q.IDs, q.GNs)
	if pointScope == "" {
		return "", operror.Validation("archive.buildSnapshotSQL", "point scope is required")
	}
	conditions := []string{
		pointScope,
		fmt.Sprintf(`"TM" BETWEEN %s AND %s`, timeLiteral(q.Range.Begin), timeLiteral(q.Range.End)),
		fmt.Sprintf(`"MODE" = %s`, sqlapi.LiteralString(string(model.ModeSpan))),
		fmt.Sprintf(`"INTERVAL" = %s`, sqlapi.LiteralString(string(q.Interval))),
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
		`"TM" ASC,"ID" ASC`,
		limit,
	), nil
}

func sampleFromRow(row sqlapi.Row) model.Sample {
	value, typ := rowconv.Value(row["AV"])
	if rt := rowconv.Int64(row["RT"]); rt != 0 || row["RT"] != nil {
		if typedValue, ok := rowconv.ValueForType(model.PointType(rt), row["AV"]); ok {
			value = typedValue
			typ = model.PointType(rt)
		}
	}
	return model.Sample{
		ID:     model.PointID(rowconv.Int32(row["ID"])),
		GN:     model.GN(rowconv.String(row["GN"])),
		Type:   typ,
		Format: rowconv.Int16(row["FM"]),
		Time:   rowconv.Time(row["TM"]),
		Status: model.DSFromInt16(rowconv.Int16(row["DS"])),
		Value:  value,
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
