package realtime

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

type ReadRequest struct {
	DB  model.DatabaseName
	IDs []model.PointID
	GNs []model.GN
}

func (r ReadRequest) Validate() error {
	if err := r.DB.Validate(); err != nil {
		return err
	}
	return (model.PointSelector{IDs: r.IDs, GNs: r.GNs}).ValidateBounded()
}

func (r ReadRequest) ValidateNative() error {
	if err := r.Validate(); err != nil {
		return err
	}
	if len(r.GNs) > 0 {
		return operror.Unsupported("realtime.ReadRequest.ValidateNative", "native realtime read requires point IDs; resolve GNs explicitly before calling native APIs")
	}
	return nil
}

type Write struct {
	ID     model.PointID
	GN     model.GN
	Type   model.PointType
	Time   time.Time
	Status model.DS
	Value  model.Value
}

func (w Write) Validate() error {
	if w.ID <= 0 && w.GN == "" {
		return operror.Validation("realtime.Write.Validate", "write requires ID or GN")
	}
	if w.GN != "" {
		if err := w.GN.Validate(); err != nil {
			return err
		}
	}
	if !w.Type.Valid() {
		return operror.Validation("realtime.Write.Validate", "write requires explicit point type")
	}
	if w.Time.IsZero() {
		return operror.Validation("realtime.Write.Validate", "write requires explicit timestamp")
	}
	if w.Value.Type() != w.Type {
		return operror.Validation("realtime.Write.Validate", "value type does not match explicit point type")
	}
	return nil
}

type WriteRequest struct {
	DB        model.DatabaseName
	Values    []Write
	ChunkSize int
}

func (r WriteRequest) Validate() error {
	if err := r.DB.Validate(); err != nil {
		return err
	}
	if len(r.Values) == 0 {
		return operror.Validation("realtime.WriteRequest.Validate", "write requires at least one value")
	}
	for _, value := range r.Values {
		if err := value.Validate(); err != nil {
			return err
		}
	}
	if r.ChunkSize < 0 {
		return operror.Validation("realtime.WriteRequest.Validate", "chunk size cannot be negative")
	}
	return nil
}

func (r WriteRequest) ValidateNative() error {
	if err := r.Validate(); err != nil {
		return err
	}
	for _, value := range r.Values {
		if value.GN != "" {
			return operror.Unsupported("realtime.WriteRequest.ValidateNative", "native realtime write requires point IDs; resolve GNs explicitly before calling native APIs")
		}
	}
	return nil
}

type Service struct {
	closed    error
	readOnly  bool
	queryer   Queryer
	requester Requester
	reader    Reader
	writer    Writer
}

type Queryer interface {
	Query(ctx context.Context, query string) (sqlapi.Result, error)
}

type Requester interface {
	QueryRealtimeByRequest(ctx context.Context, req ReadRequest) ([]model.Sample, error)
}

type Reader interface {
	ReadRealtime(ctx context.Context, req ReadRequest) ([]model.Sample, error)
}

type Writer interface {
	WriteRealtimeNative(ctx context.Context, req WriteRequest) error
}

type Options struct {
	ReadOnly  bool
	Queryer   Queryer
	Requester Requester
	Reader    Reader
	Writer    Writer
}

func NewService(opts ...Options) *Service {
	s := &Service{}
	if len(opts) > 0 {
		s.readOnly = opts[0].ReadOnly
		s.queryer = opts[0].Queryer
		s.requester = opts[0].Requester
		s.reader = opts[0].Reader
		s.writer = opts[0].Writer
	}
	return s
}

func NewClosedService(err error) *Service {
	return &Service{closed: err}
}

func (s *Service) Read(ctx context.Context, req ReadRequest) ([]model.Sample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := req.ValidateNative(); err != nil {
		return nil, err
	}
	if s.reader == nil {
		return nil, operror.Unsupported("realtime.Service.Read", "realtime reader is not configured")
	}
	return s.reader.ReadRealtime(ctx, req)
}

func (s *Service) QuerySQL(ctx context.Context, req ReadRequest) ([]model.Sample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("realtime.Service.QuerySQL", "SQL queryer is not configured")
	}
	query, err := buildSQL(req)
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

func (s *Service) QueryRequest(ctx context.Context, req ReadRequest) ([]model.Sample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.requester == nil {
		return nil, operror.Unsupported("realtime.Service.QueryRequest", "request realtime query is not configured")
	}
	return s.requester.QueryRealtimeByRequest(ctx, req)
}

func (s *Service) WriteNative(ctx context.Context, req WriteRequest) error {
	if s.closed != nil {
		return s.closed
	}
	if s.readOnly {
		return operror.ReadOnly("realtime.Service.WriteNative", "realtime write blocked in readonly mode")
	}
	if err := req.ValidateNative(); err != nil {
		return err
	}
	if s.writer == nil {
		return operror.Unsupported("realtime.Service.WriteNative", "realtime native writer is not configured")
	}
	return s.writer.WriteRealtimeNative(ctx, req)
}

func buildSQL(req ReadRequest) (string, error) {
	table, err := sqlapi.QualifiedTable(string(req.DB), "Realtime")
	if err != nil {
		return "", err
	}
	columns, err := quoteColumns([]string{"ID", "GN", "TM", "DS", "AV"})
	if err != nil {
		return "", err
	}
	pointScope := pointScopeSQL(req.IDs, req.GNs)
	if pointScope == "" {
		return "", operror.Validation("realtime.buildSQL", "point scope is required")
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s",
		strings.Join(columns, ","),
		table,
		pointScope,
		`"ID" ASC`,
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
