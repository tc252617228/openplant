package admin

import (
	"context"
	"fmt"
	"strings"

	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	sqlapi "github.com/tc252617228/openplant/sql"
)

type Options struct {
	ReadOnly bool
	Mutator  Mutator
}

type Mutator interface {
	MutateTable(ctx context.Context, req TableMutation) error
}

type Service struct {
	closed   error
	readOnly bool
	mutator  Mutator
}

func NewService(opts Options) *Service {
	return &Service{readOnly: opts.ReadOnly, mutator: opts.Mutator}
}

func NewClosedService(err error) *Service {
	return &Service{closed: err}
}

func (s *Service) EnsureWritable(op string) error {
	if s.closed != nil {
		return s.closed
	}
	if s.readOnly {
		return operror.ReadOnly(op, "admin operation blocked in readonly mode")
	}
	return nil
}

func (s *Service) PingWritable(ctx context.Context) error {
	if err := s.EnsureWritable("admin.Service.PingWritable"); err != nil {
		return err
	}
	return operror.Unsupported("admin.Service.PingWritable", "transport/protocol implementation is pending")
}

type MutationAction string

const (
	MutationInsert  MutationAction = "insert"
	MutationUpdate  MutationAction = "update"
	MutationReplace MutationAction = "replace"
	MutationDelete  MutationAction = "delete"
)

type ColumnType uint8

const (
	ColumnNull      ColumnType = 0
	ColumnBool      ColumnType = 1
	ColumnInt8      ColumnType = 2
	ColumnInt16     ColumnType = 3
	ColumnInt32     ColumnType = 4
	ColumnInt64     ColumnType = 5
	ColumnFloat32   ColumnType = 6
	ColumnFloat64   ColumnType = 7
	ColumnDateTime  ColumnType = 8
	ColumnString    ColumnType = 9
	ColumnBinary    ColumnType = 10
	ColumnObject    ColumnType = 11
	ColumnMap       ColumnType = 12
	ColumnStructure ColumnType = 13
	ColumnSlice     ColumnType = 14
)

type FilterOperator uint8

const (
	FilterEQ FilterOperator = 0
	FilterGE FilterOperator = 4
	FilterLE FilterOperator = 5
	FilterIn FilterOperator = 6
)

type FilterRelation uint8

const (
	FilterAnd FilterRelation = 0
	FilterOr  FilterRelation = 1
)

type Column struct {
	Name   string
	Type   ColumnType
	Length uint8
}

type Row map[string]any

type Indexes struct {
	Key     string
	Int32   []int32
	Int64   []int64
	Strings []string
}

type Filter struct {
	Left     string
	Operator FilterOperator
	Right    string
	Relation FilterRelation
}

type TableMutation struct {
	DB      model.DatabaseName
	Table   string
	Action  MutationAction
	Key     string
	Indexes *Indexes
	Filters []Filter
	Columns []Column
	Rows    []Row
}

func (s *Service) MutateTable(ctx context.Context, req TableMutation) error {
	if err := s.EnsureWritable("admin.Service.MutateTable"); err != nil {
		return err
	}
	if err := req.Validate(); err != nil {
		return err
	}
	if s.mutator == nil {
		return operror.Unsupported("admin.Service.MutateTable", "table mutator is not configured")
	}
	return s.mutator.MutateTable(ctx, req)
}

func (r TableMutation) Validate() error {
	if r.DB != "" {
		if err := r.DB.Validate(); err != nil {
			return err
		}
	}
	if _, err := sqlapi.QuoteIdentifier(r.Table); err != nil {
		return operror.Validation("admin.TableMutation.Validate", "table name is invalid")
	}
	if isTimeSeriesTable(r.Table) {
		return operror.Validation("admin.TableMutation.Validate", "time-series tables must use their dedicated mutation APIs")
	}
	switch r.Action {
	case MutationInsert, MutationReplace:
		if len(r.Rows) == 0 {
			return operror.Validation("admin.TableMutation.Validate", "insert/replace requires rows")
		}
		if len(r.Columns) == 0 {
			return operror.Validation("admin.TableMutation.Validate", "insert/replace requires explicit columns")
		}
	case MutationUpdate:
		if len(r.Rows) != 1 {
			return operror.Validation("admin.TableMutation.Validate", "update requires exactly one row of field values")
		}
		if len(r.Columns) == 0 {
			return operror.Validation("admin.TableMutation.Validate", "update requires explicit columns")
		}
		if !r.bounded() {
			return operror.Validation("admin.TableMutation.Validate", "update requires indexes or filters")
		}
	case MutationDelete:
		if len(r.Rows) > 0 || len(r.Columns) > 0 {
			return operror.Validation("admin.TableMutation.Validate", "delete must not include rows or columns")
		}
		if !r.bounded() {
			return operror.Validation("admin.TableMutation.Validate", "delete requires indexes or filters")
		}
	default:
		return operror.Validation("admin.TableMutation.Validate", fmt.Sprintf("unsupported mutation action %q", r.Action))
	}
	if err := validateIndexes(r.Key, r.Indexes); err != nil {
		return err
	}
	if r.Key != "" {
		if err := validateColumnName(r.Key); err != nil {
			return err
		}
	}
	if err := validateColumns(r.Columns); err != nil {
		return err
	}
	if err := validateRows(r.Columns, r.Rows); err != nil {
		return err
	}
	for _, filter := range r.Filters {
		if err := filter.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func validateIndexes(defaultKey string, indexes *Indexes) error {
	if indexes == nil {
		return nil
	}
	key := indexes.Key
	if key == "" {
		key = defaultKey
	}
	if key == "" {
		return operror.Validation("admin.TableMutation.Validate", "indexes require an explicit key column")
	}
	if err := validateColumnName(key); err != nil {
		return err
	}
	kinds := 0
	if len(indexes.Int32) > 0 {
		kinds++
	}
	if len(indexes.Int64) > 0 {
		kinds++
	}
	if len(indexes.Strings) > 0 {
		kinds++
	}
	if kinds != 1 {
		return operror.Validation("admin.TableMutation.Validate", "indexes require exactly one value type")
	}
	return nil
}

func (f Filter) Validate() error {
	if err := validateColumnName(f.Left); err != nil {
		return err
	}
	if strings.TrimSpace(f.Right) == "" {
		return operror.Validation("admin.Filter.Validate", "filter right value cannot be empty")
	}
	switch f.Operator {
	case FilterEQ, FilterGE, FilterLE, FilterIn:
	default:
		return operror.Validation("admin.Filter.Validate", fmt.Sprintf("unsupported filter operator %d", f.Operator))
	}
	switch f.Relation {
	case FilterAnd, FilterOr:
	default:
		return operror.Validation("admin.Filter.Validate", fmt.Sprintf("unsupported filter relation %d", f.Relation))
	}
	return nil
}

func (r TableMutation) bounded() bool {
	if r.Indexes != nil && (len(r.Indexes.Int32) > 0 || len(r.Indexes.Int64) > 0 || len(r.Indexes.Strings) > 0) {
		return true
	}
	return len(r.Filters) > 0
}

func validateColumns(columns []Column) error {
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if err := validateColumnName(column.Name); err != nil {
			return err
		}
		if _, ok := seen[column.Name]; ok {
			return operror.Validation("admin.TableMutation.Validate", "duplicate column: "+column.Name)
		}
		seen[column.Name] = struct{}{}
		if column.Type > ColumnSlice {
			return operror.Validation("admin.TableMutation.Validate", fmt.Sprintf("unsupported column type %d", column.Type))
		}
	}
	return nil
}

func validateRows(columns []Column, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		allowed[column.Name] = struct{}{}
	}
	for _, row := range rows {
		for key := range row {
			if _, ok := allowed[key]; !ok {
				return operror.Validation("admin.TableMutation.Validate", "row contains column not declared in Columns: "+key)
			}
		}
	}
	return nil
}

func validateColumnName(name string) error {
	if _, err := sqlapi.QuoteIdentifier(name); err != nil {
		return operror.Validation("admin.TableMutation.Validate", "column name is invalid: "+name)
	}
	return nil
}

func isTimeSeriesTable(table string) bool {
	parts := strings.Split(table, ".")
	name := strings.ToLower(parts[len(parts)-1])
	switch name {
	case "realtime", "dynamic", "archive", "stat", "alarm", "aalarm":
		return true
	default:
		return false
	}
}
