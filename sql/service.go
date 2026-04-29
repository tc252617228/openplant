package sql

import (
	"context"
	"strings"

	"github.com/tc252617228/openplant/operror"
)

type Row map[string]any

type Result struct {
	Rows         []Row
	RowsAffected int64
}

type Executor interface {
	QuerySQL(ctx context.Context, query string) (Result, error)
	ExecSQL(ctx context.Context, query string) (Result, error)
}

type Options struct {
	ReadOnly       bool
	AllowUnsafeSQL bool
	Executor       Executor
}

type Service struct {
	closed         error
	readOnly       bool
	allowUnsafeSQL bool
	executor       Executor
}

func NewService(opts Options) *Service {
	return &Service{
		readOnly:       opts.ReadOnly,
		allowUnsafeSQL: opts.AllowUnsafeSQL,
		executor:       opts.Executor,
	}
}

func NewClosedService(err error) *Service {
	return &Service{closed: err}
}

func (s *Service) Query(ctx context.Context, query string) (Result, error) {
	var zero Result
	if s.closed != nil {
		return zero, s.closed
	}
	if err := ValidateReadOnly(query); err != nil {
		return zero, err
	}
	if s.executor == nil {
		return zero, operror.Unsupported("sql.Service.Query", "transport/protocol implementation is pending")
	}
	return s.executor.QuerySQL(ctx, query)
}

func (s *Service) ExecUnsafe(ctx context.Context, query string) (Result, error) {
	var zero Result
	if s.closed != nil {
		return zero, s.closed
	}
	if strings.TrimSpace(query) == "" {
		return zero, operror.UnsafeSQL("sql.Service.ExecUnsafe", "SQL is empty")
	}
	if s.readOnly {
		return zero, operror.ReadOnly("sql.Service.ExecUnsafe", "unsafe SQL blocked in readonly mode")
	}
	if !s.allowUnsafeSQL {
		return zero, operror.UnsafeSQL("sql.Service.ExecUnsafe", "unsafe SQL requires AllowUnsafeSQL")
	}
	if s.executor == nil {
		return zero, operror.Unsupported("sql.Service.ExecUnsafe", "transport/protocol implementation is pending")
	}
	return s.executor.ExecSQL(ctx, query)
}
