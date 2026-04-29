package subscription

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	sqlapi "github.com/tc252617228/openplant/sql"
)

type Options struct {
	RefreshInterval time.Duration
	BackoffMin      time.Duration
	BackoffMax      time.Duration
	EventBuffer     int
	Source          Source
}

type Request struct {
	DB  model.DatabaseName
	GNs []model.GN
	IDs []model.PointID
}

func (r Request) Validate() error {
	if err := r.DB.Validate(); err != nil {
		return err
	}
	return (model.PointSelector{IDs: r.IDs, GNs: r.GNs}).ValidateBounded()
}

type TableRequest struct {
	DB       model.DatabaseName
	Table    string
	Columns  []string
	Key      string
	Int32    []int32
	Int64    []int64
	Strings  []string
	Snapshot bool
}

func (r TableRequest) Validate() error {
	if err := r.DB.Validate(); err != nil {
		return err
	}
	if _, err := sqlapi.QuoteIdentifier(r.Table); err != nil {
		return operror.Validation("subscription.TableRequest.Validate", "table name is invalid")
	}
	if r.Table == "*" || strings.Contains(r.Table, ".") {
		return operror.Validation("subscription.TableRequest.Validate", "table name is invalid")
	}
	if _, err := sqlapi.QuoteIdentifier(r.Key); err != nil {
		return operror.Validation("subscription.TableRequest.Validate", "key column is invalid")
	}
	if r.Key == "*" || strings.Contains(r.Key, ".") {
		return operror.Validation("subscription.TableRequest.Validate", "key column is invalid")
	}
	for _, column := range r.Columns {
		if column == "*" {
			continue
		}
		if _, err := sqlapi.QuoteIdentifier(column); err != nil {
			return operror.Validation("subscription.TableRequest.Validate", "column name is invalid: "+column)
		}
		if strings.Contains(column, ".") {
			return operror.Validation("subscription.TableRequest.Validate", "column name is invalid: "+column)
		}
	}
	kinds := 0
	if len(r.Int32) > 0 {
		kinds++
	}
	if len(r.Int64) > 0 {
		kinds++
	}
	if len(r.Strings) > 0 {
		kinds++
	}
	if kinds != 1 {
		return operror.Validation("subscription.TableRequest.Validate", fmt.Sprintf("table subscription requires exactly one index value type, got %d", kinds))
	}
	return nil
}

type EventKind string

const (
	EventData         EventKind = "data"
	EventError        EventKind = "error"
	EventReconnecting EventKind = "reconnecting"
	EventReconnected  EventKind = "reconnected"
)

type Event struct {
	Kind   EventKind
	Sample model.Sample
	Err    error
}

type TableEvent struct {
	Kind EventKind
	Row  map[string]any
	Err  error
}

func (e Event) IsData() bool {
	return e.Kind == EventData || (e.Kind == "" && e.Err == nil)
}

func (e Event) IsError() bool {
	return e.Kind == EventError || (e.Kind == "" && e.Err != nil)
}

func (e TableEvent) IsData() bool {
	return e.Kind == EventData || (e.Kind == "" && e.Err == nil)
}

func (e TableEvent) IsError() bool {
	return e.Kind == EventError || (e.Kind == "" && e.Err != nil)
}

type Source interface {
	Subscribe(ctx context.Context, req Request, emit func(Event) bool) error
}

type TableSource interface {
	SubscribeTable(ctx context.Context, req TableRequest, emit func(TableEvent) bool) error
}

type RequestValidator interface {
	ValidateRequest(req Request) error
}

type Stream struct {
	events <-chan Event
	done   <-chan struct{}
	cancel context.CancelFunc
	once   sync.Once
	mu     sync.RWMutex
	err    error
}

type TableStream struct {
	events <-chan TableEvent
	done   <-chan struct{}
	cancel context.CancelFunc
	once   sync.Once
	mu     sync.RWMutex
	err    error
}

func (s *Stream) Events() <-chan Event {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *Stream) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *Stream) Err() error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *Stream) Close() {
	if s != nil && s.cancel != nil {
		s.once.Do(s.cancel)
	}
}

func (s *TableStream) Events() <-chan TableEvent {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *TableStream) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *TableStream) Err() error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *TableStream) Close() {
	if s != nil && s.cancel != nil {
		s.once.Do(s.cancel)
	}
}

type Service struct {
	closed error
	opts   Options
	source Source
}

func NewService(opts Options) *Service {
	if opts.RefreshInterval == 0 {
		opts.RefreshInterval = time.Minute
	}
	if opts.BackoffMin == 0 {
		opts.BackoffMin = 200 * time.Millisecond
	}
	if opts.BackoffMax == 0 {
		opts.BackoffMax = 30 * time.Second
	}
	if opts.EventBuffer < 0 {
		opts.EventBuffer = 0
	}
	return &Service{opts: opts, source: opts.Source}
}

func NewClosedService(err error) *Service {
	return &Service{closed: err}
}

func (s *Service) Subscribe(ctx context.Context, req Request) (*Stream, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.source == nil {
		return nil, operror.Unsupported("subscription.Service.Subscribe", "subscription source is not configured")
	}
	if validator, ok := s.source.(RequestValidator); ok {
		if err := validator.ValidateRequest(req); err != nil {
			return nil, err
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	events := make(chan Event, s.opts.EventBuffer)
	done := make(chan struct{})
	stream := &Stream{
		events: events,
		done:   done,
		cancel: cancel,
	}
	go func() {
		defer cancel()
		defer close(done)
		defer close(events)
		err := s.source.Subscribe(runCtx, req, func(event Event) bool {
			select {
			case events <- event:
				return true
			case <-runCtx.Done():
				return false
			}
		})
		if shouldPublishErr(runCtx, err) {
			stream.setErr(err)
			select {
			case events <- Event{Kind: EventError, Err: err}:
			case <-runCtx.Done():
			default:
			}
		}
	}()
	return stream, nil
}

func (s *Service) SubscribeTable(ctx context.Context, req TableRequest) (*TableStream, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	tableSource, ok := s.source.(TableSource)
	if !ok || tableSource == nil {
		return nil, operror.Unsupported("subscription.Service.SubscribeTable", "table subscription source is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	events := make(chan TableEvent, s.opts.EventBuffer)
	done := make(chan struct{})
	stream := &TableStream{
		events: events,
		done:   done,
		cancel: cancel,
	}
	go func() {
		defer cancel()
		defer close(done)
		defer close(events)
		err := tableSource.SubscribeTable(runCtx, req, func(event TableEvent) bool {
			select {
			case events <- event:
				return true
			case <-runCtx.Done():
				return false
			}
		})
		if shouldPublishErr(runCtx, err) {
			stream.setErr(err)
			select {
			case events <- TableEvent{Kind: EventError, Err: err}:
			case <-runCtx.Done():
			default:
			}
		}
	}()
	return stream, nil
}

func (s *Stream) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *TableStream) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func shouldPublishErr(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) && ctx.Err() == context.Canceled {
		return false
	}
	return true
}
