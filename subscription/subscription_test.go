package subscription

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
)

type fakeSource struct {
	fn func(context.Context, Request, func(Event) bool) error
}

func (f fakeSource) Subscribe(ctx context.Context, req Request, emit func(Event) bool) error {
	return f.fn(ctx, req, emit)
}

type fakeTableSource struct {
	fn func(context.Context, TableRequest, func(TableEvent) bool) error
}

func (f fakeTableSource) Subscribe(ctx context.Context, req Request, emit func(Event) bool) error {
	return operror.Unsupported("fakeTableSource.Subscribe", "realtime source is not configured")
}

func (f fakeTableSource) SubscribeTable(ctx context.Context, req TableRequest, emit func(TableEvent) bool) error {
	return f.fn(ctx, req, emit)
}

func TestRequestRequiresBoundedPointSelector(t *testing.T) {
	err := Request{DB: "W3"}.Validate()
	if err == nil {
		t.Fatalf("expected missing point selector to be rejected")
	}
}

func TestTableRequestRequiresBoundedIndex(t *testing.T) {
	valid := TableRequest{DB: "W3", Table: "Point", Key: "ID", Int32: []int32{1001}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid table request rejected: %v", err)
	}
	withoutIndex := valid
	withoutIndex.Int32 = nil
	if err := withoutIndex.Validate(); err == nil {
		t.Fatalf("expected missing index values to be rejected")
	}
	ambiguous := valid
	ambiguous.Strings = []string{"W3.N.P1"}
	if err := ambiguous.Validate(); err == nil {
		t.Fatalf("expected ambiguous index values to be rejected")
	}
	qualified := valid
	qualified.Table = "W3.Point"
	if err := qualified.Validate(); err == nil {
		t.Fatalf("expected qualified table name to be rejected")
	}
}

func TestSubscribeRequiresConfiguredSource(t *testing.T) {
	svc := NewService(Options{})
	stream, err := svc.Subscribe(context.Background(), Request{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err == nil || !operror.IsKind(err, operror.KindUnsupported) {
		t.Fatalf("expected unsupported source error, got stream=%v err=%v", stream, err)
	}
}

func TestSubscribePublishesEventsAndCloses(t *testing.T) {
	svc := NewService(Options{
		EventBuffer: 1,
		Source: fakeSource{fn: func(ctx context.Context, req Request, emit func(Event) bool) error {
			if req.DB != "W3" {
				t.Fatalf("db=%s want W3", req.DB)
			}
			if ok := emit(Event{Kind: EventData, Sample: model.Sample{ID: 1001, GN: "W3.N.P1", Value: model.R8(12.5)}}); !ok {
				t.Fatalf("emit returned false")
			}
			return nil
		}},
	})
	stream, err := svc.Subscribe(context.Background(), Request{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	event, ok := <-stream.Events()
	if !ok {
		t.Fatalf("events closed before first event")
	}
	if event.Kind != EventData {
		t.Fatalf("event kind=%s want %s", event.Kind, EventData)
	}
	value, ok := event.Sample.Value.Float64()
	if !ok || event.Sample.ID != 1001 || event.Sample.GN != "W3.N.P1" || value != 12.5 {
		t.Fatalf("unexpected event: %#v", event)
	}
	<-stream.Done()
	if _, ok := <-stream.Events(); ok {
		t.Fatalf("events channel should be closed")
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream err=%v", err)
	}
}

func TestSubscribePublishesSourceError(t *testing.T) {
	sourceErr := errors.New("subscription source failed")
	svc := NewService(Options{
		EventBuffer: 1,
		Source: fakeSource{fn: func(ctx context.Context, req Request, emit func(Event) bool) error {
			return sourceErr
		}},
	})
	stream, err := svc.Subscribe(context.Background(), Request{
		DB:  "W3",
		IDs: []model.PointID{1001},
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	event, ok := <-stream.Events()
	if !ok || !errors.Is(event.Err, sourceErr) {
		t.Fatalf("expected source error event, got event=%#v ok=%v", event, ok)
	}
	if event.Kind != EventError {
		t.Fatalf("event kind=%s want %s", event.Kind, EventError)
	}
	<-stream.Done()
	if !errors.Is(stream.Err(), sourceErr) {
		t.Fatalf("stream err=%v want %v", stream.Err(), sourceErr)
	}
}

func TestSubscribeTablePublishesRowsAndCloses(t *testing.T) {
	svc := NewService(Options{
		EventBuffer: 1,
		Source: fakeTableSource{fn: func(ctx context.Context, req TableRequest, emit func(TableEvent) bool) error {
			if req.DB != "W3" || req.Table != "Point" || req.Key != "ID" {
				t.Fatalf("unexpected request: %#v", req)
			}
			if ok := emit(TableEvent{Kind: EventData, Row: map[string]any{"ID": int32(1001), "GN": "W3.N.P1"}}); !ok {
				t.Fatalf("emit returned false")
			}
			return nil
		}},
	})
	stream, err := svc.SubscribeTable(context.Background(), TableRequest{
		DB:    "W3",
		Table: "Point",
		Key:   "ID",
		Int32: []int32{1001},
	})
	if err != nil {
		t.Fatalf("SubscribeTable failed: %v", err)
	}
	event, ok := <-stream.Events()
	if !ok || event.Kind != EventData || event.Row["GN"] != "W3.N.P1" {
		t.Fatalf("unexpected table event=%#v ok=%v", event, ok)
	}
	<-stream.Done()
	if err := stream.Err(); err != nil {
		t.Fatalf("stream err=%v", err)
	}
}

func TestSubscribeTableRequiresConfiguredTableSource(t *testing.T) {
	svc := NewService(Options{Source: fakeSource{fn: func(context.Context, Request, func(Event) bool) error {
		return nil
	}}})
	stream, err := svc.SubscribeTable(context.Background(), TableRequest{
		DB:    "W3",
		Table: "Point",
		Key:   "ID",
		Int32: []int32{1001},
	})
	if err == nil || !operror.IsKind(err, operror.KindUnsupported) {
		t.Fatalf("expected unsupported table source error, got stream=%v err=%v", stream, err)
	}
}

func TestEventKindHelpersAcceptLegacyEvents(t *testing.T) {
	if !(Event{Sample: model.Sample{ID: 1001}}).IsData() {
		t.Fatalf("legacy sample event should be data")
	}
	if !(Event{Err: errors.New("legacy error")}).IsError() {
		t.Fatalf("legacy error event should be error")
	}
	if (Event{Kind: EventReconnected}).IsData() {
		t.Fatalf("status event should not be data")
	}
	if !(TableEvent{Row: map[string]any{"ID": int32(1001)}}).IsData() {
		t.Fatalf("legacy table row event should be data")
	}
	if !(TableEvent{Err: errors.New("legacy error")}).IsError() {
		t.Fatalf("legacy table error event should be error")
	}
}

func TestTerminalErrorDoesNotBlockWithoutReader(t *testing.T) {
	sourceErr := errors.New("subscription source failed")
	svc := NewService(Options{
		EventBuffer: 0,
		Source: fakeSource{fn: func(ctx context.Context, req Request, emit func(Event) bool) error {
			return sourceErr
		}},
	})
	stream, err := svc.Subscribe(context.Background(), Request{
		DB:  "W3",
		IDs: []model.PointID{1001},
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatalf("terminal error blocked stream shutdown")
	}
	if !errors.Is(stream.Err(), sourceErr) {
		t.Fatalf("stream err=%v want %v", stream.Err(), sourceErr)
	}
}

func TestCloseCancelsSourceAndClosesEvents(t *testing.T) {
	sourceDone := make(chan struct{})
	svc := NewService(Options{
		Source: fakeSource{fn: func(ctx context.Context, req Request, emit func(Event) bool) error {
			<-ctx.Done()
			close(sourceDone)
			return ctx.Err()
		}},
	})
	stream, err := svc.Subscribe(context.Background(), Request{
		DB:  "W3",
		IDs: []model.PointID{1001},
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	stream.Close()
	stream.Close()
	select {
	case <-sourceDone:
	case <-time.After(time.Second):
		t.Fatalf("source was not canceled")
	}
	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatalf("stream did not close")
	}
	if _, ok := <-stream.Events(); ok {
		t.Fatalf("events channel should be closed")
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("close should not publish cancellation error, got %v", err)
	}
}
