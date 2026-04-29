package main

import (
	"context"
	"testing"
	"time"

	openplant "github.com/tc252617228/openplant"
)

type fakeTypedSubscriber struct {
	req    openplant.TableSubscribeRequest
	events []openplant.TableSubscribeEvent
	err    error
}

func (f *fakeTypedSubscriber) subscribeTable(ctx context.Context, req openplant.TableSubscribeRequest) (tableStream, error) {
	f.req = req
	return newFakeTableStream(f.events, f.err), nil
}

type fakeTableStream struct {
	events chan openplant.TableSubscribeEvent
	done   chan struct{}
	err    error
}

func newFakeTableStream(events []openplant.TableSubscribeEvent, err error) *fakeTableStream {
	ch := make(chan openplant.TableSubscribeEvent, len(events))
	for _, event := range events {
		ch <- event
	}
	close(ch)
	done := make(chan struct{})
	close(done)
	return &fakeTableStream{events: ch, done: done, err: err}
}

func (s *fakeTableStream) Events() <-chan openplant.TableSubscribeEvent { return s.events }
func (s *fakeTableStream) Done() <-chan struct{}                        { return s.done }
func (s *fakeTableStream) Err() error                                   { return s.err }
func (s *fakeTableStream) Close()                                       {}

func TestSubscribePointRecordsMapsRows(t *testing.T) {
	sub := &fakeTypedSubscriber{events: []openplant.TableSubscribeEvent{{
		Kind: openplant.SubscribeEventData,
		Row: map[string]any{
			"ID": int32(1001),
			"GN": "W3.N.P1",
			"PN": "P1",
			"ED": "example point",
			"RT": int8(openplant.TypeR8),
			"PT": int8(openplant.SourceDAS),
			"AR": int32(1),
		},
	}}}
	stream, err := SubscribePointRecords(context.Background(), sub, PointTableSubscriptionRequest{
		DB:       "W3",
		IDs:      []openplant.PointID{1001},
		Snapshot: true,
	})
	if err != nil {
		t.Fatalf("SubscribePointRecords failed: %v", err)
	}
	if sub.req.Table != "Point" || sub.req.Key != "ID" || len(sub.req.Int32) != 1 || sub.req.Int32[0] != 1001 || !sub.req.Snapshot {
		t.Fatalf("unexpected raw request: %#v", sub.req)
	}
	event := receiveTypedEvent(t, stream.Events())
	if event.Err != nil {
		t.Fatalf("typed event err=%v", event.Err)
	}
	if event.Record.ID != 1001 || event.Record.GN != "W3.N.P1" || event.Record.Name != "P1" || event.Record.Type != openplant.TypeR8 || !event.Record.Archived {
		t.Fatalf("unexpected point record: %#v", event.Record)
	}
}

func TestSubscribeAlarmRecordsMapsRows(t *testing.T) {
	now := time.Unix(123456, 0)
	sub := &fakeTypedSubscriber{events: []openplant.TableSubscribeEvent{{
		Kind: openplant.SubscribeEventData,
		Row: map[string]any{
			"ID": int32(1001),
			"GN": "W3.N.P1",
			"PN": "P1",
			"AN": "Alias",
			"ED": "example alarm",
			"EU": "unit",
			"TM": now,
			"TA": now,
			"TF": now,
			"AV": float64(12.5),
			"DS": int16(openplant.DSInAlarm),
			"RT": int8(openplant.TypeR8),
			"AP": int8(openplant.AlarmPriorityRed),
			"LC": int16(openplant.AlarmHL),
		},
	}}}
	stream, err := SubscribeAlarmRecords(context.Background(), sub, AlarmTableSubscriptionRequest{
		DB:       "W3",
		GNs:      []openplant.GN{"W3.N.P1"},
		Snapshot: true,
	})
	if err != nil {
		t.Fatalf("SubscribeAlarmRecords failed: %v", err)
	}
	if sub.req.Table != "Alarm" || sub.req.Key != "GN" || len(sub.req.Strings) != 1 || sub.req.Strings[0] != "W3.N.P1" {
		t.Fatalf("unexpected raw request: %#v", sub.req)
	}
	event := receiveTypedEvent(t, stream.Events())
	if event.Err != nil {
		t.Fatalf("typed event err=%v", event.Err)
	}
	value, ok := event.Record.Value.Float64()
	if !ok || value != 12.5 {
		t.Fatalf("unexpected alarm value: %#v ok=%v", event.Record.Value.Interface(), ok)
	}
	if event.Record.ID != 1001 || event.Record.GN != "W3.N.P1" || event.Record.Priority != openplant.AlarmPriorityRed || event.Record.ConfigCode != openplant.AlarmHL {
		t.Fatalf("unexpected alarm record: %#v", event.Record)
	}
}

func receiveTypedEvent[T any](t testing.TB, ch <-chan TypedTableEvent[T]) TypedTableEvent[T] {
	t.Helper()
	return receiveTypedEventWithin(t, ch, time.Second)
}

func receiveTypedEventWithin[T any](t testing.TB, ch <-chan TypedTableEvent[T], timeout time.Duration) TypedTableEvent[T] {
	t.Helper()
	select {
	case event, ok := <-ch:
		if !ok {
			t.Fatalf("typed event channel closed")
		}
		return event
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for typed event")
		return TypedTableEvent[T]{}
	}
}
