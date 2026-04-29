package openplant

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/internal/transport"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/subscription"
)

func TestSubscriptionByIDUsesRealtimeSubscribeProtocol(t *testing.T) {
	source := newPipeSubscriptionIDSource(t, func(t *testing.T, conn net.Conn) {
		serveSubscriptionInitialEvent(t, conn)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := source.SubscribeIDs(ctx, "W3", []model.PointID{1001}, func(event subscription.Event) bool {
		return true
	})
	if err != nil {
		t.Fatalf("SubscribeIDs failed: %v", err)
	}
	defer stream.Close()
}

func TestClientSubscriptionReceivesRealtimeEvents(t *testing.T) {
	client := newPipeSubscriptionClient(t, func(t *testing.T, conn net.Conn) {
		serveSubscriptionInitialEvent(t, conn)
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := client.Subscription().Subscribe(ctx, subscription.Request{
		DB:  "W3",
		IDs: []model.PointID{1001},
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer stream.Close()
	event := receiveSubscriptionEvent(t, stream.Events())
	if event.Err != nil {
		t.Fatalf("event err=%v", event.Err)
	}
	if event.Kind != subscription.EventData {
		t.Fatalf("event kind=%s want %s", event.Kind, subscription.EventData)
	}
	value, ok := event.Sample.Value.Float64()
	if !ok || value != 12.5 {
		t.Fatalf("value=%#v ok=%v", event.Sample.Value.Interface(), ok)
	}
	if event.Sample.ID != 1001 || event.Sample.GN != "W3.N.P1" || event.Sample.Status != 0 {
		t.Fatalf("sample=%#v", event.Sample)
	}
}

func TestClientSubscribeTableReceivesRawRows(t *testing.T) {
	client := newPipeTableSubscriptionClient(t, func(t *testing.T, conn net.Conn) {
		serveTableSubscriptionSnapshot(t, conn)
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := client.Subscription().SubscribeTable(ctx, subscription.TableRequest{
		DB:       "W3",
		Table:    "Point",
		Columns:  []string{"ID", "GN", "PN", "RT"},
		Key:      "ID",
		Int32:    []int32{1001},
		Snapshot: true,
	})
	if err != nil {
		t.Fatalf("SubscribeTable failed: %v", err)
	}
	defer stream.Close()
	event := receiveTableSubscriptionEvent(t, stream.Events())
	if event.Err != nil {
		t.Fatalf("event err=%v", event.Err)
	}
	if event.Kind != subscription.EventData {
		t.Fatalf("event kind=%s want %s", event.Kind, subscription.EventData)
	}
	if event.Row["GN"] != "W3.N.P1" || event.Row["PN"] != "P1" || int64ValueForTest(event.Row["ID"]) != 1001 {
		t.Fatalf("unexpected row: %#v", event.Row)
	}
}

func TestClientSubscriptionRejectsGNWithoutHiddenMetadataSQL(t *testing.T) {
	source := &subscriptionIDSource{}
	_, err := subscription.NewService(subscription.Options{Source: source}).Subscribe(context.Background(), subscription.Request{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err == nil || !operror.IsKind(err, operror.KindUnsupported) {
		t.Fatalf("expected unsupported GN subscription, got %v", err)
	}
}

func TestNewClientDefaultSubscriptionRejectsGNWithoutHiddenMetadataSQL(t *testing.T) {
	client, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer client.Close()

	stream, err := client.Subscription().Subscribe(context.Background(), subscription.Request{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err == nil || !operror.IsKind(err, operror.KindUnsupported) {
		t.Fatalf("expected unsupported GN subscription, got stream=%v err=%v", stream, err)
	}
}

func TestClientExposesExplicitRealtimeSubscriptionIDSource(t *testing.T) {
	client, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer client.Close()

	source := client.RealtimeSubscriptionIDSource()
	if source == nil {
		t.Fatalf("RealtimeSubscriptionIDSource returned nil")
	}
	_, err = source.SubscribeIDs(context.Background(), "W3", nil, func(event subscription.Event) bool {
		return true
	})
	if err == nil || !operror.IsKind(err, operror.KindValidation) {
		t.Fatalf("expected validation error for empty ID source request, got %v", err)
	}
}

func TestClosedClientRealtimeSubscriptionIDSourceReturnsClosed(t *testing.T) {
	client, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	source := client.RealtimeSubscriptionIDSource()
	_, err = source.SubscribeIDs(context.Background(), "W3", []model.PointID{1001}, func(event subscription.Event) bool {
		return true
	})
	if !errors.Is(err, operror.ErrClosed) {
		t.Fatalf("expected closed error, got %v", err)
	}
}

func TestSubscriptionIDStreamWritesAddAndRemoveControls(t *testing.T) {
	added := make(chan struct{})
	removed := make(chan struct{})
	source := newPipeSubscriptionIDSource(t, func(t *testing.T, conn net.Conn) {
		serveSubscriptionControls(t, conn, added, removed)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := source.SubscribeIDs(ctx, "W3", []model.PointID{1001}, func(event subscription.Event) bool {
		return true
	})
	if err != nil {
		t.Fatalf("SubscribeIDs failed: %v", err)
	}
	defer stream.Close()
	if err := stream.AddIDs(ctx, []model.PointID{2002}); err != nil {
		t.Fatalf("AddIDs failed: %v", err)
	}
	if err := stream.RemoveIDs(ctx, []model.PointID{1001}); err != nil {
		t.Fatalf("RemoveIDs failed: %v", err)
	}
	waitClosed(t, added, "add request")
	waitClosed(t, removed, "remove request")
}

func TestSubscriptionIDStreamReconnectsAndResubscribes(t *testing.T) {
	resubscribed := make(chan struct{})
	var dials atomic.Int32
	source := newPipeSubscriptionIDSourceByDial(t, &dials, func(t *testing.T, conn net.Conn, dial int32) {
		switch dial {
		case 1:
			serveSubscriptionCloseAfterInitial(t, conn, []model.PointID{1001})
		case 2:
			serveSubscriptionResubscribeAndEvent(t, conn, resubscribed, []model.PointID{1001}, 1001, "W3.N.P1", 18.5)
		default:
			serveSubscriptionIdle(t, conn)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events := make(chan subscription.Event, 4)
	stream, err := source.SubscribeIDs(ctx, "W3", []model.PointID{1001}, func(event subscription.Event) bool {
		events <- event
		return true
	})
	if err != nil {
		t.Fatalf("SubscribeIDs failed: %v", err)
	}
	defer stream.Close()
	waitClosed(t, resubscribed, "resubscribe request")
	reconnecting := receiveSubscriptionEvent(t, events)
	if reconnecting.Kind != subscription.EventReconnecting || reconnecting.Err == nil {
		t.Fatalf("reconnecting event=%#v", reconnecting)
	}
	reconnected := receiveSubscriptionEvent(t, events)
	if reconnected.Kind != subscription.EventReconnected || reconnected.Err != nil {
		t.Fatalf("reconnected event=%#v", reconnected)
	}
	event := receiveDataSubscriptionEvent(t, events)
	value, ok := event.Sample.Value.Float64()
	if !ok || event.Sample.ID != 1001 || event.Sample.GN != "W3.N.P1" || value != 18.5 {
		t.Fatalf("event=%#v valueOk=%v", event, ok)
	}
}

func TestSubscriptionIDStreamReconnectsWithUpdatedIDs(t *testing.T) {
	added := make(chan struct{})
	resubscribed := make(chan struct{})
	var dials atomic.Int32
	source := newPipeSubscriptionIDSourceByDial(t, &dials, func(t *testing.T, conn net.Conn, dial int32) {
		switch dial {
		case 1:
			serveSubscriptionCloseAfterAdd(t, conn, added)
		case 2:
			serveSubscriptionResubscribeOnly(t, conn, resubscribed, []model.PointID{1001, 2002})
		default:
			serveSubscriptionIdle(t, conn)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := source.SubscribeIDs(ctx, "W3", []model.PointID{1001}, func(event subscription.Event) bool {
		return true
	})
	if err != nil {
		t.Fatalf("SubscribeIDs failed: %v", err)
	}
	defer stream.Close()
	if err := stream.AddIDs(ctx, []model.PointID{2002}); err != nil {
		t.Fatalf("AddIDs failed: %v", err)
	}
	waitClosed(t, added, "add request")
	waitClosed(t, resubscribed, "updated resubscribe request")
}

func TestSubscriptionBackoffJitterBounds(t *testing.T) {
	base := 100 * time.Millisecond
	if got := jitterSubscriptionBackoff(base, 0); got != 80*time.Millisecond {
		t.Fatalf("low jitter=%s", got)
	}
	if got := jitterSubscriptionBackoff(base, 0.5); got != base {
		t.Fatalf("middle jitter=%s", got)
	}
	if got := jitterSubscriptionBackoff(base, 1); got != 120*time.Millisecond {
		t.Fatalf("high jitter=%s", got)
	}
}

func newPipeSubscriptionClient(t *testing.T, serve func(*testing.T, net.Conn)) *Client {
	t.Helper()
	cfg := DefaultOptions()
	cfg.RequestTimeout = time.Second
	c := &Client{options: cfg}
	source := newPipeSubscriptionIDSource(t, serve)
	c.subscription = subscription.NewService(subscription.Options{
		Source: &subscription.GNDriftSource{Source: source, Resolver: c},
	})
	return c
}

func newPipeTableSubscriptionClient(t *testing.T, serve func(*testing.T, net.Conn)) *Client {
	t.Helper()
	cfg := DefaultOptions()
	cfg.RequestTimeout = time.Second
	c := &Client{options: cfg}
	source := newPipeSubscriptionIDSource(t, serve)
	c.subscription = subscription.NewService(subscription.Options{
		Source: source,
	})
	return c
}

func newPipeSubscriptionIDSource(t *testing.T, serve func(*testing.T, net.Conn)) *subscriptionIDSource {
	t.Helper()
	var dials atomic.Int32
	return newPipeSubscriptionIDSourceByDial(t, &dials, func(t *testing.T, conn net.Conn, dial int32) {
		serve(t, conn)
	})
}

func newPipeSubscriptionIDSourceByDial(t *testing.T, dials *atomic.Int32, serve func(*testing.T, net.Conn, int32)) *subscriptionIDSource {
	t.Helper()
	return &subscriptionIDSource{cfg: transport.Config{
		Host:           "pipe",
		Port:           1,
		User:           "test-user",
		Password:       "test-secret",
		DialTimeout:    time.Second,
		RequestTimeout: time.Second,
		PoolSize:       1,
		MaxIdle:        1,
		IdleTimeout:    time.Minute,
		MaxLifetime:    time.Minute,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			client, server := net.Pipe()
			dial := dials.Add(1)
			go serve(t, server, dial)
			return client, nil
		},
	}, backoffMin: time.Millisecond, backoffMax: time.Millisecond}
}

func serveSubscriptionInitialEvent(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)

	props := readSubscriptionRequest(t, reader)
	assertSubscriptionRequest(t, props, -1, []model.PointID{1001})
	writeSubscriptionEvent(t, writer, model.PointID(1001), "W3.N.P1", 12.5)
	_, _ = reader.ReadMessage()
}

func serveTableSubscriptionSnapshot(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)

	props := readSubscriptionRequest(t, reader)
	assertTableSubscriptionRequest(t, props, "W3.Point", "ID", protocol.IndexInt32Array, true)
	writeTableSubscriptionEvent(t, writer, []codec.Column{
		{Name: "ID", Type: codec.VtInt32},
		{Name: "GN", Type: codec.VtString},
		{Name: "PN", Type: codec.VtString},
		{Name: "RT", Type: codec.VtInt8},
	}, []map[string]any{{
		"ID": int32(1001),
		"GN": "W3.N.P1",
		"PN": "P1",
		"RT": int8(model.TypeR8),
	}})
	_, _ = reader.ReadMessage()
}

func serveSubscriptionControls(t *testing.T, conn net.Conn, added, removed chan<- struct{}) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)

	assertSubscriptionRequest(t, readSubscriptionRequest(t, reader), -1, []model.PointID{1001})
	assertSubscriptionRequest(t, readSubscriptionRequest(t, reader), 1, []model.PointID{2002})
	close(added)
	assertSubscriptionRequest(t, readSubscriptionRequest(t, reader), 0, []model.PointID{1001})
	close(removed)
	_, _ = reader.ReadMessage()
}

func serveSubscriptionCloseAfterInitial(t *testing.T, conn net.Conn, ids []model.PointID) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)
	assertSubscriptionRequest(t, readSubscriptionRequest(t, reader), -1, ids)
}

func serveSubscriptionCloseAfterAdd(t *testing.T, conn net.Conn, added chan<- struct{}) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)
	assertSubscriptionRequest(t, readSubscriptionRequest(t, reader), -1, []model.PointID{1001})
	assertSubscriptionRequest(t, readSubscriptionRequest(t, reader), 1, []model.PointID{2002})
	close(added)
}

func serveSubscriptionResubscribeAndEvent(t *testing.T, conn net.Conn, resubscribed chan<- struct{}, ids []model.PointID, id model.PointID, gn string, value float64) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)
	assertSubscriptionRequest(t, readSubscriptionRequest(t, reader), -1, ids)
	close(resubscribed)
	writeSubscriptionEvent(t, writer, id, gn, value)
	_, _ = reader.ReadMessage()
}

func serveSubscriptionResubscribeOnly(t *testing.T, conn net.Conn, resubscribed chan<- struct{}, ids []model.PointID) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)
	assertSubscriptionRequest(t, readSubscriptionRequest(t, reader), -1, ids)
	close(resubscribed)
	_, _ = reader.ReadMessage()
}

func serveSubscriptionIdle(t *testing.T, conn net.Conn) {
	defer conn.Close()
	reader := codec.NewFrameReader(conn)
	_, _ = reader.ReadMessage()
}

func readSubscriptionRequest(t *testing.T, reader *codec.FrameReader) map[string]any {
	t.Helper()
	payload, err := reader.ReadMessage()
	if err != nil {
		t.Fatalf("read subscription request: %v", err)
	}
	reader.ResetMessage()
	value, err := codec.NewDecoder(bytes.NewReader(payload)).DecodeValue()
	if err != nil {
		t.Fatalf("decode subscription request: %v", err)
	}
	props, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("props are %T", value)
	}
	return props
}

func assertSubscriptionRequest(t *testing.T, props map[string]any, change int, ids []model.PointID) {
	t.Helper()
	if props[protocol.PropService] != "openplant" || props[protocol.PropAction] != protocol.ActionSelect {
		t.Fatalf("unexpected service/action: %#v", props)
	}
	if props[protocol.PropTable] != "W3.Realtime" {
		t.Fatalf("table=%v", props[protocol.PropTable])
	}
	if props[protocol.PropKey] != "ID" {
		t.Fatalf("key=%v", props[protocol.PropKey])
	}
	if got := int64ValueForTest(props[protocol.PropAsync]); got != 1 {
		t.Fatalf("async=%v", props[protocol.PropAsync])
	}
	if change >= 0 {
		if got := int64ValueForTest(props[protocol.PropSubscribe]); got != int64(change) {
			t.Fatalf("subscribe=%v want %d", props[protocol.PropSubscribe], change)
		}
	} else if _, ok := props[protocol.PropSubscribe]; ok {
		t.Fatalf("initial request should not include Subscribe: %#v", props)
	}
	indexes, ok := props[protocol.PropIndexes].(codec.Extension)
	if !ok || indexes.Type != protocol.IndexInt32Array {
		t.Fatalf("indexes=%#v", props[protocol.PropIndexes])
	}
	decoded, err := codec.NewDecoder(bytes.NewReader(indexes.Data)).DecodeValue()
	if err != nil {
		t.Fatalf("decode indexes: %v", err)
	}
	items, ok := decoded.([]any)
	if !ok {
		t.Fatalf("indexes decoded as %T", decoded)
	}
	got := make([]model.PointID, 0, len(items))
	for _, item := range items {
		got = append(got, model.PointID(int64ValueForTest(item)))
	}
	if !slices.Equal(got, ids) {
		t.Fatalf("ids=%v want %v", got, ids)
	}
}

func assertTableSubscriptionRequest(t *testing.T, props map[string]any, table, key string, indexType uint8, snapshot bool) {
	t.Helper()
	if props[protocol.PropService] != "openplant" || props[protocol.PropAction] != protocol.ActionSelect {
		t.Fatalf("unexpected service/action: %#v", props)
	}
	if props[protocol.PropTable] != table {
		t.Fatalf("table=%v want %s", props[protocol.PropTable], table)
	}
	if props[protocol.PropKey] != key {
		t.Fatalf("key=%v want %s", props[protocol.PropKey], key)
	}
	if got := int64ValueForTest(props[protocol.PropAsync]); got != 1 {
		t.Fatalf("async=%v", props[protocol.PropAsync])
	}
	if got, ok := props[protocol.PropSnapshot].(bool); !ok || got != snapshot {
		t.Fatalf("snapshot=%#v want %v", props[protocol.PropSnapshot], snapshot)
	}
	indexes, ok := props[protocol.PropIndexes].(codec.Extension)
	if !ok || indexes.Type != indexType {
		t.Fatalf("indexes=%#v want type=%d", props[protocol.PropIndexes], indexType)
	}
}

func writeSubscriptionEvent(t *testing.T, writer *codec.FrameWriter, id model.PointID, gn string, value float64) {
	t.Helper()
	columns := []codec.Column{
		{Name: "ID", Type: codec.VtInt32},
		{Name: "GN", Type: codec.VtString},
		{Name: "TM", Type: codec.VtDateTime},
		{Name: "DS", Type: codec.VtInt16},
		{Name: "AV", Type: codec.VtObject},
	}
	body, err := codec.EncodeDataSet(columns, []map[string]any{{
		"ID": int32(id),
		"GN": gn,
		"TM": time.Unix(123456, 0),
		"DS": int16(0),
		"AV": value,
	}})
	if err != nil {
		t.Fatalf("encode subscription event: %v", err)
	}
	var response bytes.Buffer
	if err := codec.NewEncoder(&response).EncodeMap(map[string]any{
		protocol.PropErrNo:   int32(0),
		protocol.PropColumns: codec.EncodeColumns(columns),
	}); err != nil {
		t.Fatalf("encode subscription event props: %v", err)
	}
	response.Write(body)
	if err := writer.WriteMessage(response.Bytes()); err != nil && err != io.ErrClosedPipe {
		t.Fatalf("write subscription event: %v", err)
	}
}

func writeTableSubscriptionEvent(t *testing.T, writer *codec.FrameWriter, columns []codec.Column, rows []map[string]any) {
	t.Helper()
	body, err := codec.EncodeDataSet(columns, rows)
	if err != nil {
		t.Fatalf("encode table subscription event: %v", err)
	}
	var response bytes.Buffer
	if err := codec.NewEncoder(&response).EncodeMap(map[string]any{
		protocol.PropErrNo:   int32(0),
		protocol.PropColumns: codec.EncodeColumns(columns),
	}); err != nil {
		t.Fatalf("encode table subscription event props: %v", err)
	}
	response.Write(body)
	if err := writer.WriteMessage(response.Bytes()); err != nil && err != io.ErrClosedPipe {
		t.Fatalf("write table subscription event: %v", err)
	}
}

func receiveSubscriptionEvent(t *testing.T, ch <-chan subscription.Event) subscription.Event {
	t.Helper()
	select {
	case event, ok := <-ch:
		if !ok {
			t.Fatalf("subscription events channel closed")
		}
		return event
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for subscription event")
		return subscription.Event{}
	}
}

func receiveTableSubscriptionEvent(t *testing.T, ch <-chan subscription.TableEvent) subscription.TableEvent {
	t.Helper()
	select {
	case event, ok := <-ch:
		if !ok {
			t.Fatalf("table subscription events channel closed")
		}
		return event
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for table subscription event")
		return subscription.TableEvent{}
	}
}

func receiveDataSubscriptionEvent(t *testing.T, ch <-chan subscription.Event) subscription.Event {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case event, ok := <-ch:
			if !ok {
				t.Fatalf("subscription events channel closed")
				return subscription.Event{}
			}
			if !event.IsData() {
				continue
			}
			return event
		case <-deadline:
			t.Fatalf("timed out waiting for subscription data event")
			return subscription.Event{}
		}
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func int64ValueForTest(v any) int64 {
	switch x := v.(type) {
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	case uint:
		return int64(x)
	default:
		return 0
	}
}
