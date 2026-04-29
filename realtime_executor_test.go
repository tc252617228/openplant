package openplant

import (
	"bytes"
	"context"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/internal/transport"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/realtime"
)

func TestRealtimeReadByIDUsesNativeProtocol(t *testing.T) {
	client := newPipeRealtimeClient(t)
	defer client.Close()

	samples, err := client.Realtime().Read(context.Background(), realtime.ReadRequest{
		DB:  "W3",
		IDs: []model.PointID{1001},
	})
	if err != nil {
		t.Fatalf("Realtime read failed: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("samples=%d want 1", len(samples))
	}
	value, ok := samples[0].Value.Float64()
	if !ok || value != 42.5 {
		t.Fatalf("value=%#v ok=%v", samples[0].Value.Interface(), ok)
	}
	if samples[0].ID != 1001 || samples[0].Type != model.TypeR8 || samples[0].Status != 0 {
		t.Fatalf("unexpected sample: %#v", samples[0])
	}
}

func TestRealtimeReadByGNDoesNotUseHiddenMetadataSQL(t *testing.T) {
	client := newPipeRealtimeClient(t)
	defer client.Close()

	_, err := client.Realtime().Read(context.Background(), realtime.ReadRequest{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err == nil || !operror.IsKind(err, operror.KindUnsupported) {
		t.Fatalf("expected unsupported native GN read, got %v", err)
	}
}

func TestClientReadRealtimeRejectsGNBeforeNativeWire(t *testing.T) {
	client := newPipeRealtimeClient(t)
	defer client.Close()

	_, err := client.ReadRealtime(context.Background(), realtime.ReadRequest{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err == nil || !operror.IsKind(err, operror.KindUnsupported) {
		t.Fatalf("expected unsupported native GN read, got %v", err)
	}
}

func TestRealtimeReadDeduplicatesIDsAndPreservesOutputOrder(t *testing.T) {
	cfg := DefaultOptions()
	cfg.RequestTimeout = time.Second
	client := newPipeRealtimeSequenceClient(t, cfg, [][]model.PointID{{1001, 1002}})
	defer client.Close()

	samples, err := client.Realtime().Read(context.Background(), realtime.ReadRequest{
		DB:  "W3",
		IDs: []model.PointID{1001, 1001, 1002},
	})
	if err != nil {
		t.Fatalf("Realtime read failed: %v", err)
	}
	if len(samples) != 3 {
		t.Fatalf("samples=%d want 3", len(samples))
	}
	if got := []model.PointID{samples[0].ID, samples[1].ID, samples[2].ID}; !slices.Equal(got, []model.PointID{1001, 1001, 1002}) {
		t.Fatalf("sample order=%v", got)
	}
}

func TestRealtimeReadChunksNativeRequests(t *testing.T) {
	cfg := DefaultOptions()
	cfg.RequestTimeout = time.Second
	cfg.ChunkSize = 2
	client := newPipeRealtimeSequenceClient(t, cfg, [][]model.PointID{{1001, 1002}, {1003}})
	defer client.Close()

	samples, err := client.Realtime().Read(context.Background(), realtime.ReadRequest{
		DB:  "W3",
		IDs: []model.PointID{1001, 1002, 1003},
	})
	if err != nil {
		t.Fatalf("Realtime read failed: %v", err)
	}
	if len(samples) != 3 {
		t.Fatalf("samples=%d want 3", len(samples))
	}
	if got := []model.PointID{samples[0].ID, samples[1].ID, samples[2].ID}; !slices.Equal(got, []model.PointID{1001, 1002, 1003}) {
		t.Fatalf("sample order=%v", got)
	}
}

func newPipeRealtimeClient(t *testing.T) *Client {
	t.Helper()
	cfg := DefaultOptions()
	cfg.RequestTimeout = time.Second
	c := &Client{options: cfg}
	c.pool = transport.NewPool(transport.Config{
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
			go serveRealtime(t, server)
			return client, nil
		},
	})
	c.realtime = realtime.NewService(realtime.Options{Reader: c})
	return c
}

func newPipeRealtimeSequenceClient(t *testing.T, cfg Options, expected [][]model.PointID) *Client {
	t.Helper()
	c := &Client{options: cfg}
	c.pool = transport.NewPool(transport.Config{
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
			go serveRealtimeSequence(t, server, expected)
			return client, nil
		},
	})
	c.realtime = realtime.NewService(realtime.Options{Reader: c})
	return c
}

func serveRealtime(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)

	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read realtime request: %v", err)
		return
	}
	req := codec.NewReader(bytes.NewReader(payload))
	magic, err := req.ReadInt32()
	if err != nil {
		t.Errorf("read magic: %v", err)
		return
	}
	if magic != protocol.Magic {
		t.Errorf("magic=%x", magic)
		return
	}
	command, _ := req.ReadInt32()
	url, _ := req.ReadInt32()
	if command != int32(protocol.CommandSelect) || url != int32(protocol.URLDynamic) {
		t.Errorf("command/url=%d/%x", command, url)
		return
	}
	_, _ = req.ReadInt16()
	_, _ = req.ReadInt16()
	count, _ := req.ReadInt32()
	if count != 1 {
		t.Errorf("count=%d", count)
		return
	}
	id, _ := req.ReadInt32()
	if id != 1001 {
		t.Errorf("id=%d", id)
		return
	}
	tail, _ := req.ReadInt32()
	if tail != protocol.Magic {
		t.Errorf("tail=%x", tail)
		return
	}

	var response bytes.Buffer
	resp := codec.NewWriter(&response)
	_ = resp.WriteInt32(protocol.Magic)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt32(1)
	_ = resp.WriteInt8(int8(model.TypeR8))
	_ = resp.WriteInt32(123456)
	_ = resp.WriteInt16(0)
	if err := codec.EncodeTSValue(&response, model.R8(42.5)); err != nil {
		t.Errorf("encode value: %v", err)
		return
	}
	_ = resp.WriteInt32(protocol.Magic)
	if err := writer.WriteMessage(response.Bytes()); err != nil && err != io.ErrClosedPipe {
		t.Errorf("write realtime response: %v", err)
	}
}

func serveRealtimeSequence(t *testing.T, conn net.Conn, expected [][]model.PointID) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)

	for _, want := range expected {
		ids, ok := readRealtimeRequest(t, reader)
		if !ok {
			return
		}
		if !slices.Equal(ids, want) {
			t.Errorf("realtime IDs=%v want %v", ids, want)
			return
		}
		writeRealtimeResponse(t, writer, ids)
	}
}

func readRealtimeRequest(t *testing.T, reader *codec.FrameReader) ([]model.PointID, bool) {
	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read realtime request: %v", err)
		return nil, false
	}
	reader.ResetMessage()
	req := codec.NewReader(bytes.NewReader(payload))
	magic, err := req.ReadInt32()
	if err != nil {
		t.Errorf("read magic: %v", err)
		return nil, false
	}
	if magic != protocol.Magic {
		t.Errorf("magic=%x", magic)
		return nil, false
	}
	command, _ := req.ReadInt32()
	url, _ := req.ReadInt32()
	if command != int32(protocol.CommandSelect) || url != int32(protocol.URLDynamic) {
		t.Errorf("command/url=%d/%x", command, url)
		return nil, false
	}
	_, _ = req.ReadInt16()
	_, _ = req.ReadInt16()
	count, _ := req.ReadInt32()
	ids := make([]model.PointID, 0, count)
	for i := int32(0); i < count; i++ {
		id, _ := req.ReadInt32()
		ids = append(ids, model.PointID(id))
	}
	tail, _ := req.ReadInt32()
	if tail != protocol.Magic {
		t.Errorf("tail=%x", tail)
		return nil, false
	}
	return ids, true
}

func writeRealtimeResponse(t *testing.T, writer *codec.FrameWriter, ids []model.PointID) {
	var response bytes.Buffer
	resp := codec.NewWriter(&response)
	_ = resp.WriteInt32(protocol.Magic)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt32(int32(len(ids)))
	for _, id := range ids {
		_ = resp.WriteInt8(int8(model.TypeR8))
		_ = resp.WriteInt32(123456)
		_ = resp.WriteInt16(0)
		if err := codec.EncodeTSValue(&response, model.R8(float64(id)+0.5)); err != nil {
			t.Errorf("encode value: %v", err)
			return
		}
	}
	_ = resp.WriteInt32(protocol.Magic)
	if err := writer.WriteMessage(response.Bytes()); err != nil && err != io.ErrClosedPipe {
		t.Errorf("write realtime response: %v", err)
	}
}
