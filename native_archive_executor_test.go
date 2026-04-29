package openplant

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/tc252617228/openplant/archive"
	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/internal/transport"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/stat"
)

func TestArchiveQueryNativeUsesArchiveProtocol(t *testing.T) {
	client := newPipeNativeArchiveClient(t, func(t *testing.T, conn net.Conn) {
		serveNativeArchiveR8(t, conn)
	})
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	samples, err := client.Archive().QueryNative(context.Background(), archive.Query{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:  model.ModeRaw,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("Archive QueryNative failed: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("samples=%d want 1", len(samples))
	}
	value, ok := samples[0].Value.Float64()
	if !ok || value != 12.5 {
		t.Fatalf("value=%#v ok=%v", samples[0].Value.Interface(), ok)
	}
	if samples[0].ID != 1001 || samples[0].Type != model.TypeR8 || samples[0].Status != 0 {
		t.Fatalf("sample=%#v", samples[0])
	}
}

func TestArchiveQueryNativeRejectsGNWithoutHiddenMetadataSQL(t *testing.T) {
	client, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer client.Close()

	_, err = client.Archive().QueryNative(context.Background(), archive.Query{
		DB:    "W3",
		GNs:   []model.GN{"W3.N.P1"},
		Range: model.TimeRange{Begin: time.Unix(100, 0), End: time.Unix(200, 0)},
		Mode:  model.ModeRaw,
	})
	if err == nil || !operror.IsKind(err, operror.KindUnsupported) {
		t.Fatalf("expected unsupported GN native query, got %v", err)
	}
}

func TestArchiveQueryNativeRejectsTimeOutsideNativeRange(t *testing.T) {
	client, err := New()
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer client.Close()

	begin := time.Unix(1<<31, 0)
	_, err = client.Archive().QueryNative(context.Background(), archive.Query{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:  model.ModeRaw,
	})
	if err == nil || !operror.IsKind(err, operror.KindValidation) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestArchiveStreamNativeEmitsWithoutAggregating(t *testing.T) {
	client := newPipeNativeArchiveClient(t, func(t *testing.T, conn net.Conn) {
		serveNativeArchiveTwoR8(t, conn)
	})
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var got []float64
	err := client.Archive().StreamNative(context.Background(), archive.Query{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:  model.ModeRaw,
	}, func(sample model.Sample) bool {
		value, ok := sample.Value.Float64()
		if !ok {
			t.Fatalf("sample value is %T", sample.Value.Interface())
		}
		got = append(got, value)
		return true
	})
	if err != nil {
		t.Fatalf("StreamNative failed: %v", err)
	}
	if len(got) != 2 || got[0] != 12.5 || got[1] != 13.5 {
		t.Fatalf("values=%v", got)
	}
}

func TestArchiveStreamNativeStopDropsConnection(t *testing.T) {
	client := newPipeNativeArchiveClient(t, func(t *testing.T, conn net.Conn) {
		serveNativeArchiveTwoR8(t, conn)
	})
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var got int
	err := client.Archive().StreamNative(context.Background(), archive.Query{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:  model.ModeRaw,
	}, func(sample model.Sample) bool {
		got++
		return false
	})
	if err != nil {
		t.Fatalf("StreamNative stop failed: %v", err)
	}
	if got != 1 {
		t.Fatalf("callbacks=%d want 1", got)
	}
	if stats := client.pool.Stats(); stats.Open != 0 || stats.Idle != 0 {
		t.Fatalf("stats=%#v want stopped stream connection dropped", stats)
	}
}

func TestStatQueryNativeUsesArchiveProtocol(t *testing.T) {
	client := newPipeNativeArchiveClient(t, func(t *testing.T, conn net.Conn) {
		serveNativeStatAvg(t, conn)
	})
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	samples, err := client.Stat().QueryNative(context.Background(), stat.Query{
		DB:       "W3",
		IDs:      []model.PointID{1001},
		Range:    model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:     model.ModeAvg,
		Interval: "1m",
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("Stat QueryNative failed: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("samples=%d want 1", len(samples))
	}
	if samples[0].ID != 1001 || samples[0].Avg != 22.25 || samples[0].Status != 0 {
		t.Fatalf("sample=%#v", samples[0])
	}
}

func TestStatStreamNativeEmitsSamples(t *testing.T) {
	client := newPipeNativeArchiveClient(t, func(t *testing.T, conn net.Conn) {
		serveNativeStatAvg(t, conn)
	})
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var got []model.StatSample
	err := client.Stat().StreamNative(context.Background(), stat.Query{
		DB:       "W3",
		IDs:      []model.PointID{1001},
		Range:    model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:     model.ModeAvg,
		Interval: "1m",
	}, func(sample model.StatSample) bool {
		got = append(got, sample)
		return true
	})
	if err != nil {
		t.Fatalf("StreamNative failed: %v", err)
	}
	if len(got) != 1 || got[0].Avg != 22.25 {
		t.Fatalf("samples=%#v", got)
	}
}

func TestArchiveQueryNativePropagatesServerError(t *testing.T) {
	client := newPipeNativeArchiveClient(t, func(t *testing.T, conn net.Conn) {
		serveNativeArchiveServerError(t, conn)
	})
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	_, err := client.Archive().QueryNative(context.Background(), archive.Query{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:  model.ModeRaw,
	})
	if err == nil || !operror.IsKind(err, operror.KindServer) {
		t.Fatalf("expected native server error, got %v", err)
	}
}

func TestArchiveQueryNativeTimeoutDropsConnection(t *testing.T) {
	requestRead := make(chan struct{})
	cfg := DefaultOptions()
	cfg.RequestTimeout = 50 * time.Millisecond
	client := newPipeNativeArchiveClientWithOptions(t, cfg, func(t *testing.T, conn net.Conn) {
		serveNativeArchiveWithoutResponse(t, conn, requestRead)
	})
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	_, err := client.Archive().QueryNative(context.Background(), archive.Query{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:  model.ModeRaw,
	})
	if err == nil || !operror.IsKind(err, operror.KindTimeout) {
		t.Fatalf("expected timeout error, got %v", err)
	}
	select {
	case <-requestRead:
	case <-time.After(time.Second):
		t.Fatalf("server did not receive native archive request")
	}
	if stats := client.pool.Stats(); stats.Open != 0 || stats.Idle != 0 {
		t.Fatalf("stats=%#v want timed out connection dropped", stats)
	}
}

func newPipeNativeArchiveClient(t *testing.T, serve func(*testing.T, net.Conn)) *Client {
	t.Helper()
	cfg := DefaultOptions()
	cfg.RequestTimeout = time.Second
	return newPipeNativeArchiveClientWithOptions(t, cfg, serve)
}

func newPipeNativeArchiveClientWithOptions(t *testing.T, cfg Options, serve func(*testing.T, net.Conn)) *Client {
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
			go serve(t, server)
			return client, nil
		},
	})
	c.archive = archive.NewService(archive.Options{Native: c, Streamer: c})
	c.stat = stat.NewService(stat.Options{Native: c, Streamer: c})
	return c
}

func serveNativeArchiveR8(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer, reader := handshakeNativeServer(t, conn)
	req, ok := readNativeArchiveWireRequest(t, reader)
	if !ok {
		return
	}
	assertNativeArchiveRequest(t, req, nativeArchiveWireRequest{
		Rows: []nativeArchiveWireRow{{
			ID:       1001,
			Mode:     nativeModeRaw,
			Quality:  0,
			Begin:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Unix(),
			End:      time.Date(2026, 1, 2, 4, 4, 5, 0, time.UTC).Unix(),
			Interval: 1,
		}},
	})
	writeNativeArchiveR8Response(t, writer)
}

func serveNativeArchiveTwoR8(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer, reader := handshakeNativeServer(t, conn)
	req, ok := readNativeArchiveWireRequest(t, reader)
	if !ok {
		return
	}
	assertNativeArchiveRequest(t, req, nativeArchiveWireRequest{
		Rows: []nativeArchiveWireRow{{
			ID:       1001,
			Mode:     nativeModeRaw,
			Quality:  0,
			Begin:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Unix(),
			End:      time.Date(2026, 1, 2, 4, 4, 5, 0, time.UTC).Unix(),
			Interval: 1,
		}},
	})
	writeNativeArchiveTwoR8Response(t, writer)
}

func serveNativeStatAvg(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer, reader := handshakeNativeServer(t, conn)
	req, ok := readNativeArchiveWireRequest(t, reader)
	if !ok {
		return
	}
	assertNativeArchiveRequest(t, req, nativeArchiveWireRequest{
		Rows: []nativeArchiveWireRow{{
			ID:       1001,
			Mode:     nativeModeAvg,
			Quality:  0,
			Begin:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Unix(),
			End:      time.Date(2026, 1, 2, 4, 4, 5, 0, time.UTC).Unix(),
			Interval: 60,
		}},
	})
	writeNativeStatAvgResponse(t, writer)
}

func serveNativeArchiveServerError(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer, reader := handshakeNativeServer(t, conn)
	if _, ok := readNativeArchiveWireRequest(t, reader); !ok {
		return
	}
	writeNativeServerErrorResponse(t, writer, -113)
}

func serveNativeArchiveWithoutResponse(t *testing.T, conn net.Conn, requestRead chan<- struct{}) {
	defer conn.Close()
	_, reader := handshakeNativeServer(t, conn)
	if _, ok := readNativeArchiveWireRequest(t, reader); !ok {
		return
	}
	close(requestRead)
	time.Sleep(200 * time.Millisecond)
}

func handshakeNativeServer(t *testing.T, conn net.Conn) (*codec.FrameWriter, *codec.FrameReader) {
	t.Helper()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)
	return writer, reader
}

type nativeArchiveWireRequest struct {
	Rows []nativeArchiveWireRow
}

type nativeArchiveWireRow struct {
	ID       model.PointID
	Mode     int32
	Quality  int32
	Begin    int64
	End      int64
	Interval int32
}

func readNativeArchiveWireRequest(t *testing.T, reader *codec.FrameReader) (nativeArchiveWireRequest, bool) {
	t.Helper()
	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read native archive request: %v", err)
		return nativeArchiveWireRequest{}, false
	}
	req := codec.NewReader(bytes.NewReader(payload))
	magic, err := req.ReadInt32()
	if err != nil {
		t.Errorf("read magic: %v", err)
		return nativeArchiveWireRequest{}, false
	}
	if magic != protocol.Magic {
		t.Errorf("magic=%x", magic)
		return nativeArchiveWireRequest{}, false
	}
	command, _ := req.ReadInt32()
	url, _ := req.ReadInt32()
	if command != int32(protocol.CommandSelect) || url != int32(protocol.URLArchive) {
		t.Errorf("command/url=%d/%x", command, url)
		return nativeArchiveWireRequest{}, false
	}
	_, _ = req.ReadInt32()
	count, _ := req.ReadInt32()
	rows := make([]nativeArchiveWireRow, 0, count)
	for i := int32(0); i < count; i++ {
		id, _ := req.ReadInt32()
		mode, _ := req.ReadInt32()
		quality, _ := req.ReadInt32()
		begin, _ := req.ReadInt32()
		end, _ := req.ReadInt32()
		interval, _ := req.ReadInt32()
		rows = append(rows, nativeArchiveWireRow{
			ID:       model.PointID(id),
			Mode:     mode,
			Quality:  quality,
			Begin:    int64(begin),
			End:      int64(end),
			Interval: interval,
		})
	}
	tail, _ := req.ReadInt32()
	if tail != protocol.Magic {
		t.Errorf("tail=%x", tail)
		return nativeArchiveWireRequest{}, false
	}
	return nativeArchiveWireRequest{Rows: rows}, true
}

func assertNativeArchiveRequest(t *testing.T, got, want nativeArchiveWireRequest) {
	t.Helper()
	if len(got.Rows) != len(want.Rows) {
		t.Fatalf("row count=%d want %d: %#v", len(got.Rows), len(want.Rows), got)
	}
	for i := range want.Rows {
		if got.Rows[i] != want.Rows[i] {
			t.Fatalf("row %d=%#v want %#v", i, got.Rows[i], want.Rows[i])
		}
	}
}

func writeNativeArchiveR8Response(t *testing.T, writer *codec.FrameWriter) {
	t.Helper()
	var response bytes.Buffer
	resp := codec.NewWriter(&response)
	_ = resp.WriteInt32(protocol.Magic)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt32(1)
	_ = resp.WriteInt8(1)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt8(int8(model.TypeR8))
	_ = resp.WriteInt32(1)
	_ = resp.WriteInt32(123456)
	_ = resp.WriteInt16(0)
	if err := codec.EncodeTSValue(&response, model.R8(12.5)); err != nil {
		t.Errorf("encode archive value: %v", err)
		return
	}
	_ = resp.WriteInt8(0)
	_ = resp.WriteInt32(protocol.Magic)
	if err := writer.WriteMessage(response.Bytes()); err != nil && err != io.ErrClosedPipe {
		t.Errorf("write native archive response: %v", err)
	}
}

func writeNativeArchiveTwoR8Response(t *testing.T, writer *codec.FrameWriter) {
	t.Helper()
	var response bytes.Buffer
	resp := codec.NewWriter(&response)
	_ = resp.WriteInt32(protocol.Magic)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt32(1)
	_ = resp.WriteInt8(1)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt8(int8(model.TypeR8))
	_ = resp.WriteInt32(2)
	_ = resp.WriteInt32(123456)
	_ = resp.WriteInt16(0)
	if err := codec.EncodeTSValue(&response, model.R8(12.5)); err != nil {
		t.Errorf("encode archive value: %v", err)
		return
	}
	_ = resp.WriteInt32(123457)
	_ = resp.WriteInt16(0)
	if err := codec.EncodeTSValue(&response, model.R8(13.5)); err != nil {
		t.Errorf("encode archive value: %v", err)
		return
	}
	_ = resp.WriteInt8(0)
	_ = resp.WriteInt32(protocol.Magic)
	if err := writer.WriteMessage(response.Bytes()); err != nil && err != io.ErrClosedPipe {
		t.Errorf("write native archive response: %v", err)
	}
}

func writeNativeStatAvgResponse(t *testing.T, writer *codec.FrameWriter) {
	t.Helper()
	var response bytes.Buffer
	resp := codec.NewWriter(&response)
	_ = resp.WriteInt32(protocol.Magic)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt32(1)
	_ = resp.WriteInt8(1)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt8(int8(model.TypeR8))
	_ = resp.WriteInt32(1)
	_ = resp.WriteInt32(123456)
	_ = resp.WriteInt16(0)
	_ = resp.WriteFloat64(22.25)
	_ = resp.WriteInt8(0)
	_ = resp.WriteInt32(protocol.Magic)
	if err := writer.WriteMessage(response.Bytes()); err != nil && err != io.ErrClosedPipe {
		t.Errorf("write native stat response: %v", err)
	}
}

func writeNativeServerErrorResponse(t *testing.T, writer *codec.FrameWriter, code int32) {
	t.Helper()
	var response bytes.Buffer
	resp := codec.NewWriter(&response)
	_ = resp.WriteInt32(protocol.Magic)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt32(1)
	_ = resp.WriteInt8(1)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt8(-1)
	_ = resp.WriteInt32(code)
	_ = resp.WriteInt8(0)
	_ = resp.WriteInt32(protocol.Magic)
	if err := writer.WriteMessage(response.Bytes()); err != nil && err != io.ErrClosedPipe {
		t.Errorf("write native server error response: %v", err)
	}
}
