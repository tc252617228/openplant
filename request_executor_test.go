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
	"github.com/tc252617228/openplant/realtime"
	"github.com/tc252617228/openplant/stat"
)

func TestArchiveQueryRequestUsesTableSelectProtocol(t *testing.T) {
	client := newPipeArchiveRequestClient(t)
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	end := time.Date(2026, 1, 2, 4, 4, 5, 987654321, time.UTC)
	samples, err := client.Archive().QueryRequest(context.Background(), archive.Query{
		DB:      "W3",
		IDs:     []model.PointID{1001},
		Range:   model.TimeRange{Begin: begin, End: end},
		Mode:    model.ModeRaw,
		Quality: model.QualityDropBad,
		Limit:   5,
	})
	if err != nil {
		t.Fatalf("Archive query failed: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("samples=%d want 1", len(samples))
	}
	sample := samples[0]
	value, ok := sample.Value.Float64()
	if !ok || value != 12.5 {
		t.Fatalf("value=%#v ok=%v", sample.Value.Interface(), ok)
	}
	if sample.ID != 1001 || sample.GN != "W3.N.P1" || sample.Status != 0 {
		t.Fatalf("unexpected sample: %#v", sample)
	}
}

func newPipeArchiveRequestClient(t *testing.T) *Client {
	return newPipeArchiveRequestClientWithServer(t, serveArchiveRequest)
}

func TestArchiveQueryRequestUsesGNIndexWithoutMetadataResolution(t *testing.T) {
	client := newPipeArchiveRequestClientWithServer(t, serveArchiveGNRequest)
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	samples, err := client.Archive().QueryRequest(context.Background(), archive.Query{
		DB:      "W3",
		GNs:     []model.GN{"W3.N.P1"},
		Range:   model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:    model.ModeRaw,
		Quality: model.QualityNone,
		Limit:   5,
	})
	if err != nil {
		t.Fatalf("Archive query failed: %v", err)
	}
	if len(samples) != 1 || samples[0].ID != 1001 || samples[0].GN != "W3.N.P1" {
		t.Fatalf("unexpected samples: %#v", samples)
	}
}

func TestArchiveQueryRequestLimitIsGlobalAcrossChunks(t *testing.T) {
	client := newPipeArchiveRequestClientWithServer(t, serveArchiveLimitRequests)
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	samples, err := client.Archive().QueryRequest(context.Background(), archive.Query{
		DB:        "W3",
		IDs:       []model.PointID{1001, 1002},
		Range:     model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:      model.ModeRaw,
		Limit:     3,
		ChunkSize: 1,
	})
	if err != nil {
		t.Fatalf("Archive query failed: %v", err)
	}
	if len(samples) != 3 {
		t.Fatalf("samples=%d want global limit 3: %#v", len(samples), samples)
	}
}

func TestStatQueryRequestLimitIsGlobalAcrossChunks(t *testing.T) {
	client := newPipeStatRequestClientWithServer(t, serveStatLimitRequests)
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	samples, err := client.Stat().QueryRequest(context.Background(), stat.Query{
		DB:        "W3",
		IDs:       []model.PointID{1001, 1002},
		Range:     model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:      model.ModeAvg,
		Interval:  "1m",
		Limit:     3,
		ChunkSize: 1,
	})
	if err != nil {
		t.Fatalf("Stat query failed: %v", err)
	}
	if len(samples) != 3 {
		t.Fatalf("samples=%d want global limit 3: %#v", len(samples), samples)
	}
}

func TestRealtimeQueryRequestUsesGNIndex(t *testing.T) {
	client := newPipeRealtimeRequestClient(t, serveRealtimeGNRequest)
	defer client.Close()

	samples, err := client.Realtime().QueryRequest(context.Background(), realtime.ReadRequest{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err != nil {
		t.Fatalf("Realtime query failed: %v", err)
	}
	if len(samples) != 1 || samples[0].ID != 1001 || samples[0].GN != "W3.N.P1" {
		t.Fatalf("unexpected samples: %#v", samples)
	}
}

func TestClientRequestPathsValidateBeforeWire(t *testing.T) {
	client := &Client{options: DefaultOptions()}
	if _, err := client.QueryRealtimeByRequest(context.Background(), realtime.ReadRequest{DB: "W3"}); err == nil {
		t.Fatalf("expected direct realtime request path to reject an empty selector")
	}

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if _, err := client.QueryArchiveByRequest(context.Background(), archive.Query{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin},
		Mode:  model.ModeRaw,
	}); err == nil {
		t.Fatalf("expected direct archive request path to reject an invalid time range")
	}

	if _, err := client.QueryStatByRequest(context.Background(), stat.Query{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
	}); err == nil {
		t.Fatalf("expected direct stat request path to reject a missing interval")
	}
}

func newPipeArchiveRequestClientWithServer(t *testing.T, serve func(*testing.T, net.Conn)) *Client {
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
			go serve(t, server)
			return client, nil
		},
	})
	c.archive = archive.NewService(archive.Options{Requester: c})
	return c
}

func newPipeRealtimeRequestClient(t *testing.T, serve func(*testing.T, net.Conn)) *Client {
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
			go serve(t, server)
			return client, nil
		},
	})
	c.realtime = realtime.NewService(realtime.Options{Requester: c})
	return c
}

func newPipeStatRequestClientWithServer(t *testing.T, serve func(*testing.T, net.Conn)) *Client {
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
			go serve(t, server)
			return client, nil
		},
	})
	c.stat = stat.NewService(stat.Options{Requester: c})
	return c
}

func serveArchiveRequest(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)

	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read archive request: %v", err)
		return
	}
	value, err := codec.NewDecoder(bytes.NewReader(payload)).DecodeValue()
	if err != nil {
		t.Errorf("decode archive request props: %v", err)
		return
	}
	props, ok := value.(map[string]any)
	if !ok {
		t.Errorf("archive request props are %T", value)
		return
	}
	if props[protocol.PropAction] != protocol.ActionSelect {
		t.Errorf("action=%v", props[protocol.PropAction])
		return
	}
	if props[protocol.PropTable] != "W3.Archive" {
		t.Errorf("table=%v", props[protocol.PropTable])
		return
	}
	if props["mode"] != "raw" || props["interval"] != "1" || int64ValueForTest(props["qtype"]) != int64(model.QualityDropBad) || props[protocol.PropLimit] != "5" {
		t.Errorf("unexpected mode/interval/limit: %#v", props)
		return
	}
	indexes, ok := props[protocol.PropIndexes].(codec.Extension)
	if !ok {
		t.Errorf("indexes are %T", props[protocol.PropIndexes])
		return
	}
	if indexes.Type != protocol.IndexInt32Array {
		t.Errorf("index type=%d", indexes.Type)
		return
	}
	decodedIndexes, err := codec.NewDecoder(bytes.NewReader(indexes.Data)).DecodeValue()
	if err != nil {
		t.Errorf("decode indexes: %v", err)
		return
	}
	items, ok := decodedIndexes.([]any)
	if !ok || len(items) != 1 || items[0] != int64(1001) {
		t.Errorf("indexes=%#v", decodedIndexes)
		return
	}
	filters, ok := props[protocol.PropFilters].([]any)
	if !ok || len(filters) != 2 {
		t.Errorf("filters=%#v", props[protocol.PropFilters])
		return
	}
	if !filterRightValuesEqual(filters, []string{"2026-01-02 03:04:05.123", "2026-01-02 04:04:05.987"}) {
		t.Errorf("unexpected filters=%#v", filters)
		return
	}

	writeArchiveRequestResponse(t, writer)
}

func serveArchiveLimitRequests(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)

	for i, wantLimit := range []string{"3", "1"} {
		props, ok := readRequestProps(t, reader)
		if !ok {
			return
		}
		if props[protocol.PropTable] != "W3.Archive" || props[protocol.PropLimit] != wantLimit {
			t.Errorf("request %d unexpected table/limit: %#v", i, props)
			return
		}
		writeArchiveRequestRowsResponse(t, writer, []map[string]any{
			{"ID": int32(1000 + i), "GN": "W3.N.P1", "TM": time.Unix(123456+int64(i*10), 0), "DS": int16(0), "AV": float64(12.5)},
			{"ID": int32(2000 + i), "GN": "W3.N.P2", "TM": time.Unix(123457+int64(i*10), 0), "DS": int16(0), "AV": float64(13.5)},
		})
	}
}

func serveStatLimitRequests(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)

	for i, wantLimit := range []string{"3", "1"} {
		props, ok := readRequestProps(t, reader)
		if !ok {
			return
		}
		if props[protocol.PropTable] != "W3.Stat" || props[protocol.PropLimit] != wantLimit {
			t.Errorf("request %d unexpected table/limit: %#v", i, props)
			return
		}
		writeStatRequestRowsResponse(t, writer, []map[string]any{
			{"ID": int32(1000 + i), "GN": "W3.N.P1", "TM": time.Unix(123456+int64(i*10), 0), "DS": int16(0), "FLOW": float64(1), "AVGV": float64(2), "MAXV": float64(3), "MINV": float64(4), "MAXTIME": time.Unix(123456, 0), "MINTIME": time.Unix(123456, 0)},
			{"ID": int32(2000 + i), "GN": "W3.N.P2", "TM": time.Unix(123457+int64(i*10), 0), "DS": int16(0), "FLOW": float64(5), "AVGV": float64(6), "MAXV": float64(7), "MINV": float64(8), "MAXTIME": time.Unix(123457, 0), "MINTIME": time.Unix(123457, 0)},
		})
	}
}

func serveArchiveGNRequest(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)

	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read archive GN request: %v", err)
		return
	}
	value, err := codec.NewDecoder(bytes.NewReader(payload)).DecodeValue()
	if err != nil {
		t.Errorf("decode archive GN request props: %v", err)
		return
	}
	props, ok := value.(map[string]any)
	if !ok {
		t.Errorf("archive GN request props are %T", value)
		return
	}
	if props[protocol.PropTable] != "W3.Archive" || props[protocol.PropKey] != "GN" {
		t.Errorf("unexpected table/key: %#v", props)
		return
	}
	indexes, ok := props[protocol.PropIndexes].(codec.Extension)
	if !ok {
		t.Errorf("indexes are %T", props[protocol.PropIndexes])
		return
	}
	if indexes.Type != protocol.IndexStringArray {
		t.Errorf("index type=%d", indexes.Type)
		return
	}
	decodedIndexes, err := codec.NewDecoder(bytes.NewReader(indexes.Data)).DecodeValue()
	if err != nil {
		t.Errorf("decode indexes: %v", err)
		return
	}
	items, ok := decodedIndexes.([]any)
	if !ok || len(items) != 1 || items[0] != "W3.N.P1" {
		t.Errorf("indexes=%#v", decodedIndexes)
		return
	}

	writeArchiveRequestResponse(t, writer)
}

func serveRealtimeGNRequest(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)

	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read realtime GN request: %v", err)
		return
	}
	value, err := codec.NewDecoder(bytes.NewReader(payload)).DecodeValue()
	if err != nil {
		t.Errorf("decode realtime GN request props: %v", err)
		return
	}
	props, ok := value.(map[string]any)
	if !ok {
		t.Errorf("realtime GN request props are %T", value)
		return
	}
	if props[protocol.PropTable] != "W3.Realtime" || props[protocol.PropKey] != "GN" {
		t.Errorf("unexpected table/key: %#v", props)
		return
	}
	indexes, ok := props[protocol.PropIndexes].(codec.Extension)
	if !ok {
		t.Errorf("indexes are %T", props[protocol.PropIndexes])
		return
	}
	if indexes.Type != protocol.IndexStringArray {
		t.Errorf("index type=%d", indexes.Type)
		return
	}
	decodedIndexes, err := codec.NewDecoder(bytes.NewReader(indexes.Data)).DecodeValue()
	if err != nil {
		t.Errorf("decode indexes: %v", err)
		return
	}
	items, ok := decodedIndexes.([]any)
	if !ok || len(items) != 1 || items[0] != "W3.N.P1" {
		t.Errorf("indexes=%#v", decodedIndexes)
		return
	}

	writeArchiveRequestResponse(t, writer)
}

func readRequestProps(t *testing.T, reader *codec.FrameReader) (map[string]any, bool) {
	t.Helper()
	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read request: %v", err)
		return nil, false
	}
	reader.ResetMessage()
	value, err := codec.NewDecoder(bytes.NewReader(payload)).DecodeValue()
	if err != nil {
		t.Errorf("decode request props: %v", err)
		return nil, false
	}
	props, ok := value.(map[string]any)
	if !ok {
		t.Errorf("request props are %T", value)
		return nil, false
	}
	return props, true
}

func writeArchiveRequestResponse(t *testing.T, writer *codec.FrameWriter) {
	t.Helper()
	columns := []codec.Column{
		{Name: "ID", Type: codec.VtInt32},
		{Name: "GN", Type: codec.VtString},
		{Name: "TM", Type: codec.VtDateTime},
		{Name: "DS", Type: codec.VtInt16},
		{Name: "AV", Type: codec.VtObject},
	}
	writeRowsResponse(t, writer, columns, []map[string]any{{
		"ID": int32(1001),
		"GN": "W3.N.P1",
		"TM": time.Unix(123456, 0),
		"DS": int16(0),
		"AV": float64(12.5),
	}})
}

func writeArchiveRequestRowsResponse(t *testing.T, writer *codec.FrameWriter, rows []map[string]any) {
	t.Helper()
	writeRowsResponse(t, writer, []codec.Column{
		{Name: "ID", Type: codec.VtInt32},
		{Name: "GN", Type: codec.VtString},
		{Name: "TM", Type: codec.VtDateTime},
		{Name: "DS", Type: codec.VtInt16},
		{Name: "AV", Type: codec.VtObject},
	}, rows)
}

func writeStatRequestRowsResponse(t *testing.T, writer *codec.FrameWriter, rows []map[string]any) {
	t.Helper()
	writeRowsResponse(t, writer, []codec.Column{
		{Name: "ID", Type: codec.VtInt32},
		{Name: "GN", Type: codec.VtString},
		{Name: "TM", Type: codec.VtDateTime},
		{Name: "DS", Type: codec.VtInt16},
		{Name: "FLOW", Type: codec.VtDouble},
		{Name: "AVGV", Type: codec.VtDouble},
		{Name: "MAXV", Type: codec.VtDouble},
		{Name: "MINV", Type: codec.VtDouble},
		{Name: "MAXTIME", Type: codec.VtDateTime},
		{Name: "MINTIME", Type: codec.VtDateTime},
	}, rows)
}

func writeRowsResponse(t *testing.T, writer *codec.FrameWriter, columns []codec.Column, rows []map[string]any) {
	t.Helper()
	body, err := codec.EncodeDataSet(columns, rows)
	if err != nil {
		t.Errorf("encode dataset: %v", err)
		return
	}
	var response bytes.Buffer
	if err := codec.NewEncoder(&response).EncodeMap(map[string]any{
		protocol.PropErrNo:   int32(0),
		protocol.PropColumns: codec.EncodeColumns(columns),
	}); err != nil {
		t.Errorf("encode response props: %v", err)
		return
	}
	response.Write(body)
	if err := writer.WriteMessage(response.Bytes()); err != nil && err != io.ErrClosedPipe {
		t.Errorf("write response: %v", err)
	}
}

func filterRightValuesEqual(filters []any, want []string) bool {
	if len(filters) != len(want) {
		return false
	}
	for i, filter := range filters {
		item, ok := filter.(map[string]any)
		if !ok {
			return false
		}
		if item["R"] != want[i] {
			return false
		}
	}
	return true
}
