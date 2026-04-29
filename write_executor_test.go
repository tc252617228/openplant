package openplant

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/tc252617228/openplant/admin"
	"github.com/tc252617228/openplant/archive"
	"github.com/tc252617228/openplant/internal/cache"
	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/internal/transport"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/realtime"
)

func TestRealtimeWriteNativeUsesNativeInsertProtocol(t *testing.T) {
	client := newPipeWritableClient(t, serveRealtimeWrite)
	defer client.Close()

	tm := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	err := client.Realtime().WriteNative(context.Background(), realtime.WriteRequest{
		DB: "W3",
		Values: []realtime.Write{{
			ID:     1001,
			Type:   model.TypeR8,
			Time:   tm,
			Status: 0,
			Value:  model.R8(12.5),
		}},
	})
	if err != nil {
		t.Fatalf("WriteNative failed: %v", err)
	}
}

func TestArchiveWriteNativeUsesNativeArchiveInsertProtocol(t *testing.T) {
	client := newPipeWritableClient(t, serveArchiveWrite)
	defer client.Close()

	tm := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	err := client.Archive().WriteNative(context.Background(), archive.WriteRequest{
		DB:    "W3",
		Cache: true,
		Samples: []model.Sample{{
			ID:     1001,
			Type:   model.TypeR8,
			Time:   tm,
			Status: model.DSBadQuality,
			Value:  model.R8(22.5),
		}},
	})
	if err != nil {
		t.Fatalf("WriteNative failed: %v", err)
	}
}

func TestArchiveDeleteNativeUsesNativeArchiveDeleteProtocol(t *testing.T) {
	client := newPipeWritableClient(t, serveArchiveDelete)
	defer client.Close()

	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	err := client.Archive().DeleteNative(context.Background(), archive.DeleteRequest{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
	})
	if err != nil {
		t.Fatalf("DeleteNative failed: %v", err)
	}
}

func TestAdminMutateTableUsesStructuredRequestProtocol(t *testing.T) {
	client := newPipeWritableClient(t, serveTableMutation)
	defer client.Close()
	client.pointCache = cache.NewPointCache(time.Minute)
	client.pointCache.StorePoint("W3", model.Point{ID: 1001, GN: "W3.TEST.P1"})

	err := client.Admin().MutateTable(context.Background(), admin.TableMutation{
		DB:     "W3",
		Table:  "Point",
		Action: admin.MutationUpdate,
		Filters: []admin.Filter{{
			Left:     "GN",
			Operator: admin.FilterEQ,
			Right:    "W3.TEST.P1",
			Relation: admin.FilterAnd,
		}},
		Columns: []admin.Column{
			{Name: "ED", Type: admin.ColumnString},
			{Name: "FQ", Type: admin.ColumnInt16},
		},
		Rows: []admin.Row{{
			"ED": "phase 5 test",
			"FQ": int16(2),
		}},
	})
	if err != nil {
		t.Fatalf("MutateTable failed: %v", err)
	}
	if _, ok := client.pointCache.GetByGN("W3", "W3.TEST.P1"); ok {
		t.Fatalf("point cache was not invalidated after Point table mutation")
	}
}

func TestAdminMutateTableAcceptsEmptySuccessResponse(t *testing.T) {
	client := newPipeWritableClient(t, serveTableMutationEmptyResponse)
	defer client.Close()

	err := client.Admin().MutateTable(context.Background(), admin.TableMutation{
		DB:     "W3",
		Table:  "Node",
		Action: admin.MutationDelete,
		Indexes: &admin.Indexes{
			Key:   "ID",
			Int32: []int32{59},
		},
	})
	if err != nil {
		t.Fatalf("MutateTable failed: %v", err)
	}
}

func TestMutationServicesRejectReadonlyMode(t *testing.T) {
	cfg := DefaultOptions()
	cfg.ReadOnly = true
	client := &Client{options: cfg}
	client.realtime = realtime.NewService(realtime.Options{ReadOnly: true, Writer: client})
	client.archive = archive.NewService(archive.Options{ReadOnly: true, Writer: client})
	client.admin = admin.NewService(admin.Options{ReadOnly: true, Mutator: client})

	tm := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := client.Realtime().WriteNative(context.Background(), realtime.WriteRequest{
		DB: "W3",
		Values: []realtime.Write{{
			ID:    1001,
			Type:  model.TypeR8,
			Time:  tm,
			Value: model.R8(1),
		}},
	}); !operror.IsKind(err, operror.KindReadOnly) {
		t.Fatalf("realtime readonly err=%v", err)
	}
	if err := client.Archive().WriteNative(context.Background(), archive.WriteRequest{
		DB: "W3",
		Samples: []model.Sample{{
			ID:    1001,
			Type:  model.TypeR8,
			Time:  tm,
			Value: model.R8(1),
		}},
	}); !operror.IsKind(err, operror.KindReadOnly) {
		t.Fatalf("archive readonly err=%v", err)
	}
	if err := client.Admin().MutateTable(context.Background(), admin.TableMutation{
		Table:  "Point",
		Action: admin.MutationDelete,
		Filters: []admin.Filter{{
			Left:     "GN",
			Operator: admin.FilterEQ,
			Right:    "W3.TEST.P1",
			Relation: admin.FilterAnd,
		}},
	}); !operror.IsKind(err, operror.KindReadOnly) {
		t.Fatalf("admin readonly err=%v", err)
	}
	if err := client.WriteRealtimeNative(context.Background(), realtime.WriteRequest{
		DB: "W3",
		Values: []realtime.Write{{
			ID:    1001,
			Type:  model.TypeR8,
			Time:  tm,
			Value: model.R8(1),
		}},
	}); !operror.IsKind(err, operror.KindReadOnly) {
		t.Fatalf("direct client readonly err=%v", err)
	}
}

func newPipeWritableClient(t *testing.T, serve func(*testing.T, net.Conn)) *Client {
	t.Helper()
	cfg := DefaultOptions()
	cfg.ReadOnly = false
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
	c.realtime = realtime.NewService(realtime.Options{ReadOnly: false, Writer: c})
	c.archive = archive.NewService(archive.Options{ReadOnly: false, Writer: c})
	c.admin = admin.NewService(admin.Options{ReadOnly: false, Mutator: c})
	return c
}

func serveRealtimeWrite(t *testing.T, conn net.Conn) {
	defer conn.Close()
	_, reader := handshakeNativeServer(t, conn)
	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read realtime write: %v", err)
		return
	}
	req := codec.NewReader(bytes.NewReader(payload))
	assertNativeWriteHeader(t, req, protocol.CommandInsert, protocol.URLDynamic, protocol.FlagWall, 1)
	typ, _ := req.ReadInt8()
	if model.PointType(typ) != model.TypeR8 {
		t.Errorf("type=%d", typ)
		return
	}
	id, _ := req.ReadInt32()
	tm, _ := req.ReadInt32()
	ds, _ := req.ReadInt16()
	value, _ := req.ReadFloat64()
	tail, _ := req.ReadInt32()
	if id != 1001 || tm != 1767323045 || ds != 0 || value != 12.5 || tail != protocol.Magic {
		t.Errorf("unexpected realtime write id=%d tm=%d ds=%d value=%f tail=%x", id, tm, ds, value, tail)
		return
	}
	writeNativeEcho(t, conn, 0)
}

func serveArchiveWrite(t *testing.T, conn net.Conn) {
	defer conn.Close()
	_, reader := handshakeNativeServer(t, conn)
	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read archive write: %v", err)
		return
	}
	req := codec.NewReader(bytes.NewReader(payload))
	assertNativeWriteHeader(t, req, protocol.CommandInsert, protocol.URLArchive, protocol.FlagWall|protocol.FlagCache, 1)
	id, _ := req.ReadInt32()
	typ, _ := req.ReadInt8()
	count, _ := req.ReadInt32()
	tm, _ := req.ReadInt32()
	ds, _ := req.ReadInt16()
	value, _ := req.ReadFloat64()
	tail, _ := req.ReadInt32()
	if id != 1001 || model.PointType(typ) != model.TypeR8 || count != 1 || tm != 1767323045 || model.DSFromInt16(ds) != model.DSBadQuality || value != 22.5 || tail != protocol.Magic {
		t.Errorf("unexpected archive write id=%d type=%d count=%d tm=%d ds=%d value=%f tail=%x", id, typ, count, tm, ds, value, tail)
		return
	}
	writeNativeEcho(t, conn, 0)
}

func serveArchiveDelete(t *testing.T, conn net.Conn) {
	defer conn.Close()
	_, reader := handshakeNativeServer(t, conn)
	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read archive delete: %v", err)
		return
	}
	req := codec.NewReader(bytes.NewReader(payload))
	assertNativeWriteHeader(t, req, protocol.CommandDelete, protocol.URLArchive, protocol.FlagWall, 1)
	id, _ := req.ReadInt32()
	begin, _ := req.ReadInt32()
	end, _ := req.ReadInt32()
	tail, _ := req.ReadInt32()
	if id != 1001 || begin != 1767323045 || end != 1767326645 || tail != protocol.Magic {
		t.Errorf("unexpected archive delete id=%d begin=%d end=%d tail=%x", id, begin, end, tail)
		return
	}
	writeNativeEcho(t, conn, 0)
}

func serveTableMutationEmptyResponse(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer, reader := handshakeNativeServer(t, conn)
	if _, err := reader.ReadMessage(); err != nil {
		t.Errorf("read table mutation: %v", err)
		return
	}
	if err := writer.WriteMessage(nil); err != nil && err != io.ErrClosedPipe {
		t.Errorf("write empty mutation response: %v", err)
	}
}

func serveTableMutation(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer, reader := handshakeNativeServer(t, conn)
	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read table mutation: %v", err)
		return
	}
	msg := bytes.NewReader(payload)
	value, err := codec.NewDecoder(msg).DecodeValue()
	if err != nil {
		t.Errorf("decode mutation props: %v", err)
		return
	}
	props, ok := value.(map[string]any)
	if !ok {
		t.Errorf("props are %T", value)
		return
	}
	if props[protocol.PropAction] != protocol.ActionUpdate || props[protocol.PropTable] != "Point" || props[protocol.PropDB] != "W3" {
		t.Errorf("unexpected mutation props: %#v", props)
		return
	}
	filters, ok := props[protocol.PropFilters].([]any)
	if !ok || len(filters) != 1 {
		t.Errorf("filters=%#v", props[protocol.PropFilters])
		return
	}
	columns, err := codec.DecodeColumns(props[protocol.PropColumns])
	if err != nil {
		t.Errorf("decode columns: %v", err)
		return
	}
	body, err := io.ReadAll(msg)
	if err != nil {
		t.Errorf("read mutation body: %v", err)
		return
	}
	rows, err := codec.DecodeDataSet(body, columns)
	if err != nil {
		t.Errorf("decode mutation rows: %v", err)
		return
	}
	if len(rows) != 1 || rows[0]["ED"] != "phase 5 test" || rows[0]["FQ"] != int16(2) {
		t.Errorf("rows=%#v", rows)
		return
	}
	var response bytes.Buffer
	if err := codec.NewEncoder(&response).EncodeMap(map[string]any{protocol.PropErrNo: int32(0)}); err != nil {
		t.Errorf("encode mutation response: %v", err)
		return
	}
	if err := writer.WriteMessage(response.Bytes()); err != nil && err != io.ErrClosedPipe {
		t.Errorf("write mutation response: %v", err)
	}
}

func assertNativeWriteHeader(t *testing.T, req *codec.Reader, command protocol.Command, url protocol.URL, flag int16, count int32) {
	t.Helper()
	magic, err := req.ReadInt32()
	if err != nil {
		t.Fatalf("read magic: %v", err)
	}
	gotCommand, _ := req.ReadInt32()
	gotURL, _ := req.ReadInt32()
	reserved, _ := req.ReadInt16()
	gotFlag, _ := req.ReadInt16()
	gotCount, _ := req.ReadInt32()
	if magic != protocol.Magic || gotCommand != int32(command) || gotURL != int32(url) || reserved != 0 || gotFlag != flag || gotCount != count {
		t.Fatalf("header magic=%x command=%d url=%x reserved=%d flag=%x count=%d", magic, gotCommand, gotURL, reserved, gotFlag, gotCount)
	}
}

func writeNativeEcho(t *testing.T, conn net.Conn, code int8) {
	t.Helper()
	if _, err := conn.Write([]byte{byte(code)}); err != nil && err != io.ErrClosedPipe {
		t.Errorf("write native echo: %v", err)
	}
}
