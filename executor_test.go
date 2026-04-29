package openplant

import (
	"bytes"
	"context"
	"io"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/internal/transport"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/sql"
)

func TestSQLQueryUsesProtocolExecutor(t *testing.T) {
	client := newPipeSQLClient(t)
	defer client.Close()

	result, err := client.SQL().Query(context.Background(), `select ID,PN,AV from Point where ID=1`)
	if err != nil {
		t.Fatalf("SQL query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows=%d want 1", len(result.Rows))
	}
	row := result.Rows[0]
	if row["ID"] != int32(1) || row["PN"] != "P1" || row["AV"] != float64(12.5) {
		t.Fatalf("unexpected row: %#v", row)
	}
}

func TestClientDoesNotExposeRawSQLBypassMethods(t *testing.T) {
	clientType := reflect.TypeOf(&Client{})
	for _, method := range []string{"QuerySQL", "ExecSQL"} {
		if _, ok := clientType.MethodByName(method); ok {
			t.Fatalf("Client exposes %s and can bypass SQL service safety", method)
		}
	}
}

func TestSQLQueryTimeoutDropsConnection(t *testing.T) {
	requestRead := make(chan struct{})
	client := newPipeSQLTimeoutClient(t, requestRead)
	defer client.Close()

	_, err := client.SQL().Query(context.Background(), `select ID from Point where ID=1`)
	if err == nil || !operror.IsKind(err, operror.KindTimeout) {
		t.Fatalf("expected timeout error, got %v", err)
	}
	select {
	case <-requestRead:
	case <-time.After(time.Second):
		t.Fatalf("server did not receive SQL request")
	}
	if stats := client.pool.Stats(); stats.Open != 0 || stats.Idle != 0 {
		t.Fatalf("stats=%#v want timed out connection dropped", stats)
	}
}

func TestSQLQueryRedialsAfterTimeout(t *testing.T) {
	requestRead := make(chan struct{})
	var dials atomic.Int32
	client := newPipeSQLFlakyClient(t, requestRead, &dials)
	defer client.Close()

	_, err := client.SQL().Query(context.Background(), `select ID from Point where ID=1`)
	if err == nil || !operror.IsKind(err, operror.KindTimeout) {
		t.Fatalf("expected timeout error, got %v", err)
	}
	select {
	case <-requestRead:
	case <-time.After(time.Second):
		t.Fatalf("server did not receive first SQL request")
	}

	result, err := client.SQL().Query(context.Background(), `select ID,PN,AV from Point where ID=1`)
	if err != nil {
		t.Fatalf("second SQL query failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["ID"] != int32(1) {
		t.Fatalf("unexpected second result: %#v", result.Rows)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("dials=%d want 2", got)
	}
}

func newPipeSQLClient(t *testing.T) *Client {
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
			go serveSQL(t, server)
			return client, nil
		},
	})
	c.sql = sql.NewService(sql.Options{
		ReadOnly:       true,
		AllowUnsafeSQL: false,
		Executor:       clientSQLExecutor{client: c},
	})
	return c
}

func newPipeSQLTimeoutClient(t *testing.T, requestRead chan<- struct{}) *Client {
	t.Helper()
	cfg := DefaultOptions()
	cfg.RequestTimeout = 50 * time.Millisecond
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
			go serveSQLWithoutResponse(t, server, requestRead)
			return client, nil
		},
	})
	c.sql = sql.NewService(sql.Options{
		ReadOnly:       true,
		AllowUnsafeSQL: false,
		Executor:       clientSQLExecutor{client: c},
	})
	return c
}

func newPipeSQLFlakyClient(t *testing.T, requestRead chan<- struct{}, dials *atomic.Int32) *Client {
	t.Helper()
	cfg := DefaultOptions()
	cfg.RequestTimeout = 50 * time.Millisecond
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
			switch dials.Add(1) {
			case 1:
				go serveSQLWithoutResponse(t, server, requestRead)
			default:
				go serveSQL(t, server)
			}
			return client, nil
		},
	})
	c.sql = sql.NewService(sql.Options{
		ReadOnly:       true,
		AllowUnsafeSQL: false,
		Executor:       clientSQLExecutor{client: c},
	})
	return c
}

func serveSQL(t *testing.T, conn net.Conn) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)

	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("read request: %v", err)
		return
	}
	value, err := codec.NewDecoder(bytes.NewReader(payload)).DecodeValue()
	if err != nil {
		t.Errorf("decode request props: %v", err)
		return
	}
	props := value.(map[string]any)
	if props[protocol.PropAction] != protocol.ActionExecSQL {
		t.Errorf("action=%v", props[protocol.PropAction])
		return
	}
	if props[protocol.PropSQL] != `select ID,PN,AV from Point where ID=1` {
		t.Errorf("sql=%v", props[protocol.PropSQL])
		return
	}

	columns := []codec.Column{
		{Name: "ID", Type: codec.VtInt32},
		{Name: "PN", Type: codec.VtString},
		{Name: "AV", Type: codec.VtObject},
	}
	body, err := codec.EncodeDataSet(columns, []map[string]any{{
		"ID": int32(1),
		"PN": "P1",
		"AV": float64(12.5),
	}})
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
	if err := writer.WriteMessage(response.Bytes()); err != nil {
		t.Errorf("write response: %v", err)
	}
}

func serveSQLWithoutResponse(t *testing.T, conn net.Conn, requestRead chan<- struct{}) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallenge(t, writer)
	readLoginReply(t, reader)
	writeLoginOK(t, writer)
	if _, err := reader.ReadMessage(); err != nil {
		t.Errorf("read request: %v", err)
		return
	}
	close(requestRead)
	time.Sleep(200 * time.Millisecond)
}

func writeLoginChallenge(t *testing.T, writer *codec.FrameWriter) {
	t.Helper()
	challenge := make([]byte, protocol.ChallengeSize)
	copy(challenge, []byte("pipe sql server"))
	for i := 0; i < 20; i++ {
		challenge[64+i] = byte(i + 1)
	}
	codec.PutInt32(challenge[96:100], 0x00050004)
	if err := writer.WriteMessage(challenge); err != nil {
		t.Fatalf("write challenge: %v", err)
	}
}

func readLoginReply(t *testing.T, reader *codec.FrameReader) {
	t.Helper()
	reply := make([]byte, protocol.LoginReplySize)
	if _, err := io.ReadFull(reader, reply); err != nil {
		t.Fatalf("read login reply: %v", err)
	}
	reader.ResetMessage()
}

func writeLoginOK(t *testing.T, writer *codec.FrameWriter) {
	t.Helper()
	response := make([]byte, protocol.LoginResponseSize)
	copy(response[4:8], []byte{10, 1, 2, 3})
	if err := writer.WriteMessage(response); err != nil {
		t.Fatalf("write login ok: %v", err)
	}
}
