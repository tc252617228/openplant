package transport

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/operror"
)

func TestDialLoginWithPipeServer(t *testing.T) {
	cfg, dials := pipeConfig(t)
	conn, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()
	if *dials != 1 {
		t.Fatalf("dials=%d want 1", *dials)
	}
	if conn.ClientAddress() != "10.1.2.3" {
		t.Fatalf("client address=%q", conn.ClientAddress())
	}
	if conn.Version() != "5.0.4" {
		t.Fatalf("version=%q", conn.Version())
	}
}

func TestConnRequestEchoReadsRawByte(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	conn := NewConnForTest(client)
	defer conn.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		reader := codec.NewFrameReader(server)
		payload, err := reader.ReadMessage()
		if err != nil {
			t.Errorf("server read request: %v", err)
			return
		}
		if string(payload) != "native-write" {
			t.Errorf("payload=%q", payload)
			return
		}
		if _, err := server.Write([]byte{0}); err != nil && err != io.ErrClosedPipe {
			t.Errorf("server write echo: %v", err)
		}
	}()

	echo, err := conn.RequestEcho(context.Background(), []byte("native-write"))
	if err != nil {
		t.Fatalf("RequestEcho failed: %v", err)
	}
	if echo != 0 {
		t.Fatalf("echo=%d want 0", echo)
	}
	<-done
}

func TestPoolReusesReleasedConnection(t *testing.T) {
	cfg, dials := pipeConfig(t)
	pool := NewPool(cfg)
	defer pool.Close()

	conn1, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire 1 failed: %v", err)
	}
	pool.Release(conn1, nil)
	conn2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire 2 failed: %v", err)
	}
	if conn1 != conn2 {
		t.Fatalf("expected pool to reuse connection")
	}
	pool.Release(conn2, nil)
	if got := atomic.LoadInt32(dials); got != 1 {
		t.Fatalf("dials=%d want 1", got)
	}
}

func TestPoolDiscardDropsConnection(t *testing.T) {
	cfg, dials := pipeConfig(t)
	pool := NewPool(cfg)
	defer pool.Close()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	pool.Discard(conn)
	if stats := pool.Stats(); stats.Open != 0 {
		t.Fatalf("open=%d want 0", stats.Open)
	}
	if got := atomic.LoadInt32(dials); got != 1 {
		t.Fatalf("dials=%d want 1", got)
	}
}

func TestPoolAcquireHonorsContextWhenExhausted(t *testing.T) {
	cfg, _ := pipeConfig(t)
	cfg.PoolSize = 1
	cfg.MaxIdle = 1
	pool := NewPool(cfg)
	defer pool.Close()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	defer pool.Release(conn, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := pool.Acquire(ctx); err == nil || err != context.DeadlineExceeded {
		t.Fatalf("expected context deadline, got %v", err)
	}
}

func TestPoolAcquireUnblocksWhenPoolCloses(t *testing.T) {
	cfg, _ := pipeConfig(t)
	cfg.PoolSize = 1
	cfg.MaxIdle = 1
	pool := NewPool(cfg)

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := pool.Acquire(context.Background())
		errCh <- err
	}()

	if err := pool.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	select {
	case err := <-errCh:
		if err != operror.ErrClosed {
			t.Fatalf("expected closed error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Acquire did not unblock after Close")
	}
	pool.Release(conn, nil)
}

func TestPoolDropsExpiredIdleConnectionAndRedials(t *testing.T) {
	cfg, dials := pipeConfig(t)
	cfg.PoolSize = 1
	cfg.MaxIdle = 1
	cfg.IdleTimeout = time.Minute
	now := time.Unix(100, 0)
	pool := NewPool(cfg)
	pool.now = func() time.Time { return now }
	defer pool.Close()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire 1 failed: %v", err)
	}
	pool.Release(conn, nil)

	now = now.Add(time.Minute)
	conn2, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire 2 failed: %v", err)
	}
	pool.Release(conn2, nil)
	if got := atomic.LoadInt32(dials); got != 2 {
		t.Fatalf("dials=%d want 2", got)
	}
}

func TestPoolReleaseDropsErroredConnection(t *testing.T) {
	cfg, _ := pipeConfig(t)
	pool := NewPool(cfg)
	defer pool.Close()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	pool.Release(conn, operror.New(operror.KindTimeout, "test", "timeout"))
	if stats := pool.Stats(); stats.Open != 0 || stats.Idle != 0 {
		t.Fatalf("stats=%#v want no open or idle connections", stats)
	}
}

func pipeConfig(t testing.TB) (Config, *int32) {
	t.Helper()
	var dials int32
	cfg := Config{
		Host:           "pipe",
		Port:           1,
		User:           "test-user",
		Password:       "test-secret",
		DialTimeout:    time.Second,
		RequestTimeout: time.Second,
		PoolSize:       2,
		MaxIdle:        2,
		IdleTimeout:    time.Minute,
		MaxLifetime:    time.Minute,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			client, server := net.Pipe()
			atomic.AddInt32(&dials, 1)
			go serveLogin(t, server)
			return client, nil
		},
	}
	return cfg, &dials
}

func serveLogin(t testing.TB, conn net.Conn) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	challenge := make([]byte, protocol.ChallengeSize)
	copy(challenge, []byte("pipe server"))
	for i := 0; i < 20; i++ {
		challenge[64+i] = byte(i + 1)
	}
	codec.PutInt32(challenge[96:100], 0x00050004)
	if err := writer.WriteMessage(challenge); err != nil {
		t.Errorf("server challenge write failed: %v", err)
		return
	}
	reply := make([]byte, protocol.LoginReplySize)
	if _, err := io.ReadFull(reader, reply); err != nil {
		t.Errorf("server login reply read failed: %v", err)
		return
	}
	reader.ResetMessage()
	if got := string(reply[44 : 44+len("test-user")]); got != "test-user" {
		t.Errorf("server got user %q", got)
		return
	}
	response := make([]byte, protocol.LoginResponseSize)
	copy(response[4:8], []byte{10, 1, 2, 3})
	if err := writer.WriteMessage(response); err != nil {
		t.Errorf("server login response write failed: %v", err)
	}
}
