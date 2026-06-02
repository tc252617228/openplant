package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
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

func TestDialAppliesCompressionAfterLogin(t *testing.T) {
	var dials int32
	loginCompression := make(chan codec.CompressionMode, 1)
	requestCompression := make(chan codec.CompressionMode, 1)
	cfg := Config{
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
		Compression:    codec.CompressionFrame,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			client, server := net.Pipe()
			atomic.AddInt32(&dials, 1)
			go serveLoginThenRequest(t, server, loginCompression, requestCompression)
			return client, nil
		},
	}
	conn, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	payload := bytes.Repeat([]byte("business-payload;"), 4096)
	got, err := conn.Request(context.Background(), payload)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	if !bytes.Equal(got, []byte("ok")) {
		t.Fatalf("response=%q", got)
	}
	if got := <-loginCompression; got != codec.CompressionNone {
		t.Fatalf("login compression=%d want none", got)
	}
	if got := <-requestCompression; got != codec.CompressionFrame {
		t.Fatalf("request compression=%d want frame", got)
	}
}

func TestDialRejectsInvalidCompressionBeforeNetwork(t *testing.T) {
	var dials int32
	cfg := Config{
		Host:           "pipe",
		Port:           1,
		DialTimeout:    time.Second,
		RequestTimeout: time.Second,
		Compression:    codec.CompressionMode(99),
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			atomic.AddInt32(&dials, 1)
			client, server := net.Pipe()
			_ = server.Close()
			return client, nil
		},
	}
	_, err := Dial(context.Background(), cfg)
	if !operror.IsKind(err, operror.KindProtocol) {
		t.Fatalf("expected protocol error, got %v", err)
	}
	if !errors.Is(err, codec.ErrUnsupportedCompression) {
		t.Fatalf("expected unsupported compression sentinel, got %v", err)
	}
	if got := atomic.LoadInt32(&dials); got != 0 {
		t.Fatalf("dials=%d want 0", got)
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

func TestConnCompressionFailureIsProtocolError(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	conn := NewConnForTest(client)
	defer conn.Close()
	if err := conn.SetCompression(codec.CompressionFrame); err != nil {
		t.Fatalf("SetCompression failed: %v", err)
	}

	err := conn.WriteMessage(context.Background(), []byte("small"))
	if !operror.IsKind(err, operror.KindProtocol) {
		t.Fatalf("expected protocol error, got %v", err)
	}
	if !errors.Is(err, codec.ErrCompressionFailed) {
		t.Fatalf("expected compression sentinel, got %v", err)
	}
}

func TestClassifyErrPreservesOpenPlantErrorKind(t *testing.T) {
	err := operror.Wrap(operror.KindDecode, "decode.fixture", io.EOF)
	got := classifyErr("transport.fixture", err)
	if !operror.IsKind(got, operror.KindDecode) {
		t.Fatalf("kind=%v want decode; err=%v", got, got)
	}
}

func TestBindContextCleanupClearsDeadlineAfterCancel(t *testing.T) {
	raw := &deadlineRecorderConn{}
	conn := NewConnForTest(raw)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cleanup := conn.bindContextWithCleanup(ctx)
	cancel()
	if !raw.waitForNonZeroDeadline(time.Second) {
		t.Fatalf("context cancellation did not set an immediate deadline")
	}
	cleanup()
	if got := raw.lastDeadline(); !got.IsZero() {
		t.Fatalf("deadline after cleanup=%v want zero", got)
	}
}

func TestBindContextCleanupSuppressesLateCancelDeadline(t *testing.T) {
	raw := &deadlineRecorderConn{}
	conn := NewConnForTest(raw)
	defer conn.Close()

	for i := 0; i < 1000; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cleanup := conn.bindContextWithCleanup(ctx)
		cleanup()
		cancel()
	}
	time.Sleep(20 * time.Millisecond)
	if got := raw.lastDeadline(); !got.IsZero() {
		t.Fatalf("late context cancellation set deadline=%v after cleanup", got)
	}
}

func serveLoginThenRequest(t testing.TB, conn net.Conn, loginCompression, requestCompression chan<- codec.CompressionMode) {
	defer conn.Close()
	writer := codec.NewFrameWriter(conn, codec.CompressionNone)
	reader := codec.NewFrameReader(conn)
	writeLoginChallengeForTest(t, writer)
	reply := make([]byte, protocol.LoginReplySize)
	if _, err := io.ReadFull(reader, reply); err != nil {
		t.Errorf("server login reply read failed: %v", err)
		return
	}
	loginCompression <- reader.LastCompression()
	reader.ResetMessage()
	if got := string(reply[44 : 44+len("test-user")]); got != "test-user" {
		t.Errorf("server got user %q", got)
		return
	}
	writeLoginOKForTest(t, writer)
	payload, err := reader.ReadMessage()
	if err != nil {
		t.Errorf("server read business request failed: %v", err)
		return
	}
	if len(payload) == 0 {
		t.Errorf("empty business payload")
		return
	}
	requestCompression <- reader.LastCompression()
	if err := writer.WriteMessage([]byte("ok")); err != nil {
		t.Errorf("server response write failed: %v", err)
	}
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

func TestPoolAcquireDuringCloseDoesNotReturnNewConnection(t *testing.T) {
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	var dials int32
	cfg := Config{
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
			atomic.AddInt32(&dials, 1)
			close(dialStarted)
			select {
			case <-releaseDial:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			client, server := net.Pipe()
			go serveLogin(t, server)
			return client, nil
		},
	}
	pool := NewPool(cfg)
	connCh := make(chan *Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := pool.Acquire(context.Background())
		connCh <- conn
		errCh <- err
	}()

	<-dialStarted
	if err := pool.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	close(releaseDial)

	select {
	case err := <-errCh:
		if err != operror.ErrClosed {
			t.Fatalf("expected closed error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Acquire did not finish after dial was released")
	}
	if conn := <-connCh; conn != nil {
		t.Fatalf("Acquire returned a connection after Close")
	}
	if stats := pool.Stats(); stats.Open != 0 || stats.Idle != 0 {
		t.Fatalf("stats=%#v want no open or idle connections", stats)
	}
	if got := atomic.LoadInt32(&dials); got != 1 {
		t.Fatalf("dials=%d want 1", got)
	}
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
	writeLoginChallengeForTest(t, writer)
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
	writeLoginOKForTest(t, writer)
}

func writeLoginChallengeForTest(t testing.TB, writer *codec.FrameWriter) {
	t.Helper()
	challenge := make([]byte, protocol.ChallengeSize)
	copy(challenge, []byte("pipe server"))
	for i := 0; i < 20; i++ {
		challenge[64+i] = byte(i + 1)
	}
	codec.PutInt32(challenge[96:100], 0x00050004)
	if err := writer.WriteMessage(challenge); err != nil {
		t.Errorf("server challenge write failed: %v", err)
	}
}

func writeLoginOKForTest(t testing.TB, writer *codec.FrameWriter) {
	t.Helper()
	response := make([]byte, protocol.LoginResponseSize)
	copy(response[4:8], []byte{10, 1, 2, 3})
	if err := writer.WriteMessage(response); err != nil {
		t.Errorf("server login response write failed: %v", err)
	}
}

type deadlineRecorderConn struct {
	mu       sync.Mutex
	deadline time.Time
	changed  chan struct{}
}

func (c *deadlineRecorderConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (c *deadlineRecorderConn) Write(p []byte) (int, error) {
	return len(p), nil
}

func (c *deadlineRecorderConn) Close() error {
	return nil
}

func (c *deadlineRecorderConn) LocalAddr() net.Addr {
	return dummyAddr("local")
}

func (c *deadlineRecorderConn) RemoteAddr() net.Addr {
	return dummyAddr("remote")
}

func (c *deadlineRecorderConn) SetDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.deadline = deadline
	if c.changed == nil {
		c.changed = make(chan struct{})
	}
	changed := c.changed
	c.changed = make(chan struct{})
	c.mu.Unlock()
	close(changed)
	return nil
}

func (c *deadlineRecorderConn) SetReadDeadline(deadline time.Time) error {
	return c.SetDeadline(deadline)
}

func (c *deadlineRecorderConn) SetWriteDeadline(deadline time.Time) error {
	return c.SetDeadline(deadline)
}

func (c *deadlineRecorderConn) lastDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadline
}

func (c *deadlineRecorderConn) waitForNonZeroDeadline(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		c.mu.Lock()
		if !c.deadline.IsZero() {
			c.mu.Unlock()
			return true
		}
		if c.changed == nil {
			c.changed = make(chan struct{})
		}
		changed := c.changed
		c.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		select {
		case <-changed:
		case <-time.After(remaining):
			return false
		}
	}
}

type dummyAddr string

func (a dummyAddr) Network() string {
	return string(a)
}

func (a dummyAddr) String() string {
	return string(a)
}
