package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/operror"
)

type Conn struct {
	mu       sync.Mutex
	readMu   sync.Mutex
	writeMu  sync.Mutex
	netConn  net.Conn
	reader   *codec.FrameReader
	writer   *codec.FrameWriter
	info     string
	version  int32
	clientIP string
	closed   bool
}

func Dial(ctx context.Context, cfg Config) (*Conn, error) {
	cfg = cfg.withDefaults()
	ctx, cancel := protocol.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()

	raw, err := cfg.DialContext(ctx, "tcp", cfg.Address())
	if err != nil {
		return nil, classifyErr("transport.Dial", err)
	}
	conn := &Conn{
		netConn: raw,
		reader:  codec.NewFrameReader(raw),
		writer:  codec.NewFrameWriter(raw, cfg.Compression),
	}
	cleanup := conn.bindContextWithCleanup(ctx)
	defer cleanup()
	loginCtx, loginCancel := protocol.WithTimeout(ctx, cfg.RequestTimeout)
	defer loginCancel()
	if err := conn.login(loginCtx, cfg.User, cfg.Password); err != nil {
		_ = raw.Close()
		return nil, err
	}
	_ = raw.SetDeadline(time.Time{})
	return conn, nil
}

func NewConnForTest(raw net.Conn) *Conn {
	return &Conn{
		netConn: raw,
		reader:  codec.NewFrameReader(raw),
		writer:  codec.NewFrameWriter(raw, codec.CompressionNone),
	}
}

func (c *Conn) login(ctx context.Context, user, password string) error {
	challenge, err := protocol.ReadChallenge(ctx, c.reader)
	if err != nil {
		return err
	}
	if err := protocol.WriteLoginReply(ctx, c.writer, user, password, challenge.Random[:]); err != nil {
		return err
	}
	c.reader.ResetMessage()
	result, err := protocol.ReadLoginResult(ctx, c.reader, challenge)
	if err != nil {
		return err
	}
	c.info = result.Info
	c.version = result.Version
	c.clientIP = result.ClientAddress
	c.reader.ResetMessage()
	return nil
}

func (c *Conn) Info() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.info
}

func (c *Conn) VersionNumber() int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.version
}

func (c *Conn) Version() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return protocol.VersionString(c.version)
}

func (c *Conn) ClientAddress() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientIP
}

func (c *Conn) SetCompression(mode codec.CompressionMode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writer.SetCompression(mode)
}

func (c *Conn) Request(ctx context.Context, payload []byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.WriteMessage(ctx, payload); err != nil {
		return nil, err
	}
	msg, err := c.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (c *Conn) RequestEcho(ctx context.Context, payload []byte) (int8, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.WriteMessage(ctx, payload); err != nil {
		return 0, err
	}
	echo, err := c.ReadEcho(ctx)
	if err != nil {
		return 0, err
	}
	return echo, nil
}

func (c *Conn) RequestStream(ctx context.Context, payload []byte, fn func(io.Reader) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if fn == nil {
		return operror.New(operror.KindValidation, "transport.Conn.RequestStream", "stream callback is required")
	}
	if err := c.WriteMessage(ctx, payload); err != nil {
		return err
	}
	if err := c.ensureOpen(); err != nil {
		return err
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	cleanup := c.bindContextWithCleanup(ctx)
	defer cleanup()
	c.reader.ResetMessage()
	err := fn(c.reader)
	c.reader.ResetMessage()
	if err != nil {
		return classifyErr("transport.Conn.RequestStream", err)
	}
	return nil
}

func (c *Conn) ReadEcho(ctx context.Context) (int8, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.ensureOpen(); err != nil {
		return 0, err
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	cleanup := c.bindContextWithCleanup(ctx)
	defer cleanup()
	c.reader.ResetMessage()
	var echo [1]byte
	if _, err := io.ReadFull(c.netConn, echo[:]); err != nil {
		return 0, classifyErr("transport.Conn.ReadEcho", err)
	}
	c.reader.ResetMessage()
	return int8(echo[0]), nil
}

func (c *Conn) WriteMessage(ctx context.Context, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.ensureOpen(); err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	cleanup := c.bindContextWithCleanup(ctx)
	defer cleanup()
	if err := c.writer.WriteMessage(payload); err != nil {
		return classifyErr("transport.Conn.WriteMessage", err)
	}
	return nil
}

func (c *Conn) ReadMessage(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	cleanup := c.bindContextWithCleanup(ctx)
	defer cleanup()
	c.reader.ResetMessage()
	msg, err := c.reader.ReadMessage()
	if err != nil {
		return nil, classifyErr("transport.Conn.ReadMessage", err)
	}
	c.reader.ResetMessage()
	return msg, nil
}

func (c *Conn) Alive(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return operror.ErrClosed
	}
	cleanup := c.bindContextWithCleanup(ctx)
	defer cleanup()
	probe := []byte{
		0x10, 0x20, 0x30, 0x40,
		0, 0, 0, 110,
		0x46, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0xA5,
		0x10, 0x20, 0x30, 0x40,
	}
	if _, err := c.netConn.Write(probe); err != nil {
		return classifyErr("transport.Conn.Alive.write", err)
	}
	var echo [1]byte
	if _, err := io.ReadFull(c.netConn, echo[:]); err != nil {
		return classifyErr("transport.Conn.Alive.read", err)
	}
	if echo[0] != 0xA5 {
		return operror.New(operror.KindProtocol, "transport.Conn.Alive", "unexpected echo byte")
	}
	return nil
}

func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.netConn.Close()
}

func (c *Conn) ensureOpen() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return operror.ErrClosed
	}
	return nil
}

func (c *Conn) bindContextWithCleanup(ctx context.Context) func() {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.netConn.SetDeadline(deadline)
	}
	var done chan struct{}
	if ctx.Done() != nil {
		done = make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				_ = c.netConn.SetDeadline(time.Now())
			case <-done:
			}
		}()
	}
	return func() {
		if done != nil {
			close(done)
		}
		_ = c.netConn.SetDeadline(time.Time{})
	}
}

func classifyErr(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return operror.Wrap(operror.KindCanceled, op, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return operror.Wrap(operror.KindTimeout, op, err)
	}
	if errors.Is(err, codec.ErrUnsupportedCompression) {
		return operror.Wrap(operror.KindProtocol, op, err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return operror.Wrap(operror.KindTimeout, op, err)
		}
		return operror.Wrap(operror.KindNetwork, op, err)
	}
	if errors.Is(err, io.EOF) {
		return operror.Wrap(operror.KindNetwork, op, err)
	}
	return err
}

func ShouldDrop(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, operror.ErrClosed) {
		return true
	}
	if operror.IsKind(err, operror.KindNetwork) ||
		operror.IsKind(err, operror.KindTimeout) ||
		operror.IsKind(err, operror.KindCanceled) ||
		operror.IsKind(err, operror.KindProtocol) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.EOF)
}
