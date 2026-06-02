package openplant

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/transport"
	"github.com/tc252617228/openplant/operror"
)

// Conn is a narrow low-level connection for protocol checks and diagnostics.
// Application code should prefer Client and the typed service facades.
type Conn interface {
	Ping(ctx context.Context) error
	Close() error
	// CompressionMode returns the outbound business-frame compression configured
	// after login. Server response frames carry their own wire compression flag
	// and are decoded independently.
	CompressionMode() CompressionMode
}

type conn struct {
	raw *transport.Conn
}

var _ Conn = (*conn)(nil)

// Dial opens one low-level OpenPlant TCP connection.
//
// The optional addr overrides opts.Host and opts.Port when it is not empty. It
// must be formatted as host:port.
func Dial(ctx context.Context, addr string, opts *Options) (Conn, error) {
	cfg := DefaultOptions()
	if opts != nil {
		cfg = *opts
	}
	if err := applyDialAddress(&cfg, addr); err != nil {
		return nil, err
	}
	return dialConn(ctx, cfg)
}

// DialWithOptions opens one low-level OpenPlant TCP connection after applying
// option functions to the supplied base options.
func DialWithOptions(ctx context.Context, opts *Options, options ...Option) (Conn, error) {
	cfg := DefaultOptions()
	if opts != nil {
		cfg = *opts
	}
	for _, opt := range options {
		if opt != nil {
			opt(&cfg)
		}
	}
	return dialConn(ctx, cfg)
}

func (c *conn) Ping(ctx context.Context) error {
	if c == nil || c.raw == nil {
		return operror.ErrClosed
	}
	return c.raw.Alive(ctx)
}

func (c *conn) Close() error {
	if c == nil || c.raw == nil {
		return nil
	}
	return c.raw.Close()
}

func (c *conn) CompressionMode() CompressionMode {
	if c == nil || c.raw == nil {
		return CompressionNone
	}
	return CompressionMode(c.raw.CompressionMode())
}

func dialConn(ctx context.Context, cfg Options) (Conn, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	raw, err := transport.Dial(ctx, transportConfigFromOptions(cfg))
	if err != nil {
		return nil, err
	}
	return &conn{raw: raw}, nil
}

func applyDialAddress(cfg *Options, addr string) error {
	if addr == "" {
		return nil
	}
	host, portRaw, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%w: dial address must be host:port", operror.ErrInvalidOption)
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil {
		return fmt.Errorf("%w: dial address port must be numeric", operror.ErrInvalidOption)
	}
	cfg.Host = host
	cfg.Port = port
	return nil
}

func transportConfigFromOptions(cfg Options) transport.Config {
	return transport.Config{
		Host:           cfg.Host,
		Port:           cfg.Port,
		User:           cfg.User,
		Password:       cfg.Password,
		DialTimeout:    cfg.DialTimeout,
		RequestTimeout: cfg.RequestTimeout,
		PoolSize:       cfg.PoolSize,
		MaxIdle:        cfg.MaxIdle,
		IdleTimeout:    cfg.IdleTimeout,
		MaxLifetime:    cfg.MaxLifetime,
		Compression:    codec.CompressionMode(cfg.Compression),
	}
}
