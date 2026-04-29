package transport

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
)

type DialContextFunc func(ctx context.Context, network, address string) (net.Conn, error)

type Config struct {
	Host           string
	Port           int
	User           string
	Password       string
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	PoolSize       int
	MaxIdle        int
	IdleTimeout    time.Duration
	MaxLifetime    time.Duration
	Compression    codec.CompressionMode
	DialContext    DialContextFunc
}

func (c Config) Address() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

func (c Config) withDefaults() Config {
	if c.DialTimeout <= 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.PoolSize <= 0 {
		c.PoolSize = 4
	}
	if c.MaxIdle <= 0 || c.MaxIdle > c.PoolSize {
		c.MaxIdle = c.PoolSize
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 5 * time.Minute
	}
	if c.MaxLifetime <= 0 {
		c.MaxLifetime = 30 * time.Minute
	}
	if c.DialContext == nil {
		dialer := net.Dialer{Timeout: c.DialTimeout}
		c.DialContext = dialer.DialContext
	}
	return c
}
