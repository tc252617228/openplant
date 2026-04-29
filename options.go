package openplant

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/tc252617228/openplant/operror"
)

type CompressionMode int

const (
	CompressionNone CompressionMode = iota
	CompressionFrame
	CompressionBlock
)

type Options struct {
	Host                    string
	Port                    int
	User                    string
	Password                string
	DialTimeout             time.Duration
	RequestTimeout          time.Duration
	PoolSize                int
	MaxIdle                 int
	IdleTimeout             time.Duration
	MaxLifetime             time.Duration
	Compression             CompressionMode
	ReadOnly                bool
	AllowUnsafeSQL          bool
	ChunkSize               int
	MetadataCacheTTL        time.Duration
	MetadataCacheMaxEntries int
	DisableMetadataCache    bool
}

type Option func(*Options)

func DefaultOptions() Options {
	return Options{
		Host:                    "127.0.0.1",
		Port:                    8200,
		DialTimeout:             10 * time.Second,
		RequestTimeout:          30 * time.Second,
		PoolSize:                4,
		MaxIdle:                 4,
		IdleTimeout:             5 * time.Minute,
		MaxLifetime:             30 * time.Minute,
		Compression:             CompressionNone,
		ReadOnly:                true,
		ChunkSize:               200,
		MetadataCacheTTL:        5 * time.Minute,
		MetadataCacheMaxEntries: 10000,
	}
}

func newOptions(opts ...Option) (Options, error) {
	cfg := DefaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg, cfg.Validate()
}

func (o Options) Validate() error {
	if o.Host == "" {
		return fmt.Errorf("%w: host is required", operror.ErrInvalidOption)
	}
	if o.Port <= 0 || o.Port > 65535 {
		return fmt.Errorf("%w: port must be between 1 and 65535", operror.ErrInvalidOption)
	}
	if o.DialTimeout <= 0 {
		return fmt.Errorf("%w: dial timeout must be positive", operror.ErrInvalidOption)
	}
	if o.RequestTimeout <= 0 {
		return fmt.Errorf("%w: request timeout must be positive", operror.ErrInvalidOption)
	}
	if o.PoolSize <= 0 {
		return fmt.Errorf("%w: pool size must be positive", operror.ErrInvalidOption)
	}
	if o.MaxIdle <= 0 || o.MaxIdle > o.PoolSize {
		return fmt.Errorf("%w: max idle must be in range 1..pool size", operror.ErrInvalidOption)
	}
	if o.IdleTimeout <= 0 {
		return fmt.Errorf("%w: idle timeout must be positive", operror.ErrInvalidOption)
	}
	if o.MaxLifetime <= 0 {
		return fmt.Errorf("%w: max lifetime must be positive", operror.ErrInvalidOption)
	}
	if o.ChunkSize <= 0 {
		return fmt.Errorf("%w: chunk size must be positive", operror.ErrInvalidOption)
	}
	if o.MetadataCacheTTL < 0 {
		return fmt.Errorf("%w: metadata cache TTL cannot be negative", operror.ErrInvalidOption)
	}
	if !o.DisableMetadataCache && o.MetadataCacheTTL == 0 {
		return fmt.Errorf("%w: metadata cache TTL must be positive or cache must be disabled", operror.ErrInvalidOption)
	}
	if !o.DisableMetadataCache && o.MetadataCacheMaxEntries <= 0 {
		return fmt.Errorf("%w: metadata cache max entries must be positive or cache must be disabled", operror.ErrInvalidOption)
	}
	return nil
}

func (o Options) Redacted() Options {
	o.Password = ""
	return o
}

func WithEndpoint(host string, port int) Option {
	return func(o *Options) {
		o.Host = host
		o.Port = port
	}
}

func WithCredentials(user, password string) Option {
	return func(o *Options) {
		o.User = user
		o.Password = password
	}
}

func WithTimeouts(dial, request time.Duration) Option {
	return func(o *Options) {
		o.DialTimeout = dial
		o.RequestTimeout = request
	}
}

func WithPool(size, maxIdle int, idleTimeout, maxLifetime time.Duration) Option {
	return func(o *Options) {
		o.PoolSize = size
		o.MaxIdle = maxIdle
		o.IdleTimeout = idleTimeout
		o.MaxLifetime = maxLifetime
	}
}

func WithReadOnly(readonly bool) Option {
	return func(o *Options) {
		o.ReadOnly = readonly
	}
}

func WithUnsafeSQL(allow bool) Option {
	return func(o *Options) {
		o.AllowUnsafeSQL = allow
	}
}

func WithChunkSize(size int) Option {
	return func(o *Options) {
		o.ChunkSize = size
	}
}

func WithMetadataCacheTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.MetadataCacheTTL = ttl
	}
}

func WithMetadataCacheMaxEntries(maxEntries int) Option {
	return func(o *Options) {
		o.MetadataCacheMaxEntries = maxEntries
	}
}

func WithMetadataCacheDisabled(disabled bool) Option {
	return func(o *Options) {
		o.DisableMetadataCache = disabled
	}
}

func WithCompression(mode CompressionMode) Option {
	return func(o *Options) {
		o.Compression = mode
	}
}

func OptionsFromEnv(prefix string) []Option {
	if prefix == "" {
		prefix = "OPENPLANT_TEST"
	}
	host := os.Getenv(prefix + "_HOST")
	portRaw := os.Getenv(prefix + "_PORT")
	user := os.Getenv(prefix + "_USER")
	pass := os.Getenv(prefix + "_PASS")

	var opts []Option
	if host != "" || portRaw != "" {
		port := 8200
		if portRaw != "" {
			if parsed, err := strconv.Atoi(portRaw); err == nil {
				port = parsed
			}
		}
		opts = append(opts, WithEndpoint(host, port))
	}
	if user != "" || pass != "" {
		opts = append(opts, WithCredentials(user, pass))
	}
	if os.Getenv(prefix+"_READONLY") == "0" {
		opts = append(opts, WithReadOnly(false))
	}
	return opts
}
