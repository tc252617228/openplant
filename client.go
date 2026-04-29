package openplant

import (
	"sync/atomic"

	"github.com/tc252617228/openplant/admin"
	"github.com/tc252617228/openplant/alarm"
	"github.com/tc252617228/openplant/archive"
	"github.com/tc252617228/openplant/internal/cache"
	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/transport"
	"github.com/tc252617228/openplant/metadata"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/realtime"
	"github.com/tc252617228/openplant/sql"
	"github.com/tc252617228/openplant/stat"
	"github.com/tc252617228/openplant/subscription"
	"github.com/tc252617228/openplant/system"
)

type Client struct {
	options    Options
	closed     atomic.Bool
	pool       *transport.Pool
	pointCache *cache.PointCache

	metadata     *metadata.Service
	realtime     *realtime.Service
	archive      *archive.Service
	stat         *stat.Service
	alarm        *alarm.Service
	admin        *admin.Service
	sql          *sql.Service
	subscription *subscription.Service
	system       *system.Service
}

func New(opts ...Option) (*Client, error) {
	cfg, err := newOptions(opts...)
	if err != nil {
		return nil, err
	}
	c := &Client{options: cfg}
	c.pool = transport.NewPool(transport.Config{
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
	})
	if !cfg.DisableMetadataCache {
		c.pointCache = cache.NewPointCacheWithLimit(cfg.MetadataCacheTTL, cfg.MetadataCacheMaxEntries)
	}
	c.sql = sql.NewService(sql.Options{
		ReadOnly:       cfg.ReadOnly,
		AllowUnsafeSQL: cfg.AllowUnsafeSQL,
		Executor:       clientSQLExecutor{client: c},
	})
	c.metadata = metadata.NewService(metadata.Options{Queryer: c.sql})
	c.realtime = realtime.NewService(realtime.Options{ReadOnly: cfg.ReadOnly, Queryer: c.sql, Requester: c, Reader: c, Writer: c})
	c.archive = archive.NewService(archive.Options{ReadOnly: cfg.ReadOnly, Queryer: c.sql, Requester: c, Native: c, Streamer: c, Writer: c})
	c.stat = stat.NewService(stat.Options{Queryer: c.sql, Requester: c, Native: c, Streamer: c})
	c.alarm = alarm.NewService(alarm.Options{Queryer: c.sql})
	c.admin = admin.NewService(admin.Options{ReadOnly: cfg.ReadOnly, Mutator: c})
	c.system = system.NewService(system.Options{Queryer: c.sql})
	idSource := &subscriptionIDSource{cfg: subscriptionTransportConfig(cfg)}
	c.subscription = subscription.NewService(subscription.Options{
		Source: idSource,
	})
	return c, nil
}

func (c *Client) Options() Options {
	if c == nil {
		return Options{}
	}
	return c.options.Redacted()
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.closed.Store(true)
	if c.pool != nil {
		return c.pool.Close()
	}
	return nil
}

func (c *Client) ensureOpen() error {
	if c == nil || c.closed.Load() {
		return operror.ErrClosed
	}
	return nil
}

func (c *Client) Metadata() *metadata.Service {
	if err := c.ensureOpen(); err != nil {
		return metadata.NewClosedService(err)
	}
	return c.metadata
}

func (c *Client) Realtime() *realtime.Service {
	if err := c.ensureOpen(); err != nil {
		return realtime.NewClosedService(err)
	}
	return c.realtime
}

func (c *Client) Archive() *archive.Service {
	if err := c.ensureOpen(); err != nil {
		return archive.NewClosedService(err)
	}
	return c.archive
}

func (c *Client) Stat() *stat.Service {
	if err := c.ensureOpen(); err != nil {
		return stat.NewClosedService(err)
	}
	return c.stat
}

func (c *Client) Alarm() *alarm.Service {
	if err := c.ensureOpen(); err != nil {
		return alarm.NewClosedService(err)
	}
	return c.alarm
}

func (c *Client) Admin() *admin.Service {
	if err := c.ensureOpen(); err != nil {
		return admin.NewClosedService(err)
	}
	return c.admin
}

func (c *Client) SQL() *sql.Service {
	if err := c.ensureOpen(); err != nil {
		return sql.NewClosedService(err)
	}
	return c.sql
}

func (c *Client) Subscription() *subscription.Service {
	if err := c.ensureOpen(); err != nil {
		return subscription.NewClosedService(err)
	}
	return c.subscription
}

func (c *Client) System() *system.Service {
	if err := c.ensureOpen(); err != nil {
		return system.NewClosedService(err)
	}
	return c.system
}

func (c *Client) RealtimeSubscriptionIDSource() subscription.IDSource {
	if err := c.ensureOpen(); err != nil {
		return closedSubscriptionIDSource{err: err}
	}
	return &subscriptionIDSource{cfg: subscriptionTransportConfig(c.options)}
}
