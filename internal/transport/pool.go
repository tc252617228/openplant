package transport

import (
	"context"
	"sync"
	"time"

	"github.com/tc252617228/openplant/operror"
)

type Pool struct {
	mu     sync.Mutex
	cfg    Config
	idle   chan *Conn
	open   int
	closed bool
	meta   map[*Conn]connMeta
	now    func() time.Time
}

type connMeta struct {
	createdAt time.Time
	lastUsed  time.Time
}

func NewPool(cfg Config) *Pool {
	cfg = cfg.withDefaults()
	return &Pool{
		cfg:  cfg,
		idle: make(chan *Conn, cfg.MaxIdle),
		meta: make(map[*Conn]connMeta),
		now:  time.Now,
	}
}

func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, operror.ErrClosed
		}
		select {
		case conn, ok := <-p.idle:
			p.mu.Unlock()
			if !ok {
				return nil, operror.ErrClosed
			}
			if p.expired(conn) {
				p.Discard(conn)
				continue
			}
			p.touch(conn)
			return conn, nil
		default:
		}
		if p.open < p.cfg.PoolSize {
			p.open++
			cfg := p.cfg
			p.mu.Unlock()
			conn, err := Dial(ctx, cfg)
			if err != nil {
				p.mu.Lock()
				p.open--
				p.mu.Unlock()
				return nil, err
			}
			p.track(conn)
			return conn, nil
		}
		idle := p.idle
		p.mu.Unlock()

		select {
		case conn, ok := <-idle:
			if !ok {
				return nil, operror.ErrClosed
			}
			if p.expired(conn) {
				p.Discard(conn)
				continue
			}
			p.touch(conn)
			return conn, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *Pool) Release(conn *Conn, err error) {
	if conn == nil {
		return
	}
	if ShouldDrop(err) {
		p.Discard(conn)
		return
	}
	if p.expired(conn) {
		p.Discard(conn)
		return
	}
	p.touch(conn)

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.Discard(conn)
		return
	}
	select {
	case p.idle <- conn:
		p.mu.Unlock()
	default:
		p.mu.Unlock()
		p.Discard(conn)
	}
}

func (p *Pool) Discard(conn *Conn) {
	if conn == nil {
		return
	}
	_ = conn.Close()
	p.mu.Lock()
	delete(p.meta, conn)
	if p.open > 0 {
		p.open--
	}
	p.mu.Unlock()
}

func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.idle)
	p.mu.Unlock()

	for conn := range p.idle {
		_ = conn.Close()
		p.mu.Lock()
		delete(p.meta, conn)
		if p.open > 0 {
			p.open--
		}
		p.mu.Unlock()
	}
	return nil
}

func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{
		Open:     p.open,
		Idle:     len(p.idle),
		Capacity: p.cfg.PoolSize,
		Closed:   p.closed,
	}
}

type Stats struct {
	Open     int
	Idle     int
	Capacity int
	Closed   bool
}

func (p *Pool) track(conn *Conn) {
	now := p.now()
	p.mu.Lock()
	p.meta[conn] = connMeta{createdAt: now, lastUsed: now}
	p.mu.Unlock()
}

func (p *Pool) touch(conn *Conn) {
	now := p.now()
	p.mu.Lock()
	meta := p.meta[conn]
	if meta.createdAt.IsZero() {
		meta.createdAt = now
	}
	meta.lastUsed = now
	p.meta[conn] = meta
	p.mu.Unlock()
}

func (p *Pool) expired(conn *Conn) bool {
	now := p.now()
	p.mu.Lock()
	meta := p.meta[conn]
	p.mu.Unlock()
	if meta.createdAt.IsZero() {
		return false
	}
	if p.cfg.MaxLifetime > 0 && now.Sub(meta.createdAt) >= p.cfg.MaxLifetime {
		return true
	}
	if p.cfg.IdleTimeout > 0 && now.Sub(meta.lastUsed) >= p.cfg.IdleTimeout {
		return true
	}
	return false
}
