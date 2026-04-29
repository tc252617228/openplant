package transport

import (
	"context"
	"testing"
)

func BenchmarkPoolAcquireReleaseIdle(b *testing.B) {
	cfg, _ := pipeConfig(b)
	cfg.PoolSize = 1
	cfg.MaxIdle = 1
	pool := NewPool(cfg)
	defer pool.Close()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	pool.Release(conn, nil)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			b.Fatal(err)
		}
		pool.Release(conn, nil)
	}
}
