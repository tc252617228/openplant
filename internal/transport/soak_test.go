//go:build soak

package transport

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSoakPoolConcurrentAcquireRelease(t *testing.T) {
	duration := requireSoak(t)
	cfg, _ := pipeConfig(t)
	cfg.PoolSize = 4
	cfg.MaxIdle = 4
	pool := NewPool(cfg)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	var ops atomic.Int64
	var failures atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				conn, err := pool.Acquire(ctx)
				if err != nil {
					if ctx.Err() == nil {
						failures.Add(1)
					}
					return
				}
				pool.Release(conn, nil)
				ops.Add(1)
			}
		}()
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("pool soak failures=%d", failures.Load())
	}
	if ops.Load() == 0 {
		t.Fatalf("pool soak did not acquire any connections")
	}
	stats := pool.Stats()
	if stats.Open > stats.Capacity {
		t.Fatalf("pool open=%d capacity=%d", stats.Open, stats.Capacity)
	}
}

func requireSoak(t *testing.T) time.Duration {
	t.Helper()
	if os.Getenv("OPENPLANT_TEST_SOAK") != "1" {
		t.Skip("set OPENPLANT_TEST_SOAK=1 to run soak tests")
	}
	duration := 3 * time.Second
	if raw := os.Getenv("OPENPLANT_TEST_SOAK_DURATION_MS"); raw != "" {
		ms, err := strconv.Atoi(raw)
		if err != nil || ms <= 0 {
			t.Fatalf("OPENPLANT_TEST_SOAK_DURATION_MS must be a positive integer")
		}
		duration = time.Duration(ms) * time.Millisecond
	}
	return duration
}
