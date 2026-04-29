//go:build safe_readonly && soak

package tests

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	openplant "github.com/tc252617228/openplant"
	"github.com/tc252617228/openplant/internal/testenv"
)

func TestSafeReadonlySoakRealtimeArchiveStat(t *testing.T) {
	cfg := testenv.RequireSafeReadonly(t)
	duration := requireSoak(t)
	client := newSafeReadonlyClient(t, cfg)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), duration+30*time.Second)
	defer cancel()
	db := resolveSafeReadonlyDB(t, ctx, client, cfg)
	point := resolveSafeReadonlyPoint(t, ctx, client, cfg, db)
	timeRange := openplant.TimeRange{Begin: time.Now().Add(-1 * time.Hour), End: time.Now()}

	deadline := time.Now().Add(duration)
	iterations := 0
	for time.Now().Before(deadline) {
		opCtx, opCancel := context.WithTimeout(ctx, 15*time.Second)
		if samples, err := client.Realtime().Read(opCtx, openplant.RealtimeReadRequest{
			DB:  db,
			IDs: []openplant.PointID{point.ID},
		}); err != nil {
			opCancel()
			t.Fatalf("Realtime.Read failed during soak: %v", err)
		} else if len(samples) == 0 {
			opCancel()
			t.Fatalf("Realtime.Read returned no samples during soak")
		}
		if _, err := client.Archive().QueryRequest(opCtx, openplant.ArchiveQuery{
			DB:      db,
			IDs:     []openplant.PointID{point.ID},
			Range:   timeRange,
			Mode:    openplant.ModeRaw,
			Quality: openplant.QualityNone,
			Limit:   10,
		}); err != nil {
			opCancel()
			t.Fatalf("Archive.QueryRequest failed during soak: %v", err)
		}
		if _, err := client.Stat().QuerySQL(opCtx, openplant.StatQuery{
			DB:       db,
			IDs:      []openplant.PointID{point.ID},
			Range:    timeRange,
			Mode:     openplant.ModeAvg,
			Interval: "1m",
			Quality:  openplant.QualityNone,
			Limit:    10,
		}); err != nil {
			opCancel()
			t.Fatalf("Stat.QuerySQL failed during soak: %v", err)
		}
		opCancel()
		iterations++
	}
	if iterations == 0 {
		t.Fatalf("safe-readonly soak completed no iterations")
	}
	t.Logf("safe-readonly soak iterations=%d db=%s point_id=%d", iterations, db, point.ID)
}

func TestSafeReadonlySoakSubscriptionClose(t *testing.T) {
	cfg := testenv.RequireSafeReadonly(t)
	duration := requireSoak(t)
	client := newSafeReadonlyClient(t, cfg)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), duration+30*time.Second)
	defer cancel()
	db := resolveSafeReadonlyDB(t, ctx, client, cfg)
	point := resolveSafeReadonlyPoint(t, ctx, client, cfg, db)

	stream, err := client.Subscription().Subscribe(ctx, openplant.SubscribeRequest{
		DB:  db,
		IDs: []openplant.PointID{point.ID},
	})
	if err != nil {
		t.Fatalf("Subscription.Subscribe failed during soak: %v", err)
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				if err := stream.Err(); err != nil {
					t.Fatalf("subscription closed during soak: %v", err)
				}
				return
			}
			if event.IsError() {
				t.Fatalf("subscription error during soak: %v", event.Err)
			}
		case <-timer.C:
			stream.Close()
			select {
			case <-stream.Done():
			case <-time.After(5 * time.Second):
				t.Fatalf("subscription did not close after soak")
			}
			if err := stream.Err(); err != nil {
				t.Fatalf("subscription close returned error after soak: %v", err)
			}
			return
		}
	}
}

func requireSoak(t testing.TB) time.Duration {
	t.Helper()
	if os.Getenv("OPENPLANT_TEST_SOAK") != "1" {
		t.Skip("set OPENPLANT_TEST_SOAK=1 to run soak tests")
	}
	duration := 5 * time.Second
	if raw := os.Getenv("OPENPLANT_TEST_SOAK_DURATION_MS"); raw != "" {
		ms, err := strconv.Atoi(raw)
		if err != nil || ms <= 0 {
			t.Fatalf("OPENPLANT_TEST_SOAK_DURATION_MS must be a positive integer")
		}
		duration = time.Duration(ms) * time.Millisecond
	}
	return duration
}
