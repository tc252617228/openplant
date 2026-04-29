//go:build soak

package subscription

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/tc252617228/openplant/model"
)

func TestSoakSubscriptionDispatchAndClose(t *testing.T) {
	duration := requireSoak(t)
	svc := NewService(Options{
		EventBuffer: 256,
		Source: fakeSource{fn: func(ctx context.Context, req Request, emit func(Event) bool) error {
			ticker := time.NewTicker(100 * time.Microsecond)
			defer ticker.Stop()
			var i int64
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case tm := <-ticker.C:
					i++
					if !emit(Event{Kind: EventData, Sample: model.Sample{
						ID:     model.PointID(1000 + i%10),
						GN:     model.GN("W3.N.P1"),
						Time:   tm,
						Status: 0,
						Value:  model.R8(float64(i)),
					}}) {
						return ctx.Err()
					}
				}
			}
		}},
	})
	stream, err := svc.Subscribe(context.Background(), Request{
		DB:  "W3",
		IDs: []model.PointID{1001},
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()
	var count int64
	running := true
	for running {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				running = false
				break
			}
			if event.IsData() {
				count++
			}
		case <-timer.C:
			stream.Close()
			running = false
		}
	}
	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatalf("stream did not close after soak cancellation")
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream err=%v", err)
	}
	if count == 0 {
		t.Fatalf("soak did not receive any subscription events")
	}
}

func TestSoakTableSubscriptionDispatchAndClose(t *testing.T) {
	duration := requireSoak(t)
	svc := NewService(Options{
		EventBuffer: 256,
		Source: fakeTableSource{fn: func(ctx context.Context, req TableRequest, emit func(TableEvent) bool) error {
			ticker := time.NewTicker(100 * time.Microsecond)
			defer ticker.Stop()
			var i int64
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
					i++
					if !emit(TableEvent{Kind: EventData, Row: map[string]any{
						"ID": int32(1000 + i%10),
						"GN": "W3.N.P1",
						"PN": "P1",
						"RT": int8(model.TypeR8),
					}}) {
						return ctx.Err()
					}
				}
			}
		}},
	})
	stream, err := svc.SubscribeTable(context.Background(), TableRequest{
		DB:    "W3",
		Table: "Point",
		Key:   "ID",
		Int32: []int32{1001},
	})
	if err != nil {
		t.Fatalf("SubscribeTable failed: %v", err)
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()
	var count int64
	running := true
	for running {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				running = false
				break
			}
			if event.IsData() {
				count++
			}
		case <-timer.C:
			stream.Close()
			running = false
		}
	}
	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatalf("table stream did not close after soak cancellation")
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("table stream err=%v", err)
	}
	if count == 0 {
		t.Fatalf("soak did not receive any table subscription events")
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
