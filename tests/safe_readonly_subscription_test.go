//go:build safe_readonly

package tests

import (
	"context"
	"sort"
	"testing"
	"time"

	openplant "github.com/tc252617228/openplant"
	"github.com/tc252617228/openplant/internal/testenv"
)

func TestSafeReadonlyTableSubscriptionPayloads(t *testing.T) {
	cfg := testenv.RequireSafeReadonly(t)
	client := newSafeReadonlyClient(t, cfg)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := resolveSafeReadonlyDB(t, ctx, client, cfg)
	point := resolveSafeReadonlyPoint(t, ctx, client, cfg, db)

	t.Run("point_snapshot", func(t *testing.T) {
		stream, err := client.Subscription().SubscribeTable(ctx, openplant.TableSubscribeRequest{
			DB:       db,
			Table:    "Point",
			Columns:  []string{"ID", "GN", "PN", "RT", "ED"},
			Key:      "ID",
			Int32:    []int32{int32(point.ID)},
			Snapshot: true,
		})
		if err != nil {
			t.Fatalf("SubscribeTable Point failed: %v", err)
		}
		defer stream.Close()

		row := waitForTableSubscriptionRow(t, stream, 10*time.Second)
		if got := int32FromAny(row["ID"]); got != int32(point.ID) {
			t.Fatalf("Point subscription ID=%d want %d row=%#v", got, point.ID, row)
		}
		if got := stringFromAny(row["GN"]); got != string(point.GN) {
			t.Fatalf("Point subscription GN=%q want %q row=%#v", got, point.GN, row)
		}
		t.Logf("point subscription payload keys: %v", sortedRowKeys(row))
	})

	t.Run("alarm_snapshot", func(t *testing.T) {
		alarms, err := client.Alarm().ActiveSQL(ctx, db, 1)
		if err != nil {
			t.Fatalf("Alarm.ActiveSQL failed: %v", err)
		}
		if len(alarms) == 0 {
			t.Skip("no active alarm row available for subscription payload verification")
		}
		alarm := alarms[0]
		stream, err := client.Subscription().SubscribeTable(ctx, openplant.TableSubscribeRequest{
			DB:    db,
			Table: "Alarm",
			Columns: []string{
				"ID", "GN", "PN", "AN", "ED", "EU",
				"TM", "TA", "TF", "AV", "DS", "RT",
				"AP", "LC", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8",
			},
			Key:      "ID",
			Int32:    []int32{int32(alarm.ID)},
			Snapshot: true,
		})
		if err != nil {
			t.Fatalf("SubscribeTable Alarm failed: %v", err)
		}
		defer stream.Close()

		row := waitForTableSubscriptionRow(t, stream, 10*time.Second)
		if got := int32FromAny(row["ID"]); got != int32(alarm.ID) {
			t.Fatalf("Alarm subscription ID=%d want %d row=%#v", got, alarm.ID, row)
		}
		if got := stringFromAny(row["GN"]); got != string(alarm.GN) {
			t.Fatalf("Alarm subscription GN=%q want %q row=%#v", got, alarm.GN, row)
		}
		t.Logf("alarm subscription payload keys: %v", sortedRowKeys(row))
	})
}

func waitForTableSubscriptionRow(t testing.TB, stream *openplant.TableSubscribeStream, timeout time.Duration) map[string]any {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				t.Fatalf("table subscription closed before data row: %v", stream.Err())
			}
			if event.Err != nil || event.IsError() {
				t.Fatalf("table subscription error: %v", event.Err)
			}
			if !event.IsData() {
				continue
			}
			return event.Row
		case <-timer.C:
			t.Fatalf("timed out waiting for table subscription row")
		}
	}
}

func int32FromAny(value any) int32 {
	switch v := value.(type) {
	case int8:
		return int32(v)
	case int16:
		return int32(v)
	case int32:
		return v
	case int64:
		return int32(v)
	case int:
		return int32(v)
	case uint8:
		return int32(v)
	case uint16:
		return int32(v)
	case uint32:
		return int32(v)
	case uint64:
		return int32(v)
	case uint:
		return int32(v)
	default:
		return 0
	}
}

func stringFromAny(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func sortedRowKeys(row map[string]any) []string {
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
