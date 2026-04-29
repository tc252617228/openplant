package system

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tc252617228/openplant/model"
	sqlapi "github.com/tc252617228/openplant/sql"
)

type fakeQueryer struct {
	query string
	rows  []sqlapi.Row
	err   error
}

func (f *fakeQueryer) Query(ctx context.Context, query string) (sqlapi.Result, error) {
	f.query = query
	return sqlapi.Result{Rows: f.rows}, f.err
}

func TestReadSQLUsesRealtimeSystemGNs(t *testing.T) {
	tm := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(1001), "GN": "W3.SYS.SESSION", "TM": tm, "DS": int16(0), "AV": float64(3), "RT": int8(model.TypeAX), "FM": int16(0),
	}}}
	svc := NewService(Options{Queryer: fake})

	samples, err := svc.ReadSQL(context.Background(), Query{
		DB:      "W3",
		Metrics: []Metric{MetricSession, MetricLoad},
	})
	if err != nil {
		t.Fatalf("ReadSQL failed: %v", err)
	}
	if len(samples) != 1 || samples[0].Metric != MetricSession || samples[0].Sample.GN != "W3.SYS.SESSION" {
		t.Fatalf("unexpected samples: %#v", samples)
	}
	if got, ok := samples[0].Sample.Value.Float32(); !ok || got != 3 {
		t.Fatalf("value=%#v ok=%v", samples[0].Sample.Value.Interface(), ok)
	}
	for _, want := range []string{
		`SELECT "ID","GN","TM","DS","AV","RT","FM" FROM W3.Realtime`,
		`"GN" IN ('W3.SYS.SESSION','W3.SYS.LOAD')`,
		`ORDER BY "GN" ASC`,
	} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestHistorySQLUsesSpanArchiveShape(t *testing.T) {
	fake := &fakeQueryer{}
	svc := NewService(Options{Queryer: fake})
	_, err := svc.HistorySQL(context.Background(), HistoryQuery{
		DB:       "W3",
		Metrics:  []Metric{MetricSession},
		Range:    model.TimeRange{Begin: time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC), End: time.Date(2026, 1, 2, 3, 4, 8, 0, time.UTC)},
		Interval: "2s",
		Limit:    100,
	})
	if err != nil {
		t.Fatalf("HistorySQL failed: %v", err)
	}
	for _, want := range []string{
		`FROM W3.Archive`,
		`"GN" IN ('W3.SYS.SESSION')`,
		`"TM" BETWEEN '2026-01-02 03:04:05.123' AND '2026-01-02 03:04:08'`,
		`"MODE" = 'span'`,
		`"INTERVAL" = '2s'`,
		`ORDER BY "TM" ASC,"GN" ASC LIMIT 100`,
	} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestCatalogFormatsDatabaseSpecificFormulas(t *testing.T) {
	info, ok := LookupMetric(MetricRate, "W3")
	if !ok {
		t.Fatalf("MetricRate not found")
	}
	if info.Formula != `return op.rate("W3.SYS.EVENT", 5)` {
		t.Fatalf("formula=%q", info.Formula)
	}
	catalog, err := Catalog("W3")
	if err != nil {
		t.Fatalf("Catalog failed: %v", err)
	}
	if len(catalog) != len(Metrics()) {
		t.Fatalf("catalog=%d metrics=%d", len(catalog), len(Metrics()))
	}
}

func TestQueryRejectsUnknownMetric(t *testing.T) {
	svc := NewService(Options{Queryer: &fakeQueryer{}})
	_, err := svc.ReadSQL(context.Background(), Query{DB: "W3", Metrics: []Metric{"NOPE"}})
	if err == nil {
		t.Fatalf("expected unknown metric error")
	}
}
