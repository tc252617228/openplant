package stat

import (
	"context"
	"errors"
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

type fakeRequester struct {
	called  bool
	samples []model.StatSample
	err     error
}

func (f *fakeRequester) QueryStatByRequest(ctx context.Context, q Query) ([]model.StatSample, error) {
	f.called = true
	return f.samples, f.err
}

type fakeNative struct {
	called  bool
	samples []model.StatSample
	err     error
}

func (f *fakeNative) QueryStatNative(ctx context.Context, q Query) ([]model.StatSample, error) {
	f.called = true
	return f.samples, f.err
}

func TestStatQueryRequiresPointTimeAndInterval(t *testing.T) {
	begin := time.Now().Add(-time.Hour)
	end := time.Now()
	valid := Query{
		DB:       "W3",
		IDs:      []model.PointID{1001},
		Range:    model.TimeRange{Begin: begin, End: end},
		Mode:     model.ModeAvg,
		Interval: "1m",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}
	withoutInterval := valid
	withoutInterval.Interval = ""
	if err := withoutInterval.Validate(); err == nil {
		t.Fatalf("expected missing interval to be rejected")
	}
	withoutPoint := valid
	withoutPoint.IDs = nil
	if err := withoutPoint.Validate(); err == nil {
		t.Fatalf("expected missing point scope to be rejected")
	}
}

func TestStatQueryUsesBoundedReadonlySQL(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := begin.Add(time.Hour)
	maxTime := begin.Add(30 * time.Minute)
	minTime := begin.Add(10 * time.Minute)
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(1001), "GN": "W3.N.P1", "TM": begin.Add(time.Minute), "DS": int16(0),
		"FLOW": float64(1), "AVGV": float64(2), "MAXV": float64(3), "MINV": float64(4),
		"MAXTIME": maxTime, "MINTIME": minTime,
	}}}
	svc := NewService(Options{Queryer: fake})

	samples, err := svc.QuerySQL(context.Background(), Query{
		DB:       "W3",
		IDs:      []model.PointID{1001},
		GNs:      []model.GN{"W3.N.P1"},
		Range:    model.TimeRange{Begin: begin, End: end},
		Mode:     model.ModeAvg,
		Interval: "1m",
		Quality:  model.QualityDropTimeout,
		Limit:    20,
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("samples=%d want 1", len(samples))
	}
	sample := samples[0]
	if sample.ID != 1001 || sample.GN != "W3.N.P1" || sample.Avg != 2 || sample.MaxTime != maxTime || sample.Sum != 0 {
		t.Fatalf("unexpected sample: %#v", sample)
	}
	if strings.Contains(fake.query, `"MEAN"`) || strings.Contains(fake.query, `"SUM"`) {
		t.Fatalf("query should stay within documented Stat columns: %s", fake.query)
	}
	for _, want := range []string{
		`FROM W3.Stat`,
		`("ID" IN (1001) OR "GN" IN ('W3.N.P1'))`,
		`"TM" BETWEEN '2026-01-02 03:04:05' AND '2026-01-02 04:04:05'`,
		`"INTERVAL" = '1m'`,
		`"QTYPE" = 2`,
		`LIMIT 20`,
	} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestStatQuerySQLPreservesMillisecondTimeBounds(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	end := time.Date(2026, 1, 2, 3, 4, 6, 987654321, time.UTC)
	fake := &fakeQueryer{}
	svc := NewService(Options{Queryer: fake})

	_, err := svc.QuerySQL(context.Background(), Query{
		DB:       "W3",
		IDs:      []model.PointID{1001},
		Range:    model.TimeRange{Begin: begin, End: end},
		Mode:     model.ModeAvg,
		Interval: "1m",
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	want := `"TM" BETWEEN '2026-01-02 03:04:05.123' AND '2026-01-02 03:04:06.987'`
	if !strings.Contains(fake.query, want) {
		t.Fatalf("query missing millisecond bounds %q: %s", want, fake.query)
	}
}

func TestStatQueryNativeDoesNotFallback(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := begin.Add(time.Hour)
	nativeErr := errors.New("native failed")
	queryer := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(1001), "GN": "W3.N.P1", "TM": begin.Add(time.Minute), "DS": int16(0),
		"FLOW": float64(1), "AVGV": float64(2), "MAXV": float64(3), "MINV": float64(4),
		"MAXTIME": begin.Add(30 * time.Minute), "MINTIME": begin.Add(10 * time.Minute), "MEAN": float64(5), "SUM": float64(6),
	}}}
	requester := &fakeRequester{samples: []model.StatSample{{ID: 1001, Avg: 2}}}
	native := &fakeNative{err: nativeErr}
	svc := NewService(Options{Queryer: queryer, Requester: requester, Native: native})

	_, err := svc.QueryNative(context.Background(), Query{
		DB:       "W3",
		IDs:      []model.PointID{1001},
		Range:    model.TimeRange{Begin: begin, End: end},
		Mode:     model.ModeAvg,
		Interval: "1m",
	})
	if !errors.Is(err, nativeErr) {
		t.Fatalf("err=%v want native error", err)
	}
	if !native.called {
		t.Fatalf("native path was not called")
	}
	if requester.called {
		t.Fatalf("request path was called by QueryNative")
	}
	if queryer.query != "" {
		t.Fatalf("SQL path was called by QueryNative: %s", queryer.query)
	}
}

func TestStatQueryNativeRejectsGNsBeforeNativeCall(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := begin.Add(time.Hour)
	native := &fakeNative{}
	svc := NewService(Options{Native: native})

	_, err := svc.QueryNative(context.Background(), Query{
		DB:       "W3",
		GNs:      []model.GN{"W3.N.P1"},
		Range:    model.TimeRange{Begin: begin, End: end},
		Mode:     model.ModeAvg,
		Interval: "1m",
	})
	if err == nil {
		t.Fatalf("expected GN native query to be rejected")
	}
	if native.called {
		t.Fatalf("native path should not be called for GN query")
	}
}
