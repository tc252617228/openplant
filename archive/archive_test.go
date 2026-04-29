package archive

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
	samples []model.Sample
	err     error
}

func (f *fakeRequester) QueryArchiveByRequest(ctx context.Context, q Query) ([]model.Sample, error) {
	f.called = true
	return f.samples, f.err
}

type fakeNative struct {
	called  bool
	samples []model.Sample
	err     error
}

func (f *fakeNative) QueryArchiveNative(ctx context.Context, q Query) ([]model.Sample, error) {
	f.called = true
	return f.samples, f.err
}

func TestArchiveQueryRequiresPointAndTimeBounds(t *testing.T) {
	now := time.Now()
	valid := Query{
		DB:    "W3",
		GNs:   []model.GN{"W3.NODE.P1"},
		Range: model.TimeRange{Begin: now.Add(-time.Hour), End: now},
		Mode:  model.ModeRaw,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}
	withoutPoint := valid
	withoutPoint.GNs = nil
	if err := withoutPoint.Validate(); err == nil {
		t.Fatalf("expected missing point scope to be rejected")
	}
	withoutTime := valid
	withoutTime.Range = model.TimeRange{}
	if err := withoutTime.Validate(); err == nil {
		t.Fatalf("expected missing time range to be rejected")
	}
	spanWithoutInterval := valid
	spanWithoutInterval.Mode = model.ModeSpan
	if err := spanWithoutInterval.Validate(); err == nil {
		t.Fatalf("expected span mode without interval to be rejected")
	}
}

func TestArchiveQueryUsesBoundedReadonlySQL(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := begin.Add(time.Hour)
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(1001), "GN": "W3.N.P1", "TM": begin.Add(time.Minute), "DS": int16(0), "AV": float64(12.5),
	}}}
	svc := NewService(Options{Queryer: fake})

	samples, err := svc.QuerySQL(context.Background(), Query{
		DB:      "W3",
		IDs:     []model.PointID{1001},
		GNs:     []model.GN{"W3.N.P1"},
		Range:   model.TimeRange{Begin: begin, End: end},
		Mode:    model.ModeRaw,
		Quality: model.QualityDropBad,
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("samples=%d want 1", len(samples))
	}
	sample := samples[0]
	if sample.ID != 1001 || sample.GN != "W3.N.P1" || sample.Type != model.TypeR8 || sample.Status != 0 {
		t.Fatalf("unexpected sample: %#v", sample)
	}
	value, ok := sample.Value.Float64()
	if !ok || value != 12.5 {
		t.Fatalf("value=%#v ok=%v", sample.Value.Interface(), ok)
	}
	for _, want := range []string{
		`FROM W3.Archive`,
		`("ID" IN (1001) OR "GN" IN ('W3.N.P1'))`,
		`"TM" BETWEEN '2026-01-02 03:04:05' AND '2026-01-02 04:04:05'`,
		`"MODE" = 'raw'`,
		`"QTYPE" = 1`,
		`LIMIT 10`,
	} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestArchiveQueryIncludesIntervalForIntervalModes(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := begin.Add(time.Hour)
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
	if !strings.Contains(fake.query, `"INTERVAL" = '1m'`) {
		t.Fatalf("query missing interval: %s", fake.query)
	}
}

func TestArchiveSnapshotSQLUsesOPConsoleProjection(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := begin.Add(time.Hour)
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(1001), "GN": "W3.N.P1", "TM": begin.Add(time.Minute),
		"DS": int16(0), "AV": float64(12.5), "RT": int8(model.TypeR8), "FM": int16(3),
	}}}
	svc := NewService(Options{Queryer: fake})

	samples, err := svc.SnapshotSQL(context.Background(), SnapshotQuery{
		DB:       "W3",
		IDs:      []model.PointID{1001},
		Range:    model.TimeRange{Begin: begin, End: end},
		Interval: "2s",
		Limit:    20,
	})
	if err != nil {
		t.Fatalf("SnapshotSQL failed: %v", err)
	}
	if len(samples) != 1 || samples[0].Type != model.TypeR8 || samples[0].Format != 3 {
		t.Fatalf("unexpected snapshot samples: %#v", samples)
	}
	for _, want := range []string{
		`SELECT "ID","GN","TM","DS","AV","RT","FM" FROM W3.Archive`,
		`"MODE" = 'span'`,
		`"INTERVAL" = '2s'`,
		`ORDER BY "TM" ASC,"ID" ASC`,
		`LIMIT 20`,
	} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("snapshot query missing %q: %s", want, fake.query)
		}
	}
}

func TestArchiveQuerySQLPreservesMillisecondTimeBounds(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	end := time.Date(2026, 1, 2, 3, 4, 6, 987654321, time.UTC)
	fake := &fakeQueryer{}
	svc := NewService(Options{Queryer: fake})

	_, err := svc.QuerySQL(context.Background(), Query{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: end},
		Mode:  model.ModeRaw,
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	want := `"TM" BETWEEN '2026-01-02 03:04:05.123' AND '2026-01-02 03:04:06.987'`
	if !strings.Contains(fake.query, want) {
		t.Fatalf("query missing millisecond bounds %q: %s", want, fake.query)
	}
}

func TestArchiveQueryNativeDoesNotFallback(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := begin.Add(time.Hour)
	nativeErr := errors.New("native failed")
	queryer := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(1001), "GN": "W3.N.P1", "TM": begin.Add(time.Minute), "DS": int16(0), "AV": float64(12.5),
	}}}
	requester := &fakeRequester{samples: []model.Sample{{ID: 1001, Value: model.R8(12.5)}}}
	native := &fakeNative{err: nativeErr}
	svc := NewService(Options{Queryer: queryer, Requester: requester, Native: native})

	_, err := svc.QueryNative(context.Background(), Query{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: end},
		Mode:  model.ModeRaw,
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

func TestArchiveQueryNativeRejectsGNsBeforeNativeCall(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := begin.Add(time.Hour)
	native := &fakeNative{}
	svc := NewService(Options{Native: native})

	_, err := svc.QueryNative(context.Background(), Query{
		DB:    "W3",
		GNs:   []model.GN{"W3.N.P1"},
		Range: model.TimeRange{Begin: begin, End: end},
		Mode:  model.ModeRaw,
	})
	if err == nil {
		t.Fatalf("expected GN native query to be rejected")
	}
	if native.called {
		t.Fatalf("native path should not be called for GN query")
	}
}
