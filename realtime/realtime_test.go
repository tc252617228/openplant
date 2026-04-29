package realtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
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

func (f *fakeRequester) QueryRealtimeByRequest(ctx context.Context, req ReadRequest) ([]model.Sample, error) {
	f.called = true
	return f.samples, f.err
}

type fakeReader struct {
	called bool
}

func (f *fakeReader) ReadRealtime(ctx context.Context, req ReadRequest) ([]model.Sample, error) {
	f.called = true
	return nil, nil
}

func TestQuerySQLUsesBoundedReadonlySQL(t *testing.T) {
	tm := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(1001), "GN": "W3.N.P1", "TM": tm, "DS": int16(0), "AV": float64(12.5),
	}}}
	svc := NewService(Options{Queryer: fake})

	samples, err := svc.QuerySQL(context.Background(), ReadRequest{
		DB:  "W3",
		IDs: []model.PointID{1001},
		GNs: []model.GN{"W3.N.P1"},
	})
	if err != nil {
		t.Fatalf("QuerySQL failed: %v", err)
	}
	if len(samples) != 1 || samples[0].ID != 1001 || samples[0].GN != "W3.N.P1" || samples[0].Time != tm {
		t.Fatalf("unexpected samples: %#v", samples)
	}
	value, ok := samples[0].Value.Float64()
	if !ok || value != 12.5 {
		t.Fatalf("value=%#v ok=%v", samples[0].Value.Interface(), ok)
	}
	for _, want := range []string{
		`FROM W3.Realtime`,
		`("ID" IN (1001) OR "GN" IN ('W3.N.P1'))`,
		`ORDER BY "ID" ASC`,
	} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestQueryRequestAllowsGNWithoutNativeReader(t *testing.T) {
	requester := &fakeRequester{samples: []model.Sample{{ID: 1001, GN: "W3.N.P1"}}}
	reader := &fakeReader{}
	svc := NewService(Options{Requester: requester, Reader: reader})

	samples, err := svc.QueryRequest(context.Background(), ReadRequest{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err != nil {
		t.Fatalf("QueryRequest failed: %v", err)
	}
	if len(samples) != 1 || samples[0].GN != "W3.N.P1" {
		t.Fatalf("unexpected samples: %#v", samples)
	}
	if !requester.called {
		t.Fatalf("request path was not called")
	}
	if reader.called {
		t.Fatalf("native reader was called by QueryRequest")
	}
}

func TestReadRejectsGNBeforeNativeReader(t *testing.T) {
	reader := &fakeReader{}
	svc := NewService(Options{Reader: reader})

	_, err := svc.Read(context.Background(), ReadRequest{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err == nil || !operror.IsKind(err, operror.KindUnsupported) {
		t.Fatalf("expected unsupported GN native read, got %v", err)
	}
	if reader.called {
		t.Fatalf("native reader should not be called for GN read")
	}
}
