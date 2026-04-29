package stat

import (
	"context"
	"testing"
	"time"

	"github.com/tc252617228/openplant/model"
	sqlapi "github.com/tc252617228/openplant/sql"
)

type benchmarkStatQueryer struct {
	rows []sqlapi.Row
}

func (q benchmarkStatQueryer) Query(ctx context.Context, query string) (sqlapi.Result, error) {
	return sqlapi.Result{Rows: q.rows}, nil
}

func BenchmarkStatQuerySQLRows(b *testing.B) {
	rows := make([]sqlapi.Row, 1000)
	for i := range rows {
		rows[i] = sqlapi.Row{
			"ID":      int32(1001),
			"GN":      "W3.N.P1",
			"TM":      time.Unix(int64(123456+i), 0),
			"DS":      int16(0),
			"FLOW":    float64(i),
			"AVGV":    float64(i),
			"MAXV":    float64(i + 1),
			"MINV":    float64(i - 1),
			"MAXTIME": time.Unix(int64(123456+i), 0),
			"MINTIME": time.Unix(int64(123456+i), 0),
		}
	}
	svc := NewService(Options{Queryer: benchmarkStatQueryer{rows: rows}})
	query := Query{
		DB:       "W3",
		IDs:      []model.PointID{1001},
		Range:    model.TimeRange{Begin: time.Unix(123456, 0), End: time.Unix(124456, 0)},
		Mode:     model.ModeAvg,
		Interval: model.Interval("1m"),
		Quality:  model.QualityNone,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		samples, err := svc.QuerySQL(context.Background(), query)
		if err != nil {
			b.Fatal(err)
		}
		if len(samples) != len(rows) {
			b.Fatalf("samples=%d", len(samples))
		}
	}
}
