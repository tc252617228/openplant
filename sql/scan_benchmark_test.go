package sql

import (
	"testing"
	"time"
)

type benchmarkScanSample struct {
	ID     int32     `openplant:"ID"`
	GN     string    `openplant:"GN"`
	Time   time.Time `openplant:"TM"`
	Status int16     `openplant:"DS"`
	Value  float64   `openplant:"AV"`
}

func BenchmarkScanRows(b *testing.B) {
	rows := make([]Row, 1000)
	for i := range rows {
		rows[i] = Row{
			"ID": int32(1000 + i),
			"GN": "W3.N.P1",
			"TM": int32(123456 + i),
			"DS": int16(0),
			"AV": float64(i),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := ScanRows[benchmarkScanSample](rows)
		if err != nil {
			b.Fatal(err)
		}
		if len(got) != len(rows) {
			b.Fatalf("rows=%d", len(got))
		}
	}
}
