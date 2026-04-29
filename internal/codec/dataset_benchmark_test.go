package codec

import (
	"testing"
	"time"
)

func BenchmarkDecodeDataSet(b *testing.B) {
	columns := benchmarkColumns()
	data := benchmarkDataSet(b, columns, 1000)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := DecodeDataSet(data, columns)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != 1000 {
			b.Fatalf("rows=%d", len(rows))
		}
	}
}

func BenchmarkRowDecoderDecode(b *testing.B) {
	columns := benchmarkColumns()
	row, err := EncodeRow(columns, benchmarkRow(1))
	if err != nil {
		b.Fatal(err)
	}
	decoder := NewRowDecoder(columns)
	b.ReportAllocs()
	b.SetBytes(int64(len(row)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := decoder.Decode(row)
		if err != nil {
			b.Fatal(err)
		}
		if got["ID"] != int32(1001) {
			b.Fatalf("ID=%#v", got["ID"])
		}
	}
}

func benchmarkColumns() []Column {
	return []Column{
		{Name: "ID", Type: VtInt32},
		{Name: "GN", Type: VtString},
		{Name: "TM", Type: VtDateTime},
		{Name: "DS", Type: VtInt16},
		{Name: "AV", Type: VtDouble},
		{Name: "ED", Type: VtString},
	}
}

func benchmarkDataSet(b testing.TB, columns []Column, n int) []byte {
	b.Helper()
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = benchmarkRow(i)
	}
	data, err := EncodeDataSet(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	return data
}

func benchmarkRow(i int) map[string]any {
	return map[string]any{
		"ID": int32(1000 + i),
		"GN": "W3.N.P1",
		"TM": time.Unix(int64(123456+i), 0),
		"DS": int16(0),
		"AV": float64(i),
		"ED": "benchmark row",
	}
}
