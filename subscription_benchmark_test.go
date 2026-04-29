package openplant

import (
	"bytes"
	"testing"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/model"
)

func BenchmarkSubscriptionRealtimeDecode(b *testing.B) {
	raw := benchmarkSubscriptionRealtimeResponse(b, 1000)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events, err := decodeSubscriptionEvents(raw)
		if err != nil {
			b.Fatal(err)
		}
		if len(events) != 1000 {
			b.Fatalf("events=%d", len(events))
		}
	}
}

func BenchmarkSubscriptionTableDecode(b *testing.B) {
	raw := benchmarkSubscriptionTableResponse(b, 1000)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events, err := decodeSubscriptionTableEvents(raw)
		if err != nil {
			b.Fatal(err)
		}
		if len(events) != 1000 {
			b.Fatalf("events=%d", len(events))
		}
	}
}

func benchmarkSubscriptionRealtimeResponse(b testing.TB, n int) []byte {
	b.Helper()
	columns := []codec.Column{
		{Name: "ID", Type: codec.VtInt32},
		{Name: "GN", Type: codec.VtString},
		{Name: "TM", Type: codec.VtDateTime},
		{Name: "DS", Type: codec.VtInt16},
		{Name: "AV", Type: codec.VtObject},
	}
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"ID": int32(1000 + i),
			"GN": "W3.N.P1",
			"TM": time.Unix(int64(123456+i), 0),
			"DS": int16(0),
			"AV": float64(i),
		}
	}
	return benchmarkTableResponse(b, columns, rows)
}

func benchmarkSubscriptionTableResponse(b testing.TB, n int) []byte {
	b.Helper()
	columns := []codec.Column{
		{Name: "ID", Type: codec.VtInt32},
		{Name: "GN", Type: codec.VtString},
		{Name: "PN", Type: codec.VtString},
		{Name: "RT", Type: codec.VtInt8},
		{Name: "ED", Type: codec.VtString},
	}
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"ID": int32(1000 + i),
			"GN": "W3.N.P1",
			"PN": "P1",
			"RT": int8(model.TypeR8),
			"ED": "benchmark point",
		}
	}
	return benchmarkTableResponse(b, columns, rows)
}

func benchmarkTableResponse(b testing.TB, columns []codec.Column, rows []map[string]any) []byte {
	b.Helper()
	body, err := codec.EncodeDataSet(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	var response bytes.Buffer
	if err := codec.NewEncoder(&response).EncodeMap(map[string]any{
		protocol.PropErrNo:   int32(0),
		protocol.PropColumns: codec.EncodeColumns(columns),
	}); err != nil {
		b.Fatal(err)
	}
	response.Write(body)
	return response.Bytes()
}
