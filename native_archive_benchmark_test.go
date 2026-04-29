package openplant

import (
	"bytes"
	"testing"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/model"
)

func BenchmarkDecodeNativeArchive(b *testing.B) {
	raw := benchmarkNativeArchiveResponse(b, 1000)
	ids := []model.PointID{1001}
	idToGN := map[model.PointID]model.GN{1001: "W3.N.P1"}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		samples, err := decodeNativeArchives(raw, ids, idToGN)
		if err != nil {
			b.Fatal(err)
		}
		if len(samples) != 1000 {
			b.Fatalf("samples=%d", len(samples))
		}
	}
}

func BenchmarkStreamNativeArchiveDecode(b *testing.B) {
	raw := benchmarkNativeArchiveResponse(b, 1000)
	ids := []model.PointID{1001}
	idToGN := map[model.PointID]model.GN{1001: "W3.N.P1"}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var count int
		err := decodeNativeArchivesStream(codec.NewReader(bytes.NewReader(raw)), ids, idToGN, func(sample model.Sample) bool {
			count++
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
		if count != 1000 {
			b.Fatalf("samples=%d", count)
		}
	}
}

func BenchmarkStreamNativeStatDecode(b *testing.B) {
	raw := benchmarkNativeStatResponse(b, 1000)
	ids := []model.PointID{1001}
	idToGN := map[model.PointID]model.GN{1001: "W3.N.P1"}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var count int
		err := decodeNativeStatsStream(codec.NewReader(bytes.NewReader(raw)), ids, nativeModeAvg, idToGN, func(sample model.StatSample) bool {
			count++
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
		if count != 1000 {
			b.Fatalf("samples=%d", count)
		}
	}
}

func BenchmarkEncodeNativeArchiveRequest(b *testing.B) {
	ids := make([]model.PointID, 200)
	for i := range ids {
		ids[i] = model.PointID(1000 + i)
	}
	begin := time.Unix(123456, 0)
	end := begin.Add(time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := encodeNativeArchiveRequest(ids, nativeModeRaw, int32(model.QualityNone), begin, end, 1)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func BenchmarkEncodeRealtimeRead(b *testing.B) {
	ids := make([]model.PointID, 200)
	for i := range ids {
		ids[i] = model.PointID(1000 + i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := encodeRealtimeRead(ids)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func benchmarkNativeArchiveResponse(b testing.TB, n int) []byte {
	b.Helper()
	var response bytes.Buffer
	resp := codec.NewWriter(&response)
	_ = resp.WriteInt32(protocol.Magic)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt32(1)
	_ = resp.WriteInt8(1)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt8(int8(model.TypeR8))
	_ = resp.WriteInt32(int32(n))
	for i := 0; i < n; i++ {
		_ = resp.WriteInt32(int32(123456 + i))
		_ = resp.WriteInt16(0)
		if err := codec.EncodeTSValue(&response, model.R8(float64(i))); err != nil {
			b.Fatal(err)
		}
	}
	_ = resp.WriteInt8(0)
	_ = resp.WriteInt32(protocol.Magic)
	return response.Bytes()
}

func benchmarkNativeStatResponse(b testing.TB, n int) []byte {
	b.Helper()
	var response bytes.Buffer
	resp := codec.NewWriter(&response)
	_ = resp.WriteInt32(protocol.Magic)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt32(1)
	_ = resp.WriteInt8(1)
	_ = resp.WriteInt32(0)
	_ = resp.WriteInt8(int8(model.TypeR8))
	_ = resp.WriteInt32(int32(n))
	for i := 0; i < n; i++ {
		_ = resp.WriteInt32(int32(123456 + i))
		_ = resp.WriteInt16(0)
		_ = resp.WriteFloat64(float64(i))
	}
	_ = resp.WriteInt8(0)
	_ = resp.WriteInt32(protocol.Magic)
	return response.Bytes()
}
