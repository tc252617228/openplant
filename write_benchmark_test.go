package openplant

import (
	"testing"
	"time"

	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/realtime"
)

func BenchmarkEncodeRealtimeWriteNativeRequest(b *testing.B) {
	values := make([]realtime.Write, 200)
	for i := range values {
		values[i] = realtime.Write{
			ID:     model.PointID(1000 + i),
			Time:   time.Unix(int64(123456+i), 0),
			Status: 0,
			Value:  model.R8(float64(i)),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := encodeRealtimeWriteNativeRequest(values, model.TypeR8)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func BenchmarkEncodeArchiveWriteNativeRequest(b *testing.B) {
	samples := make([]model.Sample, 200)
	for i := range samples {
		samples[i] = model.Sample{
			ID:     1001,
			Type:   model.TypeR8,
			Time:   time.Unix(int64(123456+i), 0),
			Status: 0,
			Value:  model.R8(float64(i)),
		}
	}
	blocks := []archiveWriteBlock{{
		ID:      1001,
		Type:    model.TypeR8,
		Samples: samples,
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := encodeArchiveWriteNativeRequest(blocks, false)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}
