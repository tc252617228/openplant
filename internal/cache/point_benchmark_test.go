package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/tc252617228/openplant/model"
)

func BenchmarkPointCacheLookupByID(b *testing.B) {
	cache := NewPointCacheWithLimit(time.Minute, 2000)
	points := make([]model.Point, 1000)
	for i := range points {
		points[i] = model.Point{
			ID: model.PointID(1000 + i),
			GN: model.GN(fmt.Sprintf("W3.N.P%d", i)),
		}
	}
	cache.Store("W3", points)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		point, ok := cache.GetByID("W3", model.PointID(1000+i%len(points)))
		if !ok || point.ID == 0 {
			b.Fatalf("missing point")
		}
	}
}

func BenchmarkPointCacheStoreWithLimit(b *testing.B) {
	points := make([]model.Point, 1000)
	for i := range points {
		points[i] = model.Point{
			ID: model.PointID(1000 + i),
			GN: model.GN(fmt.Sprintf("W3.N.P%d", i)),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache := NewPointCacheWithLimit(time.Minute, 500)
		cache.Store("W3", points)
		if cache.Len() != 500 {
			b.Fatalf("len=%d", cache.Len())
		}
	}
}
