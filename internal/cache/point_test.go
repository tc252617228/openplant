package cache

import (
	"testing"
	"time"

	"github.com/tc252617228/openplant/model"
)

func TestPointCacheIndexesByGNAndID(t *testing.T) {
	c := NewPointCache(time.Minute)
	point := model.Point{ID: 1001, GN: "W3.N.P1", Type: model.TypeR8}

	c.StorePoint("W3", point)

	byGN, ok := c.GetByGN("W3", "W3.N.P1")
	if !ok || byGN.ID != point.ID || byGN.Type != point.Type {
		t.Fatalf("GetByGN=%#v ok=%v", byGN, ok)
	}
	byID, ok := c.GetByID("W3", 1001)
	if !ok || byID.GN != point.GN || byID.Type != point.Type {
		t.Fatalf("GetByID=%#v ok=%v", byID, ok)
	}
}

func TestPointCacheExpiresEntries(t *testing.T) {
	now := time.Unix(100, 0)
	c := NewPointCache(time.Second)
	c.now = func() time.Time { return now }

	c.StorePoint("W3", model.Point{ID: 1001, GN: "W3.N.P1"})
	now = now.Add(time.Second)

	if _, ok := c.GetByGN("W3", "W3.N.P1"); ok {
		t.Fatalf("expected GN entry to expire")
	}
	if _, ok := c.GetByID("W3", 1001); ok {
		t.Fatalf("expected ID entry to expire")
	}
}

func TestPointCacheInvalidateDB(t *testing.T) {
	c := NewPointCache(time.Minute)
	c.StorePoint("W3", model.Point{ID: 1001, GN: "W3.N.P1"})
	c.StorePoint("W4", model.Point{ID: 1001, GN: "W4.N.P1"})

	c.InvalidateDB("W3")

	if _, ok := c.GetByID("W3", 1001); ok {
		t.Fatalf("expected W3 entry to be invalidated")
	}
	if _, ok := c.GetByID("W4", 1001); !ok {
		t.Fatalf("expected W4 entry to remain")
	}
}

func TestPointCacheRefreshesGNDrift(t *testing.T) {
	c := NewPointCache(time.Minute)
	c.StorePoint("W3", model.Point{ID: 1001, GN: "W3.N.P1", Type: model.TypeR8})
	c.StorePoint("W3", model.Point{ID: 2002, GN: "W3.N.P1", Type: model.TypeI4})

	byGN, ok := c.GetByGN("W3", "W3.N.P1")
	if !ok || byGN.ID != 2002 || byGN.Type != model.TypeI4 {
		t.Fatalf("GetByGN=%#v ok=%v", byGN, ok)
	}
	if _, ok := c.GetByID("W3", 1001); ok {
		t.Fatalf("old ID mapping should be removed after GN drift")
	}
	byID, ok := c.GetByID("W3", 2002)
	if !ok || byID.GN != "W3.N.P1" {
		t.Fatalf("GetByID=%#v ok=%v", byID, ok)
	}
}

func TestPointCacheRefreshesIDRename(t *testing.T) {
	c := NewPointCache(time.Minute)
	c.StorePoint("W3", model.Point{ID: 1001, GN: "W3.N.P1"})
	c.StorePoint("W3", model.Point{ID: 1001, GN: "W3.N.P2"})

	if _, ok := c.GetByGN("W3", "W3.N.P1"); ok {
		t.Fatalf("old GN mapping should be removed after ID rename")
	}
	byID, ok := c.GetByID("W3", 1001)
	if !ok || byID.GN != "W3.N.P2" {
		t.Fatalf("GetByID=%#v ok=%v", byID, ok)
	}
}

func TestPointCachePrunesToConfiguredLimit(t *testing.T) {
	c := NewPointCacheWithLimit(time.Minute, 2)
	c.Store("W3", []model.Point{
		{ID: 1001, GN: "W3.N.P1"},
		{ID: 1002, GN: "W3.N.P2"},
		{ID: 1003, GN: "W3.N.P3"},
	})
	if got := c.Len(); got != 2 {
		t.Fatalf("cache len=%d want 2", got)
	}
}

func TestPointCacheLimitIncludesGNOnlyEntries(t *testing.T) {
	c := NewPointCacheWithLimit(time.Minute, 1)
	c.Store("W3", []model.Point{
		{GN: "W3.N.P1"},
		{GN: "W3.N.P2"},
	})
	if got := c.Len(); got != 1 {
		t.Fatalf("cache len=%d want 1", got)
	}
}
