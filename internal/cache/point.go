package cache

import (
	"sync"
	"time"

	"github.com/tc252617228/openplant/model"
)

type pointGNKey struct {
	db model.DatabaseName
	gn model.GN
}

type pointIDKey struct {
	db model.DatabaseName
	id model.PointID
}

type pointEntry struct {
	point   model.Point
	expires time.Time
}

type PointCache struct {
	mu         sync.RWMutex
	ttl        time.Duration
	maxEntries int
	byGN       map[pointGNKey]pointEntry
	byID       map[pointIDKey]pointEntry
	now        func() time.Time
}

func NewPointCache(ttl time.Duration) *PointCache {
	return NewPointCacheWithLimit(ttl, 0)
}

func NewPointCacheWithLimit(ttl time.Duration, maxEntries int) *PointCache {
	return &PointCache{
		ttl:        ttl,
		maxEntries: maxEntries,
		byGN:       make(map[pointGNKey]pointEntry),
		byID:       make(map[pointIDKey]pointEntry),
		now:        time.Now,
	}
}

func (c *PointCache) GetByGN(db model.DatabaseName, gn model.GN) (model.Point, bool) {
	if c == nil || gn == "" {
		return model.Point{}, false
	}
	key := pointGNKey{db: db, gn: gn}
	c.mu.RLock()
	entry, ok := c.byGN[key]
	c.mu.RUnlock()
	if !ok {
		return model.Point{}, false
	}
	if c.expired(entry) {
		c.deleteEntryByGN(key, entry)
		return model.Point{}, false
	}
	return entry.point, true
}

func (c *PointCache) GetByID(db model.DatabaseName, id model.PointID) (model.Point, bool) {
	if c == nil || id <= 0 {
		return model.Point{}, false
	}
	key := pointIDKey{db: db, id: id}
	c.mu.RLock()
	entry, ok := c.byID[key]
	c.mu.RUnlock()
	if !ok {
		return model.Point{}, false
	}
	if c.expired(entry) {
		c.deleteEntryByID(key, entry)
		return model.Point{}, false
	}
	return entry.point, true
}

func (c *PointCache) Store(db model.DatabaseName, points []model.Point) {
	if c == nil || len(points) == 0 {
		return
	}
	expires := time.Time{}
	if c.ttl > 0 {
		expires = c.now().Add(c.ttl)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, point := range points {
		c.storeLocked(db, point, expires)
	}
	c.pruneLocked()
}

func (c *PointCache) StorePoint(db model.DatabaseName, point model.Point) {
	if c == nil {
		return
	}
	c.Store(db, []model.Point{point})
}

func (c *PointCache) InvalidateGN(db model.DatabaseName, gn model.GN) {
	if c == nil || gn == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.byGN[pointGNKey{db: db, gn: gn}]
	if !ok {
		return
	}
	c.deletePointLocked(db, entry.point)
}

func (c *PointCache) InvalidateID(db model.DatabaseName, id model.PointID) {
	if c == nil || id <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.byID[pointIDKey{db: db, id: id}]
	if !ok {
		return
	}
	c.deletePointLocked(db, entry.point)
}

func (c *PointCache) InvalidateDB(db model.DatabaseName) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.byGN {
		if key.db == db {
			delete(c.byGN, key)
		}
	}
	for key := range c.byID {
		if key.db == db {
			delete(c.byID, key)
		}
	}
}

func (c *PointCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byGN = make(map[pointGNKey]pointEntry)
	c.byID = make(map[pointIDKey]pointEntry)
}

func (c *PointCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.entryCountLocked()
}

func (c *PointCache) storeLocked(db model.DatabaseName, point model.Point, expires time.Time) {
	if point.ID <= 0 && point.GN == "" {
		return
	}
	entry := pointEntry{point: point, expires: expires}
	if point.ID > 0 {
		idKey := pointIDKey{db: db, id: point.ID}
		if old, ok := c.byID[idKey]; ok && old.point.GN != "" && old.point.GN != point.GN {
			delete(c.byGN, pointGNKey{db: db, gn: old.point.GN})
		}
		c.byID[idKey] = entry
	}
	if point.GN != "" {
		gnKey := pointGNKey{db: db, gn: point.GN}
		if old, ok := c.byGN[gnKey]; ok && old.point.ID > 0 && old.point.ID != point.ID {
			delete(c.byID, pointIDKey{db: db, id: old.point.ID})
		}
		c.byGN[gnKey] = entry
	}
}

func (c *PointCache) expired(entry pointEntry) bool {
	if entry.expires.IsZero() {
		return false
	}
	return !c.now().Before(entry.expires)
}

func (c *PointCache) pruneLocked() {
	remaining := c.entryCountLocked()
	if c.maxEntries <= 0 || remaining <= c.maxEntries {
		return
	}
	now := c.now()
	for key, entry := range c.byID {
		if entry.expires.IsZero() || now.Before(entry.expires) {
			continue
		}
		if c.deletePointCountedLocked(key.db, entry.point) {
			remaining--
		}
		if remaining <= c.maxEntries {
			return
		}
	}
	for key, entry := range c.byGN {
		if entry.point.ID > 0 || entry.expires.IsZero() || now.Before(entry.expires) {
			continue
		}
		if c.deletePointCountedLocked(key.db, entry.point) {
			remaining--
		}
		if remaining <= c.maxEntries {
			return
		}
	}
	for key, entry := range c.byID {
		if c.deletePointCountedLocked(key.db, entry.point) {
			remaining--
		}
		if remaining <= c.maxEntries {
			return
		}
	}
	for key, entry := range c.byGN {
		if entry.point.ID > 0 {
			continue
		}
		if c.deletePointCountedLocked(key.db, entry.point) {
			remaining--
		}
		if remaining <= c.maxEntries {
			return
		}
	}
}

func (c *PointCache) entryCountLocked() int {
	count := len(c.byID)
	for _, entry := range c.byGN {
		if entry.point.ID <= 0 {
			count++
		}
	}
	return count
}

func (c *PointCache) deleteEntryByGN(key pointGNKey, entry pointEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.byGN[key]; ok && current == entry {
		c.deletePointLocked(key.db, entry.point)
	}
}

func (c *PointCache) deleteEntryByID(key pointIDKey, entry pointEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.byID[key]; ok && current == entry {
		c.deletePointLocked(key.db, entry.point)
	}
}

func (c *PointCache) deletePointLocked(db model.DatabaseName, point model.Point) {
	if point.GN != "" {
		delete(c.byGN, pointGNKey{db: db, gn: point.GN})
	}
	if point.ID > 0 {
		delete(c.byID, pointIDKey{db: db, id: point.ID})
	}
}

func (c *PointCache) deletePointCountedLocked(db model.DatabaseName, point model.Point) bool {
	if point.ID > 0 {
		if _, ok := c.byID[pointIDKey{db: db, id: point.ID}]; !ok {
			return false
		}
		c.deletePointLocked(db, point)
		return true
	}
	if point.GN != "" {
		if _, ok := c.byGN[pointGNKey{db: db, gn: point.GN}]; !ok {
			return false
		}
		c.deletePointLocked(db, point)
		return true
	}
	return false
}
