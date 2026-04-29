package subscription

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/tc252617228/openplant/model"
)

type fakeResolver struct {
	mu        sync.Mutex
	responses []map[model.GN]model.PointID
	calls     int
}

func (r *fakeResolver) ResolvePoints(ctx context.Context, db model.DatabaseName, gns []model.GN) ([]model.Point, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.calls >= len(r.responses) {
		r.calls++
		return pointsFromMapping(r.responses[len(r.responses)-1]), nil
	}
	response := r.responses[r.calls]
	r.calls++
	return pointsFromMapping(response), nil
}

type fakeIDSource struct {
	stream     *fakeIDStream
	subscribed chan []model.PointID
}

func newFakeIDSource() *fakeIDSource {
	return &fakeIDSource{
		stream:     newFakeIDStream(),
		subscribed: make(chan []model.PointID, 1),
	}
}

func (s *fakeIDSource) SubscribeIDs(ctx context.Context, db model.DatabaseName, ids []model.PointID, emit func(Event) bool) (IDStream, error) {
	s.stream.setEmit(emit)
	s.subscribed <- append([]model.PointID(nil), ids...)
	go func() {
		<-ctx.Done()
		s.stream.Close()
	}()
	return s.stream, nil
}

type fakeIDStream struct {
	mu        sync.RWMutex
	emit      func(Event) bool
	done      chan struct{}
	closeOnce sync.Once
	added     chan []model.PointID
	removed   chan []model.PointID
}

func newFakeIDStream() *fakeIDStream {
	return &fakeIDStream{
		done:    make(chan struct{}),
		added:   make(chan []model.PointID, 4),
		removed: make(chan []model.PointID, 4),
	}
}

func (s *fakeIDStream) AddIDs(ctx context.Context, ids []model.PointID) error {
	s.added <- append([]model.PointID(nil), ids...)
	return nil
}

func (s *fakeIDStream) RemoveIDs(ctx context.Context, ids []model.PointID) error {
	s.removed <- append([]model.PointID(nil), ids...)
	return nil
}

func (s *fakeIDStream) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
	})
}

func (s *fakeIDStream) Done() <-chan struct{} {
	return s.done
}

func (s *fakeIDStream) Err() error {
	return nil
}

func (s *fakeIDStream) setEmit(emit func(Event) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emit = emit
}

func (s *fakeIDStream) emitEvent(event Event) bool {
	s.mu.RLock()
	emit := s.emit
	s.mu.RUnlock()
	if emit == nil {
		return false
	}
	return emit(event)
}

func TestGNDriftSourceResolvesGNAndExpandsAliasEvents(t *testing.T) {
	resolver := &fakeResolver{responses: []map[model.GN]model.PointID{{
		"W3.N.P1":       1001,
		"W3.N.P1_ALIAS": 1001,
	}}}
	idSource := newFakeIDSource()
	svc := NewService(Options{
		EventBuffer: 2,
		Source: &GNDriftSource{
			Source:          idSource,
			Resolver:        resolver,
			RefreshInterval: time.Hour,
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := svc.Subscribe(ctx, Request{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1", "W3.N.P1_ALIAS"},
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer stream.Close()
	assertIDs(t, receiveIDs(t, idSource.subscribed), []model.PointID{1001})

	if ok := idSource.stream.emitEvent(Event{Kind: EventData, Sample: model.Sample{ID: 1001, Value: model.R8(12.5)}}); !ok {
		t.Fatalf("emit failed")
	}
	first := receiveEvent(t, stream.Events())
	second := receiveEvent(t, stream.Events())
	if first.Kind != EventData || second.Kind != EventData {
		t.Fatalf("events=%#v %#v", first, second)
	}
	got := []model.GN{first.Sample.GN, second.Sample.GN}
	if !slices.Equal(got, []model.GN{"W3.N.P1", "W3.N.P1_ALIAS"}) {
		t.Fatalf("GNs=%v", got)
	}
}

func TestGNDriftSourceRefreshesDriftedIDs(t *testing.T) {
	resolver := &fakeResolver{responses: []map[model.GN]model.PointID{
		{"W3.N.P1": 1001},
		{"W3.N.P1": 2002},
	}}
	idSource := newFakeIDSource()
	svc := NewService(Options{
		EventBuffer: 1,
		Source: &GNDriftSource{
			Source:          idSource,
			Resolver:        resolver,
			RefreshInterval: 10 * time.Millisecond,
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := svc.Subscribe(ctx, Request{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer stream.Close()
	assertIDs(t, receiveIDs(t, idSource.subscribed), []model.PointID{1001})
	assertIDs(t, receiveIDs(t, idSource.stream.added), []model.PointID{2002})
	assertIDs(t, receiveIDs(t, idSource.stream.removed), []model.PointID{1001})

	if ok := idSource.stream.emitEvent(Event{Kind: EventData, Sample: model.Sample{ID: 2002, Value: model.R8(18.5)}}); !ok {
		t.Fatalf("emit failed")
	}
	event := receiveEvent(t, stream.Events())
	if event.Kind != EventData || event.Sample.ID != 2002 || event.Sample.GN != "W3.N.P1" {
		t.Fatalf("event=%#v", event)
	}
}

func TestGNDriftSourceKeepsExplicitIDDuringGNDrift(t *testing.T) {
	resolver := &fakeResolver{responses: []map[model.GN]model.PointID{
		{"W3.N.P1": 1001},
		{"W3.N.P1": 2002},
	}}
	idSource := newFakeIDSource()
	svc := NewService(Options{
		EventBuffer: 1,
		Source: &GNDriftSource{
			Source:          idSource,
			Resolver:        resolver,
			RefreshInterval: 10 * time.Millisecond,
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := svc.Subscribe(ctx, Request{
		DB:  "W3",
		IDs: []model.PointID{1001},
		GNs: []model.GN{"W3.N.P1"},
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer stream.Close()
	assertIDs(t, receiveIDs(t, idSource.subscribed), []model.PointID{1001})
	assertIDs(t, receiveIDs(t, idSource.stream.added), []model.PointID{2002})
	select {
	case ids := <-idSource.stream.removed:
		t.Fatalf("removed explicit IDs: %v", ids)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestGNDriftSourceIgnoresResolverSuperset(t *testing.T) {
	resolver := &fakeResolver{responses: []map[model.GN]model.PointID{{
		"W3.N.P1":    1001,
		"W3.N.EXTRA": 9999,
	}}}
	idSource := newFakeIDSource()
	svc := NewService(Options{
		Source: &GNDriftSource{
			Source:          idSource,
			Resolver:        resolver,
			RefreshInterval: time.Hour,
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := svc.Subscribe(ctx, Request{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer stream.Close()
	assertIDs(t, receiveIDs(t, idSource.subscribed), []model.PointID{1001})
}

func receiveIDs(t *testing.T, ch <-chan []model.PointID) []model.PointID {
	t.Helper()
	select {
	case ids := <-ch:
		return ids
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for IDs")
		return nil
	}
}

func receiveEvent(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for event")
		return Event{}
	}
}

func assertIDs(t *testing.T, got, want []model.PointID) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("ids=%v want %v", got, want)
	}
}

func pointsFromMapping(mapping map[model.GN]model.PointID) []model.Point {
	points := make([]model.Point, 0, len(mapping))
	for gn, id := range mapping {
		points = append(points, model.Point{ID: id, GN: gn})
	}
	return points
}
