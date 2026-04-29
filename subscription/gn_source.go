package subscription

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
)

type PointResolver interface {
	ResolvePoints(ctx context.Context, db model.DatabaseName, gns []model.GN) ([]model.Point, error)
}

type IDSource interface {
	SubscribeIDs(ctx context.Context, db model.DatabaseName, ids []model.PointID, emit func(Event) bool) (IDStream, error)
}

type IDStream interface {
	AddIDs(ctx context.Context, ids []model.PointID) error
	RemoveIDs(ctx context.Context, ids []model.PointID) error
	Close()
	Done() <-chan struct{}
	Err() error
}

type GNDriftSource struct {
	Source          IDSource
	Resolver        PointResolver
	RefreshInterval time.Duration
}

func (s *GNDriftSource) Subscribe(ctx context.Context, req Request, emit func(Event) bool) error {
	if err := req.Validate(); err != nil {
		return err
	}
	if s.Source == nil {
		return operror.Unsupported("subscription.GNDriftSource.Subscribe", "ID source is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	explicitIDs := uniquePointIDs(req.IDs)
	explicitIDSet := pointIDSet(explicitIDs)
	if len(req.GNs) == 0 {
		stream, err := s.Source.SubscribeIDs(ctx, req.DB, explicitIDs, emit)
		if err != nil {
			return err
		}
		defer stream.Close()
		return waitIDStream(ctx, stream)
	}
	if s.Resolver == nil {
		return operror.Unsupported("subscription.GNDriftSource.Subscribe", "point resolver is not configured")
	}

	gns := uniqueGNs(req.GNs)
	initial, err := s.resolveGNToID(ctx, req.DB, gns)
	if err != nil {
		return err
	}
	binding := buildGNBinding(initial)
	var bindingMu sync.RWMutex
	ids := mergePointIDs(explicitIDs, binding.ids)
	stream, err := s.Source.SubscribeIDs(ctx, req.DB, ids, func(event Event) bool {
		return emitBoundEvent(emit, event, explicitIDSet, &bindingMu, &binding)
	})
	if err != nil {
		return err
	}
	defer stream.Close()

	interval := s.RefreshInterval
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stream.Done():
			return stream.Err()
		case <-ticker.C:
			if err := s.refreshBinding(ctx, req.DB, gns, stream, explicitIDSet, &bindingMu, &binding); err != nil {
				if !emit(Event{Kind: EventError, Err: err}) {
					return ctx.Err()
				}
			}
		}
	}
}

func (s *GNDriftSource) resolveGNToID(ctx context.Context, db model.DatabaseName, gns []model.GN) (map[model.GN]model.PointID, error) {
	points, err := s.Resolver.ResolvePoints(ctx, db, gns)
	if err != nil {
		return nil, err
	}
	found := make(map[model.GN]model.PointID, len(points))
	for _, point := range points {
		if point.GN != "" && point.ID > 0 {
			found[point.GN] = point.ID
		}
	}
	out := make(map[model.GN]model.PointID, len(gns))
	for _, gn := range gns {
		if found[gn] <= 0 {
			return nil, operror.Validation("subscription.GNDriftSource.resolveGNToID", fmt.Sprintf("GN not found: %s", gn))
		}
		out[gn] = found[gn]
	}
	return out, nil
}

func (s *GNDriftSource) refreshBinding(ctx context.Context, db model.DatabaseName, gns []model.GN, stream IDStream, explicitIDs map[model.PointID]struct{}, mu *sync.RWMutex, binding *gnBinding) error {
	target, err := s.resolveGNToID(ctx, db, gns)
	if err != nil {
		return err
	}
	mu.RLock()
	current := cloneGNToID(binding.gnToID)
	mu.RUnlock()
	plan := planGNRebind(current, target)
	addIDs := filterImplicitIDs(plan.addIDs, explicitIDs)
	removeIDs := filterImplicitIDs(plan.removeIDs, explicitIDs)
	if len(addIDs) == 0 && len(removeIDs) == 0 && len(plan.changedGNs) == 0 {
		return nil
	}
	if len(addIDs) > 0 {
		if err := stream.AddIDs(ctx, addIDs); err != nil {
			return err
		}
	}
	next := buildGNBinding(target)
	mu.Lock()
	*binding = next
	mu.Unlock()
	if len(removeIDs) > 0 {
		if err := stream.RemoveIDs(ctx, removeIDs); err != nil {
			return err
		}
	}
	return nil
}

func emitBoundEvent(emit func(Event) bool, event Event, explicitIDs map[model.PointID]struct{}, mu *sync.RWMutex, binding *gnBinding) bool {
	if emit == nil {
		return false
	}
	if !event.IsData() {
		return emit(event)
	}
	mu.RLock()
	mapped := expandGNEvent(event, binding.idToGNs)
	mu.RUnlock()
	if len(mapped) > 0 {
		for _, mappedEvent := range mapped {
			if !emit(mappedEvent) {
				return false
			}
		}
		return true
	}
	if _, explicit := explicitIDs[event.Sample.ID]; explicit {
		return emit(event)
	}
	return true
}

func waitIDStream(ctx context.Context, stream IDStream) error {
	if stream == nil {
		return operror.Unsupported("subscription.waitIDStream", "ID stream is nil")
	}
	done := stream.Done()
	if done == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return stream.Err()
	}
}

func filterImplicitIDs(ids []model.PointID, explicit map[model.PointID]struct{}) []model.PointID {
	out := make([]model.PointID, 0, len(ids))
	for _, id := range ids {
		if _, ok := explicit[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

func uniqueGNs(gns []model.GN) []model.GN {
	out := make([]model.GN, 0, len(gns))
	seen := make(map[model.GN]struct{}, len(gns))
	for _, gn := range gns {
		if _, ok := seen[gn]; ok {
			continue
		}
		seen[gn] = struct{}{}
		out = append(out, gn)
	}
	return out
}

func uniquePointIDs(ids []model.PointID) []model.PointID {
	out := make([]model.PointID, 0, len(ids))
	seen := make(map[model.PointID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func mergePointIDs(left, right []model.PointID) []model.PointID {
	out := make([]model.PointID, 0, len(left)+len(right))
	seen := make(map[model.PointID]struct{}, len(left)+len(right))
	for _, group := range [][]model.PointID{left, right} {
		for _, id := range group {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

func pointIDSet(ids []model.PointID) map[model.PointID]struct{} {
	out := make(map[model.PointID]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}
