package subscription

import (
	"slices"

	"github.com/tc252617228/openplant/model"
)

type gnBinding struct {
	gnToID  map[model.GN]model.PointID
	idToGNs map[model.PointID][]model.GN
	ids     []model.PointID
}

type rebindPlan struct {
	addIDs     []model.PointID
	removeIDs  []model.PointID
	changedGNs []model.GN
}

func buildGNBinding(gnToID map[model.GN]model.PointID) gnBinding {
	binding := gnBinding{
		gnToID:  cloneGNToID(gnToID),
		idToGNs: make(map[model.PointID][]model.GN, len(gnToID)),
	}
	seenIDs := make(map[model.PointID]struct{}, len(gnToID))
	for gn, id := range gnToID {
		if id <= 0 {
			continue
		}
		binding.idToGNs[id] = append(binding.idToGNs[id], gn)
		if _, ok := seenIDs[id]; ok {
			continue
		}
		seenIDs[id] = struct{}{}
		binding.ids = append(binding.ids, id)
	}
	slices.Sort(binding.ids)
	for id := range binding.idToGNs {
		slices.Sort(binding.idToGNs[id])
	}
	return binding
}

func planGNRebind(current, target map[model.GN]model.PointID) rebindPlan {
	currentBinding := buildGNBinding(current)
	targetBinding := buildGNBinding(target)
	plan := rebindPlan{
		addIDs:     diffIDs(targetBinding.ids, currentBinding.ids),
		removeIDs:  diffIDs(currentBinding.ids, targetBinding.ids),
		changedGNs: changedGNs(currentBinding.gnToID, targetBinding.gnToID),
	}
	return plan
}

func cloneGNToID(src map[model.GN]model.PointID) map[model.GN]model.PointID {
	dst := make(map[model.GN]model.PointID, len(src))
	for gn, id := range src {
		dst[gn] = id
	}
	return dst
}

func diffIDs(left, right []model.PointID) []model.PointID {
	if len(left) == 0 {
		return nil
	}
	rightSet := make(map[model.PointID]struct{}, len(right))
	for _, id := range right {
		rightSet[id] = struct{}{}
	}
	out := make([]model.PointID, 0, len(left))
	for _, id := range left {
		if _, ok := rightSet[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

func changedGNs(current, target map[model.GN]model.PointID) []model.GN {
	seen := make(map[model.GN]struct{}, len(current)+len(target))
	out := make([]model.GN, 0)
	for gn, currentID := range current {
		if targetID, ok := target[gn]; ok && targetID != currentID {
			out = append(out, gn)
			seen[gn] = struct{}{}
		}
	}
	for gn := range target {
		if _, ok := current[gn]; !ok {
			if _, seen := seen[gn]; !seen {
				out = append(out, gn)
			}
		}
	}
	slices.Sort(out)
	return out
}

func expandGNEvent(event Event, idToGNs map[model.PointID][]model.GN) []Event {
	if !event.IsData() {
		return []Event{event}
	}
	gns := idToGNs[event.Sample.ID]
	if len(gns) == 0 {
		return nil
	}
	out := make([]Event, 0, len(gns))
	for _, gn := range gns {
		sample := event.Sample
		sample.GN = gn
		out = append(out, Event{Kind: EventData, Sample: sample})
	}
	return out
}
