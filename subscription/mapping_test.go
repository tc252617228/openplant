package subscription

import (
	"slices"
	"testing"

	"github.com/tc252617228/openplant/model"
)

func TestBuildGNBindingDeduplicatesIDsAndKeepsAliases(t *testing.T) {
	binding := buildGNBinding(map[model.GN]model.PointID{
		"W3.N.P1":       1001,
		"W3.N.P1_ALIAS": 1001,
		"W3.N.P2":       1002,
	})

	if !slices.Equal(binding.ids, []model.PointID{1001, 1002}) {
		t.Fatalf("ids=%v", binding.ids)
	}
	if !slices.Equal(binding.idToGNs[1001], []model.GN{"W3.N.P1", "W3.N.P1_ALIAS"}) {
		t.Fatalf("idToGNs[1001]=%v", binding.idToGNs[1001])
	}
}

func TestPlanGNRebindDetectsDriftAndNewGN(t *testing.T) {
	plan := planGNRebind(
		map[model.GN]model.PointID{
			"W3.N.P1": 1001,
			"W3.N.P2": 1002,
		},
		map[model.GN]model.PointID{
			"W3.N.P1": 2001,
			"W3.N.P2": 1002,
			"W3.N.P3": 3003,
		},
	)

	if !slices.Equal(plan.addIDs, []model.PointID{2001, 3003}) {
		t.Fatalf("addIDs=%v", plan.addIDs)
	}
	if !slices.Equal(plan.removeIDs, []model.PointID{1001}) {
		t.Fatalf("removeIDs=%v", plan.removeIDs)
	}
	if !slices.Equal(plan.changedGNs, []model.GN{"W3.N.P1", "W3.N.P3"}) {
		t.Fatalf("changedGNs=%v", plan.changedGNs)
	}
}

func TestPlanGNRebindDoesNotRemoveIDStillUsedByAlias(t *testing.T) {
	plan := planGNRebind(
		map[model.GN]model.PointID{
			"W3.N.P1":       1001,
			"W3.N.P1_ALIAS": 1001,
		},
		map[model.GN]model.PointID{
			"W3.N.P1":       2001,
			"W3.N.P1_ALIAS": 1001,
		},
	)

	if !slices.Equal(plan.addIDs, []model.PointID{2001}) {
		t.Fatalf("addIDs=%v", plan.addIDs)
	}
	if len(plan.removeIDs) != 0 {
		t.Fatalf("removeIDs=%v, want none because alias still uses old ID", plan.removeIDs)
	}
	if !slices.Equal(plan.changedGNs, []model.GN{"W3.N.P1"}) {
		t.Fatalf("changedGNs=%v", plan.changedGNs)
	}
}

func TestExpandGNEventClonesSampleForAliases(t *testing.T) {
	events := expandGNEvent(Event{
		Kind:   EventData,
		Sample: model.Sample{ID: 1001, Value: model.R8(12.5)},
	}, map[model.PointID][]model.GN{
		1001: {"W3.N.P1", "W3.N.P1_ALIAS"},
	})

	if len(events) != 2 {
		t.Fatalf("events=%d want 2", len(events))
	}
	if events[0].Sample.GN != "W3.N.P1" || events[1].Sample.GN != "W3.N.P1_ALIAS" {
		t.Fatalf("events=%#v", events)
	}
	if events[0].Sample.ID != 1001 || events[1].Sample.ID != 1001 {
		t.Fatalf("events=%#v", events)
	}
	if events[0].Kind != EventData || events[1].Kind != EventData {
		t.Fatalf("events=%#v", events)
	}
}

func TestExpandGNEventForwardsErrors(t *testing.T) {
	errEvent := Event{Kind: EventError, Err: errTestSubscription}
	events := expandGNEvent(errEvent, nil)
	if len(events) != 1 || events[0].Kind != EventError || events[0].Err != errTestSubscription {
		t.Fatalf("events=%#v", events)
	}
}

func TestExpandGNEventForwardsStatusEvents(t *testing.T) {
	statusEvent := Event{Kind: EventReconnected}
	events := expandGNEvent(statusEvent, map[model.PointID][]model.GN{
		0: {"W3.N.SHOULD_NOT_APPLY"},
	})
	if len(events) != 1 || events[0].Kind != EventReconnected {
		t.Fatalf("events=%#v", events)
	}
}

var errTestSubscription = &testSubscriptionError{}

type testSubscriptionError struct{}

func (e *testSubscriptionError) Error() string { return "test subscription error" }
