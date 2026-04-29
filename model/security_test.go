package model

import "testing"

func TestSecurityGroups(t *testing.T) {
	groups := SecurityGroupsFromBytes([]byte{0b00000011, 0, 0, 0})
	if !groups.Has(0) || !groups.Has(1) {
		t.Fatalf("expected groups 0 and 1 to be set")
	}
	if groups.Has(2) || groups.Has(-1) || groups.Has(SecurityGroupCount) {
		t.Fatalf("unexpected group membership")
	}

	var ok bool
	groups, ok = groups.With(9)
	if !ok || !groups.Has(9) {
		t.Fatalf("expected group 9 to be set")
	}
	groups, ok = groups.With(SecurityGroupCount)
	if ok {
		t.Fatalf("out of range group should not be set")
	}
	groups, ok = groups.Without(1)
	if !ok || groups.Has(1) {
		t.Fatalf("expected group 1 to be cleared")
	}

	bytes := groups.Bytes()
	bytes[0] = 0
	if !groups.Has(0) {
		t.Fatalf("Bytes must return a copy")
	}
}
