package openplant

import (
	"context"
	"testing"
)

func TestDialRejectsInvalidAddressBeforeNetwork(t *testing.T) {
	if _, err := Dial(context.Background(), "invalid-address", nil); err == nil {
		t.Fatalf("expected invalid dial address to be rejected")
	}
}
