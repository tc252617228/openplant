package model

import (
	"strings"
	"testing"
)

func TestPointSelectorValidateBoundedInDatabase(t *testing.T) {
	valid := PointSelector{
		IDs: []PointID{1001},
		GNs: []GN{"W3.N.P1"},
	}
	if err := valid.ValidateBoundedInDatabase("W3"); err != nil {
		t.Fatalf("valid selector rejected: %v", err)
	}

	crossDB := PointSelector{GNs: []GN{"X.N.P1"}}
	err := crossDB.ValidateBoundedInDatabase("W3")
	if err == nil || !strings.Contains(err.Error(), `belongs to database "X", not "W3"`) {
		t.Fatalf("expected database mismatch, got %v", err)
	}
}

func TestGNValidateInDatabaseRejectsUnqualifiedGN(t *testing.T) {
	err := GN("N.P1").ValidateInDatabase("W3")
	if err == nil || !strings.Contains(err.Error(), `belongs to database "N", not "W3"`) {
		t.Fatalf("expected unqualified GN to be rejected, got %v", err)
	}
}
