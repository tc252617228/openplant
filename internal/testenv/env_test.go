package testenv

import (
	"testing"

	"github.com/tc252617228/openplant/model"
)

func TestLoadMarksInvalidPortAndPointID(t *testing.T) {
	t.Setenv("OPENPLANT_ENVTEST_PORT", "not-a-port")
	t.Setenv("OPENPLANT_ENVTEST_POINT_ID", "not-a-point-id")

	cfg := Load("OPENPLANT_ENVTEST")
	if cfg.Port != 0 {
		t.Fatalf("invalid port loaded as %d, want 0", cfg.Port)
	}
	if cfg.PointID != model.PointID(-1) {
		t.Fatalf("invalid point ID loaded as %d, want -1", cfg.PointID)
	}
}
