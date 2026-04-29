//go:build safe_readonly

package tests

import (
	"testing"

	"github.com/tc252617228/openplant/internal/testenv"
)

func TestSafeReadonlyEnvironment(t *testing.T) {
	cfg := testenv.RequireSafeReadonly(t)
	if cfg.Port <= 0 {
		t.Fatalf("invalid port %d", cfg.Port)
	}
}
