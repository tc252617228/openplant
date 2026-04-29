//go:build mutation

package tests

import (
	"testing"
	"time"

	openplant "github.com/tc252617228/openplant"
	"github.com/tc252617228/openplant/internal/testenv"
)

func TestMutationEnvironmentIsExplicitAndWritable(t *testing.T) {
	cfg := testenv.RequireMutation(t)
	client, err := openplant.New(
		openplant.WithEndpoint(cfg.Host, cfg.Port),
		openplant.WithCredentials(cfg.User, cfg.Pass),
		openplant.WithReadOnly(false),
		openplant.WithTimeouts(5*time.Second, 15*time.Second),
		openplant.WithPool(1, 1, time.Minute, 5*time.Minute),
	)
	if err != nil {
		t.Fatalf("New mutation client failed: %v", err)
	}
	defer client.Close()
	if client.Options().ReadOnly {
		t.Fatalf("mutation client must be writable")
	}
	if cfg.Prefix == "" || cfg.DB == "" {
		t.Fatalf("mutation tests require isolated database and prefix")
	}
}
