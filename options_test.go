package openplant

import (
	"testing"
	"time"
)

func TestDefaultOptionsAreValidAndReadonly(t *testing.T) {
	cfg := DefaultOptions()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default options invalid: %v", err)
	}
	if !cfg.ReadOnly {
		t.Fatalf("default client must be readonly")
	}
	if cfg.ChunkSize != 200 {
		t.Fatalf("default chunk size = %d, want 200", cfg.ChunkSize)
	}
	if cfg.MetadataCacheTTL != 5*time.Minute || cfg.MetadataCacheMaxEntries != 10000 || cfg.DisableMetadataCache {
		t.Fatalf("unexpected metadata cache defaults: ttl=%s max=%d disabled=%v", cfg.MetadataCacheTTL, cfg.MetadataCacheMaxEntries, cfg.DisableMetadataCache)
	}
}

func TestMetadataCacheTTLRequiresPositiveDurationUnlessDisabled(t *testing.T) {
	if _, err := New(WithMetadataCacheTTL(0)); err == nil {
		t.Fatalf("expected zero metadata cache TTL to be rejected")
	}
	if _, err := New(WithMetadataCacheTTL(0), WithMetadataCacheDisabled(true)); err != nil {
		t.Fatalf("disabled metadata cache should allow zero TTL: %v", err)
	}
}

func TestMetadataCacheMaxEntriesRequiresPositiveLimitUnlessDisabled(t *testing.T) {
	if _, err := New(WithMetadataCacheMaxEntries(0)); err == nil {
		t.Fatalf("expected zero metadata cache max entries to be rejected")
	}
	if _, err := New(WithMetadataCacheMaxEntries(0), WithMetadataCacheDisabled(true)); err != nil {
		t.Fatalf("disabled metadata cache should allow zero max entries: %v", err)
	}
}
