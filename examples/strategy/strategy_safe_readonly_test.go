//go:build safe_readonly

package main

import (
	"context"
	"testing"
	"time"

	openplant "github.com/tc252617228/openplant"
	"github.com/tc252617228/openplant/internal/testenv"
)

func TestSafeReadonlyArchiveStrategyExample(t *testing.T) {
	cfg := testenv.RequireSafeReadonly(t)
	client, err := openplant.New(
		openplant.WithEndpoint(cfg.Host, cfg.Port),
		openplant.WithCredentials(cfg.User, cfg.Pass),
		openplant.WithReadOnly(true),
		openplant.WithTimeouts(5*time.Second, 15*time.Second),
	)
	if err != nil {
		t.Fatalf("New client failed: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := strategyExampleDB(t, ctx, client, cfg)
	point := strategyExamplePoint(t, ctx, client, cfg, db)
	end := time.Now()
	result, err := QueryArchiveWithStrategy(ctx, client.Archive(), openplant.ArchiveQuery{
		DB:      db,
		IDs:     []openplant.PointID{point.ID},
		Range:   openplant.TimeRange{Begin: end.Add(-1 * time.Hour), End: end},
		Mode:    openplant.ModeRaw,
		Quality: openplant.QualityNone,
		Limit:   10,
	}, ArchiveStrategyPolicy{
		Layers: []ArchiveLayer{ArchiveLayerRequest, ArchiveLayerSQL},
	})
	if err != nil {
		t.Fatalf("QueryArchiveWithStrategy failed: trace=%#v err=%v", result.Trace, err)
	}
	if len(result.Trace) == 0 || result.Trace[len(result.Trace)-1].Err != nil {
		t.Fatalf("strategy trace did not record success: %#v", result.Trace)
	}
}

func strategyExampleDB(t testing.TB, ctx context.Context, client *openplant.Client, cfg testenv.Config) openplant.DatabaseName {
	t.Helper()
	if cfg.DB != "" {
		return openplant.DatabaseName(cfg.DB)
	}
	dbs, err := client.Metadata().ListDatabases(ctx)
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	if len(dbs) == 0 {
		t.Skip("no readable database discovered")
	}
	return openplant.DatabaseName(dbs[0].Name)
}

func strategyExamplePoint(t testing.TB, ctx context.Context, client *openplant.Client, cfg testenv.Config, db openplant.DatabaseName) openplant.Point {
	t.Helper()
	query := openplant.MetadataPointQuery{DB: db}
	switch {
	case cfg.PointID > 0:
		query.IDs = []openplant.PointID{cfg.PointID}
	case cfg.PointGN != "":
		query.GNs = []openplant.GN{cfg.PointGN}
	default:
		query.Limit = 1
	}
	points, err := client.Metadata().FindPoints(ctx, query)
	if err != nil {
		t.Fatalf("FindPoints failed: %v", err)
	}
	if len(points) == 0 {
		t.Skip("no readable point discovered")
	}
	return points[0]
}
