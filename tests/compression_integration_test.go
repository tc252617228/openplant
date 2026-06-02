//go:build compression_integration

package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	openplant "github.com/tc252617228/openplant"
	"github.com/tc252617228/openplant/internal/testenv"
)

func TestCompressionModesAgainstReadonlyTarget(t *testing.T) {
	cfg := testenv.RequireSafeReadonly(t)

	discovery := newCompressionReadonlyClient(t, cfg, openplant.CompressionNone)
	defer discovery.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := resolveCompressionDB(t, ctx, discovery, cfg)
	point := resolveCompressionPoint(t, ctx, discovery, cfg, db)
	t.Logf("compression readonly target: db=%s point_id=%d point_gn=%s", db, point.ID, point.GN)

	for _, mode := range []openplant.CompressionMode{
		openplant.CompressionFrame,
		openplant.CompressionBlock,
	} {
		mode := mode
		t.Run(mode.String(), func(t *testing.T) {
			client := newCompressionReadonlyClient(t, cfg, mode)
			defer client.Close()

			query := fmt.Sprintf("SELECT ID,GN FROM %s.Point WHERE ID=%d", db, point.ID)
			query += strings.Repeat(" ", 8192)
			result, err := client.SQL().Query(ctx, query)
			if err != nil {
				t.Fatalf("compressed SQL query failed: %v", err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("rows=%d want 1", len(result.Rows))
			}
		})
	}
}

func TestLowLevelConnAgainstReadonlyTarget(t *testing.T) {
	cfg := testenv.RequireSafeReadonly(t)
	opts := openplant.DefaultOptions()
	opts.Host = cfg.Host
	opts.Port = cfg.Port
	opts.User = cfg.User
	opts.Password = cfg.Pass
	opts.Compression = openplant.CompressionFrame

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := openplant.Dial(ctx, "", &opts)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()
	if got := conn.CompressionMode(); got != openplant.CompressionFrame {
		t.Fatalf("compression mode=%s want frame", got)
	}
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func newCompressionReadonlyClient(t testing.TB, cfg testenv.Config, mode openplant.CompressionMode) *openplant.Client {
	t.Helper()
	client, err := openplant.New(
		openplant.WithEndpoint(cfg.Host, cfg.Port),
		openplant.WithCredentials(cfg.User, cfg.Pass),
		openplant.WithReadOnly(true),
		openplant.WithCompression(mode),
		openplant.WithTimeouts(5*time.Second, 20*time.Second),
		openplant.WithPool(1, 1, time.Minute, 5*time.Minute),
	)
	if err != nil {
		t.Fatalf("New compression client failed: %v", err)
	}
	return client
}

func resolveCompressionDB(t testing.TB, ctx context.Context, client *openplant.Client, cfg testenv.Config) openplant.DatabaseName {
	t.Helper()
	if cfg.DB != "" {
		return openplant.DatabaseName(cfg.DB)
	}
	dbs, err := client.Metadata().ListDatabases(ctx)
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	for _, db := range dbs {
		if db.Name == "" {
			continue
		}
		name := openplant.DatabaseName(db.Name)
		if err := name.Validate(); err == nil {
			return name
		}
	}
	t.Skip("no readable database discovered; set OPENPLANT_TEST_DB")
	return ""
}

func resolveCompressionPoint(t testing.TB, ctx context.Context, client *openplant.Client, cfg testenv.Config, db openplant.DatabaseName) openplant.Point {
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
		t.Skip("no readable point discovered; set OPENPLANT_TEST_POINT_ID or OPENPLANT_TEST_POINT_GN")
	}
	return points[0]
}
