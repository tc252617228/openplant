//go:build safe_readonly

package tests

import (
	"context"
	"testing"
	"time"

	openplant "github.com/tc252617228/openplant"
	"github.com/tc252617228/openplant/internal/testenv"
)

func TestSafeReadonlyMetadataRealtimeArchiveStat(t *testing.T) {
	cfg := testenv.RequireSafeReadonly(t)
	client := newSafeReadonlyClient(t, cfg)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := resolveSafeReadonlyDB(t, ctx, client, cfg)
	point := resolveSafeReadonlyPoint(t, ctx, client, cfg, db)
	t.Logf("safe readonly target: db=%s point_id=%d point_gn=%s", db, point.ID, point.GN)

	t.Run("metadata", func(t *testing.T) {
		points, err := client.Metadata().FindPoints(ctx, openplant.MetadataPointQuery{
			DB:  db,
			IDs: []openplant.PointID{point.ID},
		})
		if err != nil {
			t.Fatalf("FindPoints failed: %v", err)
		}
		if len(points) != 1 || points[0].ID != point.ID {
			t.Fatalf("unexpected metadata points: %#v", points)
		}
	})

	t.Run("metadata_point_configs", func(t *testing.T) {
		configs, err := client.Metadata().FindPointConfigs(ctx, openplant.MetadataPointQuery{
			DB:  db,
			IDs: []openplant.PointID{point.ID},
		})
		if err != nil {
			handleOptionalReadonlyResult(t, "Metadata.FindPointConfigs", err)
			return
		}
		if len(configs) != 1 || configs[0].ID != point.ID {
			t.Fatalf("unexpected point configs: %#v", configs)
		}
	})

	t.Run("metadata_calculation_points", func(t *testing.T) {
		configs, err := client.Metadata().FindCalculationPointConfigs(ctx, openplant.MetadataPointQuery{
			DB:    db,
			Limit: 1,
		})
		if err != nil {
			handleOptionalReadonlyResult(t, "Metadata.FindCalculationPointConfigs", err)
			return
		}
		if len(configs) == 0 {
			t.Skip("no calculation point configs discovered")
		}
	})

	t.Run("nodes", func(t *testing.T) {
		nodes, err := client.Metadata().ListNodes(ctx, openplant.MetadataNodeQuery{
			DB:    db,
			Limit: 1,
		})
		if err != nil {
			t.Fatalf("ListNodes failed: %v", err)
		}
		if len(nodes) == 0 {
			t.Skip("no readable nodes discovered")
		}
	})

	t.Run("das", func(t *testing.T) {
		items, err := client.Metadata().ListDAS(ctx, openplant.MetadataDASQuery{
			DB:    db,
			Limit: 1,
		})
		if err != nil {
			handleOptionalReadonlyResult(t, "Metadata.ListDAS", err)
			return
		}
		if len(items) == 0 {
			t.Skip("no readable DAS records discovered")
		}
	})

	t.Run("devices", func(t *testing.T) {
		items, err := client.Metadata().ListDevices(ctx, openplant.MetadataDeviceQuery{
			DB:    db,
			Limit: 1,
		})
		if err != nil {
			handleOptionalReadonlyResult(t, "Metadata.ListDevices", err)
			return
		}
		if len(items) == 0 {
			t.Skip("no readable device records discovered")
		}
	})

	t.Run("realtime", func(t *testing.T) {
		samples, err := client.Realtime().Read(ctx, openplant.RealtimeReadRequest{
			DB:  db,
			IDs: []openplant.PointID{point.ID},
		})
		if err != nil {
			t.Fatalf("Realtime.Read failed: %v", err)
		}
		if len(samples) != 1 || samples[0].ID != point.ID {
			t.Fatalf("unexpected realtime samples: %#v", samples)
		}
	})

	t.Run("realtime_sql", func(t *testing.T) {
		samples, err := client.Realtime().QuerySQL(ctx, openplant.RealtimeReadRequest{
			DB:  db,
			GNs: []openplant.GN{point.GN},
		})
		if err != nil {
			t.Fatalf("Realtime.QuerySQL failed: %v", err)
		}
		if len(samples) == 0 || samples[0].GN != point.GN {
			t.Fatalf("unexpected realtime SQL samples: %#v", samples)
		}
	})

	t.Run("realtime_request", func(t *testing.T) {
		samples, err := client.Realtime().QueryRequest(ctx, openplant.RealtimeReadRequest{
			DB:  db,
			GNs: []openplant.GN{point.GN},
		})
		if err != nil {
			handleOptionalReadonlyResult(t, "Realtime.QueryRequest", err)
			return
		}
		if len(samples) == 0 || samples[0].GN != point.GN {
			t.Fatalf("unexpected realtime request samples: %#v", samples)
		}
	})

	begin := time.Now().Add(-1 * time.Hour)
	end := time.Now()
	timeRange := openplant.TimeRange{Begin: begin, End: end}

	t.Run("archive", func(t *testing.T) {
		_, err := client.Archive().QuerySQL(ctx, openplant.ArchiveQuery{
			DB:      db,
			IDs:     []openplant.PointID{point.ID},
			Range:   timeRange,
			Mode:    openplant.ModeRaw,
			Quality: openplant.QualityNone,
			Limit:   10,
		})
		if err != nil {
			t.Fatalf("Archive.Query failed: %v", err)
		}
	})

	t.Run("archive_request", func(t *testing.T) {
		_, err := client.Archive().QueryRequest(ctx, openplant.ArchiveQuery{
			DB:      db,
			IDs:     []openplant.PointID{point.ID},
			Range:   timeRange,
			Mode:    openplant.ModeRaw,
			Quality: openplant.QualityNone,
			Limit:   10,
		})
		if err != nil {
			t.Fatalf("Archive.QueryRequest failed: %v", err)
		}
	})

	t.Run("archive_native", func(t *testing.T) {
		_, err := client.Archive().QueryNative(ctx, openplant.ArchiveQuery{
			DB:      db,
			IDs:     []openplant.PointID{point.ID},
			Range:   timeRange,
			Mode:    openplant.ModeRaw,
			Quality: openplant.QualityNone,
			Limit:   10,
		})
		handleNativeReadonlyResult(t, "Archive.QueryNative", err)
	})

	t.Run("stat", func(t *testing.T) {
		_, err := client.Stat().QuerySQL(ctx, openplant.StatQuery{
			DB:       db,
			IDs:      []openplant.PointID{point.ID},
			Range:    timeRange,
			Mode:     openplant.ModeAvg,
			Interval: "1m",
			Quality:  openplant.QualityNone,
			Limit:    10,
		})
		if err != nil {
			t.Fatalf("Stat.Query failed: %v", err)
		}
	})

	t.Run("stat_request", func(t *testing.T) {
		_, err := client.Stat().QueryRequest(ctx, openplant.StatQuery{
			DB:       db,
			IDs:      []openplant.PointID{point.ID},
			Range:    timeRange,
			Mode:     openplant.ModeAvg,
			Interval: "1m",
			Quality:  openplant.QualityNone,
			Limit:    10,
		})
		if err != nil {
			handleOptionalReadonlyResult(t, "Stat.QueryRequest", err)
		}
	})

	t.Run("stat_native", func(t *testing.T) {
		_, err := client.Stat().QueryNative(ctx, openplant.StatQuery{
			DB:       db,
			IDs:      []openplant.PointID{point.ID},
			Range:    timeRange,
			Mode:     openplant.ModeAvg,
			Interval: "1m",
			Quality:  openplant.QualityNone,
			Limit:    10,
		})
		handleNativeReadonlyResult(t, "Stat.QueryNative", err)
	})

	t.Run("alarm", func(t *testing.T) {
		if _, err := client.Alarm().ActiveSQL(ctx, db, 10); err != nil {
			t.Fatalf("Alarm.ActiveSQL failed: %v", err)
		}
		if _, err := client.Alarm().HistorySQL(ctx, openplant.AlarmHistoryQuery{
			DB:    db,
			IDs:   []openplant.PointID{point.ID},
			Range: timeRange,
			Limit: 10,
		}); err != nil {
			t.Fatalf("Alarm.HistorySQL failed: %v", err)
		}
	})

	t.Run("system", func(t *testing.T) {
		samples, err := client.System().ReadSQL(ctx, openplant.SystemQuery{
			DB:      db,
			Metrics: openplant.DefaultSystemTrendMetrics(),
		})
		if err != nil {
			handleOptionalReadonlyResult(t, "System.ReadSQL", err)
			return
		}
		if len(samples) == 0 {
			t.Skip("no readable W3.SYS-style system metric points discovered")
		}
	})
}

func newSafeReadonlyClient(t testing.TB, cfg testenv.Config) *openplant.Client {
	t.Helper()
	client, err := openplant.New(
		openplant.WithEndpoint(cfg.Host, cfg.Port),
		openplant.WithCredentials(cfg.User, cfg.Pass),
		openplant.WithReadOnly(true),
		openplant.WithTimeouts(5*time.Second, 15*time.Second),
		openplant.WithPool(2, 2, time.Minute, 5*time.Minute),
	)
	if err != nil {
		t.Fatalf("New client failed: %v", err)
	}
	return client
}

func resolveSafeReadonlyDB(t testing.TB, ctx context.Context, client *openplant.Client, cfg testenv.Config) openplant.DatabaseName {
	t.Helper()
	if cfg.DB != "" {
		return openplant.DatabaseName(cfg.DB)
	}
	dbs, err := client.Metadata().ListDatabases(ctx)
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	for _, db := range dbs {
		if db.Name != "" {
			name := openplant.DatabaseName(db.Name)
			if err := name.Validate(); err == nil {
				return name
			}
		}
	}
	t.Skip("no readable database discovered; set OPENPLANT_TEST_DB")
	return ""
}

func resolveSafeReadonlyPoint(t testing.TB, ctx context.Context, client *openplant.Client, cfg testenv.Config, db openplant.DatabaseName) openplant.Point {
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
		if cfg.PointID > 0 || cfg.PointGN != "" {
			t.Fatalf("configured test point not found: id=%d gn=%s", cfg.PointID, cfg.PointGN)
		}
		t.Skip("no readable point discovered; set OPENPLANT_TEST_POINT_ID or OPENPLANT_TEST_POINT_GN")
	}
	return points[0]
}

func handleNativeReadonlyResult(t testing.TB, op string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if openplant.IsErrorKind(err, openplant.KindUnsupported) || openplant.IsErrorKind(err, openplant.KindServer) {
		t.Skipf("%s unavailable for this safe-readonly target: %v", op, err)
	}
	t.Fatalf("%s failed: %v", op, err)
}

func handleOptionalReadonlyResult(t testing.TB, op string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if openplant.IsErrorKind(err, openplant.KindUnsupported) || openplant.IsErrorKind(err, openplant.KindServer) {
		t.Skipf("%s unavailable for this safe-readonly target: %v", op, err)
	}
	t.Fatalf("%s failed: %v", op, err)
}
