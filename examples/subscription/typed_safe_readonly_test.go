//go:build safe_readonly

package main

import (
	"context"
	"testing"
	"time"

	openplant "github.com/tc252617228/openplant"
	"github.com/tc252617228/openplant/internal/testenv"
)

func TestSafeReadonlyTypedSubscriptionExamples(t *testing.T) {
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
	db := exampleDB(t, ctx, client, cfg)
	point := examplePoint(t, ctx, client, cfg, db)

	t.Run("point", func(t *testing.T) {
		stream, err := SubscribePointRecordsFromService(ctx, client.Subscription(), PointTableSubscriptionRequest{
			DB:       db,
			IDs:      []openplant.PointID{point.ID},
			Snapshot: true,
		})
		if err != nil {
			t.Fatalf("SubscribePointRecordsFromService failed: %v", err)
		}
		defer stream.Close()
		event := receiveTypedEventWithin(t, stream.Events(), 10*time.Second)
		if event.Err != nil {
			t.Fatalf("point typed event err=%v", event.Err)
		}
		if event.Record.ID != point.ID || event.Record.GN != point.GN {
			t.Fatalf("unexpected point record: %#v want id=%d gn=%s", event.Record, point.ID, point.GN)
		}
	})

	t.Run("alarm", func(t *testing.T) {
		alarms, err := client.Alarm().ActiveSQL(ctx, db, 1)
		if err != nil {
			t.Fatalf("Alarm.ActiveSQL failed: %v", err)
		}
		if len(alarms) == 0 {
			t.Skip("no active alarm row available for typed subscription example")
		}
		stream, err := SubscribeAlarmRecordsFromService(ctx, client.Subscription(), AlarmTableSubscriptionRequest{
			DB:       db,
			IDs:      []openplant.PointID{alarms[0].ID},
			Snapshot: true,
		})
		if err != nil {
			t.Fatalf("SubscribeAlarmRecordsFromService failed: %v", err)
		}
		defer stream.Close()
		event := receiveTypedEventWithin(t, stream.Events(), 10*time.Second)
		if event.Err != nil {
			t.Fatalf("alarm typed event err=%v", event.Err)
		}
		if event.Record.ID != alarms[0].ID || event.Record.GN != alarms[0].GN {
			t.Fatalf("unexpected alarm record: %#v want id=%d gn=%s", event.Record, alarms[0].ID, alarms[0].GN)
		}
	})
}

func exampleDB(t testing.TB, ctx context.Context, client *openplant.Client, cfg testenv.Config) openplant.DatabaseName {
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

func examplePoint(t testing.TB, ctx context.Context, client *openplant.Client, cfg testenv.Config, db openplant.DatabaseName) openplant.Point {
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
