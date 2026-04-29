package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	openplant "github.com/tc252617228/openplant"
	openplantsql "github.com/tc252617228/openplant/sql"
)

type pointRow struct {
	ID int32  `openplant:"ID"`
	GN string `openplant:"GN"`
	RT int8   `openplant:"RT"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := openplant.New(openplant.OptionsFromEnv("OPENPLANT_TEST")...)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	db := envDatabase()
	pointID := envPointID()

	points, err := client.Metadata().FindPoints(ctx, openplant.MetadataPointQuery{
		DB:    db,
		IDs:   []openplant.PointID{pointID},
		Limit: 1,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("metadata points: %d\n", len(points))

	realtime, err := client.Realtime().Read(ctx, openplant.RealtimeReadRequest{
		DB:  db,
		IDs: []openplant.PointID{pointID},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("realtime samples: %d\n", len(realtime))

	pointTable, err := openplantsql.QualifiedTable(string(db), "Point")
	if err != nil {
		log.Fatal(err)
	}
	result, err := client.SQL().Query(ctx, fmt.Sprintf(`SELECT "ID","GN","RT" FROM %s WHERE "ID"=%d LIMIT 1`, pointTable, pointID))
	if err != nil {
		log.Fatal(err)
	}
	rows, err := openplantsql.ScanRows[pointRow](result.Rows)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("scanned rows: %d\n", len(rows))

	systemSamples, err := client.System().ReadSQL(ctx, openplant.SystemQuery{
		DB:      db,
		Metrics: openplant.DefaultSystemTrendMetrics(),
	})
	if err != nil {
		fmt.Printf("system metrics unavailable: %v\n", err)
	} else {
		fmt.Printf("system metric samples: %d\n", len(systemSamples))
	}
	if template, ok := openplant.LookupSystemPointTemplate(openplant.SystemMetricRate, db); ok {
		fmt.Printf("system rate formula refs: %v\n", openplant.CalcFormulaReferences(template.Expression))
	}

	now := time.Now()
	query := openplant.ArchiveQuery{
		DB:    db,
		IDs:   []openplant.PointID{pointID},
		Range: openplant.TimeRange{Begin: now.Add(-time.Hour), End: now},
		Mode:  openplant.ModeRaw,
		Limit: 5,
	}
	err = client.Archive().StreamNative(ctx, query, func(sample openplant.Sample) bool {
		fmt.Printf("archive sample: id=%d time=%s type=%s\n", sample.ID, sample.Time.Format(time.RFC3339), sample.Type)
		return true
	})
	if err != nil {
		log.Fatal(err)
	}
}

func envDatabase() openplant.DatabaseName {
	if value := os.Getenv("OPENPLANT_TEST_DB"); value != "" {
		return openplant.DatabaseName(value)
	}
	return "W3"
}

func envPointID() openplant.PointID {
	if value := os.Getenv("OPENPLANT_TEST_POINT_ID"); value != "" {
		id, err := strconv.Atoi(value)
		if err == nil && id > 0 {
			return openplant.PointID(id)
		}
	}
	return 1001
}
