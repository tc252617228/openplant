package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	openplant "github.com/tc252617228/openplant"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	client, err := openplant.New(openplant.OptionsFromEnv("OPENPLANT_TEST")...)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	end := time.Now()
	begin := end.Add(-1 * time.Hour)
	result, err := QueryArchiveWithStrategy(ctx, client.Archive(), openplant.ArchiveQuery{
		DB:      envDatabase(),
		IDs:     []openplant.PointID{envPointID()},
		Range:   openplant.TimeRange{Begin: begin, End: end},
		Mode:    openplant.ModeRaw,
		Quality: openplant.QualityNone,
		Limit:   10,
	}, ArchiveStrategyPolicy{
		Layers: []ArchiveLayer{ArchiveLayerNative, ArchiveLayerRequest, ArchiveLayerSQL},
	})
	for _, attempt := range result.Trace {
		if attempt.Err != nil {
			fmt.Printf("archive layer=%s took=%s err=%v\n", attempt.Layer, attempt.Took, attempt.Err)
			continue
		}
		fmt.Printf("archive layer=%s took=%s ok\n", attempt.Layer, attempt.Took)
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("samples=%d\n", len(result.Samples))
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
