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

	if os.Getenv("OPENPLANT_TEST_TABLE_SUBSCRIBE") == "1" {
		if err := runTypedPointTableSnapshot(ctx, client); err != nil {
			log.Fatal(err)
		}
		return
	}

	stream, err := client.Subscription().Subscribe(ctx, openplant.SubscribeRequest{
		DB:  envDatabase(),
		IDs: []openplant.PointID{envPointID()},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	for event := range stream.Events() {
		switch event.Kind {
		case openplant.SubscribeEventData:
			fmt.Printf("sample id=%d time=%s\n", event.Sample.ID, event.Sample.Time.Format(time.RFC3339))
		case openplant.SubscribeEventReconnecting, openplant.SubscribeEventReconnected:
			fmt.Printf("status=%s\n", event.Kind)
		case openplant.SubscribeEventError:
			log.Fatal(event.Err)
		}
	}
	if err := stream.Err(); err != nil {
		log.Fatal(err)
	}
}

func runTypedPointTableSnapshot(ctx context.Context, client *openplant.Client) error {
	stream, err := SubscribePointRecordsFromService(ctx, client.Subscription(), PointTableSubscriptionRequest{
		DB:       envDatabase(),
		IDs:      []openplant.PointID{envPointID()},
		Snapshot: true,
	})
	if err != nil {
		return err
	}
	defer stream.Close()
	select {
	case event, ok := <-stream.Events():
		if !ok {
			return stream.Err()
		}
		if event.Err != nil {
			return event.Err
		}
		fmt.Printf("point id=%d gn=%s name=%s type=%s\n", event.Record.ID, event.Record.GN, event.Record.Name, event.Record.Type)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
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
