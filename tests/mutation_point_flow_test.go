//go:build mutation

package tests

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	openplant "github.com/tc252617228/openplant"
	"github.com/tc252617228/openplant/internal/testenv"
)

type mutationCleanup struct {
	db             openplant.DatabaseName
	nodeID         openplant.NodeID
	pointID        openplant.PointID
	archiveBegin   time.Time
	archiveEnd     time.Time
	archiveWritten bool
}

func TestMutationRunsIsolatedPointLifecycle(t *testing.T) {
	cfg := testenv.RequireMutation(t)
	client := newMutationClient(t, cfg)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db := openplant.DatabaseName(cfg.DB)
	cleanupMutationPrefix(t, ctx, client, db, cfg.Prefix)

	token := strings.ToUpper(fmt.Sprintf("%x", time.Now().UnixNano()))
	if len(token) > 8 {
		token = token[len(token)-8:]
	}
	nodePN := mutationName(cfg.Prefix, "N", token, 24)
	pointPN := mutationName(cfg.Prefix, "P", token, 32)
	cleanup := mutationCleanup{db: db}
	defer func() {
		cleanup.run(t, client)
		assertMutationPrefixEmpty(t, ctx, client, db, cfg.Prefix)
	}()

	t.Logf("mutation target: db=%s node=%s point=%s", db, nodePN, pointPN)

	node := createMutationNode(t, ctx, client, db, nodePN)
	cleanup.nodeID = node.ID

	point := createMutationPoint(t, ctx, client, db, node, pointPN)
	cleanup.pointID = point.ID
	if point.Type != openplant.TypeR8 {
		t.Fatalf("created point type = %v, want R8", point.Type)
	}
	if !point.Archived {
		t.Fatalf("created point should have archive enabled")
	}

	updatedDescription := "openplant sdk mutation point updated"
	updateMutationPointDescription(t, ctx, client, db, point.ID, updatedDescription)
	waitForMutationPointDescription(t, ctx, client, db, point.GN, updatedDescription)

	rtValue := 42.125
	writeRealtimeValue(t, ctx, client, db, point.ID, rtValue)
	waitForRealtimeValue(t, ctx, client, db, point.ID, rtValue)

	requestSamples, err := client.Realtime().QueryRequest(ctx, openplant.RealtimeReadRequest{
		DB:  db,
		GNs: []openplant.GN{point.GN},
	})
	if err != nil {
		t.Fatalf("Realtime.QueryRequest by GN failed: %v", err)
	}
	if len(requestSamples) == 0 || requestSamples[0].GN != point.GN {
		t.Fatalf("unexpected realtime request samples: %#v", requestSamples)
	}

	stream, err := client.Subscription().Subscribe(ctx, openplant.SubscribeRequest{
		DB:  db,
		IDs: []openplant.PointID{point.ID},
	})
	if err != nil {
		t.Fatalf("Subscription.Subscribe failed: %v", err)
	}
	defer stream.Close()
	waitForSubscriptionValue(t, ctx, client, stream, db, point.ID, 43.5)
	stream.Close()

	archiveValue := 77.5
	archiveTime := time.Now().Add(-30 * time.Second).Truncate(time.Second)
	if err := client.Archive().WriteNative(ctx, openplant.ArchiveWriteRequest{
		DB: db,
		Samples: []openplant.Sample{{
			ID:     point.ID,
			Type:   openplant.TypeR8,
			Time:   archiveTime,
			Status: 0,
			Value:  openplant.R8(archiveValue),
		}},
	}); err != nil {
		t.Fatalf("Archive.WriteNative failed: %v", err)
	}
	cleanup.archiveWritten = true
	cleanup.archiveBegin = archiveTime.Add(-5 * time.Second)
	cleanup.archiveEnd = archiveTime.Add(5 * time.Second)
	waitForArchiveValue(t, ctx, client, db, point.ID, archiveTime, archiveValue)
}

func newMutationClient(t testing.TB, cfg testenv.Config) *openplant.Client {
	t.Helper()
	client, err := openplant.New(
		openplant.WithEndpoint(cfg.Host, cfg.Port),
		openplant.WithCredentials(cfg.User, cfg.Pass),
		openplant.WithReadOnly(false),
		openplant.WithUnsafeSQL(true),
		openplant.WithTimeouts(5*time.Second, 20*time.Second),
		openplant.WithPool(2, 2, time.Minute, 5*time.Minute),
	)
	if err != nil {
		t.Fatalf("New mutation client failed: %v", err)
	}
	return client
}

func createMutationNode(t testing.TB, ctx context.Context, client *openplant.Client, db openplant.DatabaseName, nodePN string) openplant.Node {
	t.Helper()
	req, err := openplant.BuildNodeInsert(db, []openplant.Node{{
		Name:        nodePN,
		Description: "openplant sdk mutation node",
	}})
	if err != nil {
		t.Fatalf("BuildNodeInsert failed: %v", err)
	}
	if err := client.Admin().MutateTable(ctx, req); err != nil {
		t.Fatalf("Admin.MutateTable Node insert failed: %v", err)
	}
	return waitForMutationNode(t, ctx, client, db, nodePN)
}

func createMutationPoint(t testing.TB, ctx context.Context, client *openplant.Client, db openplant.DatabaseName, node openplant.Node, pointPN string) openplant.Point {
	t.Helper()
	req, err := openplant.BuildPointConfigInsert(db, []openplant.PointConfig{{
		NodeID:      node.ID,
		Source:      openplant.SourceDAS,
		Type:        openplant.TypeR8,
		Name:        pointPN,
		Description: "openplant sdk mutation point",
		Unit:        "unit",
		Format:      2,
		Archived:    true,
		RangeLower:  0,
		RangeUpper:  100,
		ScaleFactor: 1,
	}})
	if err != nil {
		t.Fatalf("BuildPointConfigInsert failed: %v", err)
	}
	if err := client.Admin().MutateTable(ctx, req); err != nil {
		t.Fatalf("Admin.MutateTable Point insert failed: %v", err)
	}
	return waitForMutationPoint(t, ctx, client, db, node.GN, pointPN)
}

func updateMutationPointDescription(t testing.TB, ctx context.Context, client *openplant.Client, db openplant.DatabaseName, id openplant.PointID, description string) {
	t.Helper()
	req := openplant.TableMutation{
		DB:     db,
		Table:  "Point",
		Action: openplant.MutationUpdate,
		Filters: []openplant.MutationFilter{{
			Left:     "ID",
			Operator: openplant.FilterEQ,
			Right:    fmt.Sprintf("%d", id),
			Relation: openplant.FilterAnd,
		}},
		Columns: []openplant.MutationColumn{{Name: "ED", Type: openplant.ColumnString}},
		Rows:    []openplant.MutationRow{{"ED": description}},
	}
	if err := client.Admin().MutateTable(ctx, req); err != nil {
		t.Fatalf("Admin.MutateTable Point update failed: %v", err)
	}
}

func writeRealtimeValue(t testing.TB, ctx context.Context, client *openplant.Client, db openplant.DatabaseName, id openplant.PointID, value float64) {
	t.Helper()
	if err := client.Realtime().WriteNative(ctx, openplant.RealtimeWriteRequest{
		DB: db,
		Values: []openplant.RealtimeWrite{{
			ID:     id,
			Type:   openplant.TypeR8,
			Time:   time.Now().Truncate(time.Second),
			Status: 0,
			Value:  openplant.R8(value),
		}},
	}); err != nil {
		t.Fatalf("Realtime.WriteNative failed: %v", err)
	}
}

func (c *mutationCleanup) run(t testing.TB, client *openplant.Client) {
	t.Helper()
	if c.db == "" || client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if c.pointID > 0 && c.archiveWritten {
		err := client.Archive().DeleteNative(ctx, openplant.ArchiveDeleteRequest{
			DB:    c.db,
			IDs:   []openplant.PointID{c.pointID},
			Range: openplant.TimeRange{Begin: c.archiveBegin, End: c.archiveEnd},
		})
		if err != nil {
			t.Logf("cleanup archive native delete for point %d unavailable, using explicit SQL cleanup: %v", c.pointID, err)
			if err := deleteMutationArchiveBySQL(ctx, client, c.db, c.pointID, c.archiveBegin, c.archiveEnd); err != nil {
				t.Errorf("cleanup archive samples for point %d failed: %v", c.pointID, err)
			}
		}
	}
	if c.pointID > 0 {
		deleteErr, confirmErr := deleteMutationPointAndConfirm(ctx, client, c.db, c.pointID)
		if deleteErr != nil {
			t.Logf("cleanup point %d delete returned before confirmation: %v", c.pointID, deleteErr)
		}
		if confirmErr != nil {
			t.Errorf("cleanup point %d failed: %v", c.pointID, confirmErr)
		}
	}
	if c.nodeID > 0 {
		deleteErr, confirmErr := deleteMutationNodeAndConfirm(ctx, client, c.db, c.nodeID)
		if deleteErr != nil {
			t.Logf("cleanup node %d delete returned before confirmation: %v", c.nodeID, deleteErr)
		}
		if confirmErr != nil {
			t.Errorf("cleanup node %d failed: %v", c.nodeID, confirmErr)
		}
	}
}

func cleanupMutationPrefix(t testing.TB, ctx context.Context, client *openplant.Client, db openplant.DatabaseName, prefix string) {
	t.Helper()
	clean := sanitizeMutationPrefix(prefix)
	if !strings.HasPrefix(clean, "SDK") {
		t.Fatalf("mutation cleanup prefix must start with SDK, got %q", clean)
	}
	gnPrefix := openplant.GN(fmt.Sprintf("%s.%s", db, clean))

	points, err := client.Metadata().FindPoints(ctx, openplant.MetadataPointQuery{
		DB:     db,
		Prefix: gnPrefix,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("pre-clean query points for %s failed: %v", gnPrefix, err)
	}
	if len(points) > 0 {
		configs := make([]openplant.PointConfig, 0, len(points))
		for _, point := range points {
			if point.ID > 0 {
				configs = append(configs, openplant.PointConfig{ID: point.ID})
			}
		}
		cleanupBegin, cleanupEnd := mutationArchiveCleanupRange()
		for _, config := range configs {
			if err := deleteMutationArchiveBySQL(ctx, client, db, config.ID, cleanupBegin, cleanupEnd); err != nil {
				t.Logf("pre-clean archive SQL cleanup for point %d skipped: %v", config.ID, err)
			}
		}
		for _, config := range configs {
			deleteErr, confirmErr := deleteMutationPointAndConfirm(ctx, client, db, config.ID)
			if deleteErr != nil {
				t.Logf("pre-clean point %d delete returned before confirmation: %v", config.ID, deleteErr)
			}
			if confirmErr != nil {
				t.Fatalf("pre-clean delete point %d for %s failed: %v", config.ID, gnPrefix, confirmErr)
			}
		}
	}

	nodes, err := client.Metadata().ListNodes(ctx, openplant.MetadataNodeQuery{
		DB:     db,
		Prefix: gnPrefix,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("pre-clean query nodes for %s failed: %v", gnPrefix, err)
	}
	if len(nodes) == 0 {
		return
	}
	configs := make([]openplant.Node, 0, len(nodes))
	for _, node := range nodes {
		if node.ID > 0 {
			configs = append(configs, openplant.Node{ID: node.ID})
		}
	}
	if len(configs) == 0 {
		return
	}
	for _, config := range configs {
		deleteErr, confirmErr := deleteMutationNodeAndConfirm(ctx, client, db, config.ID)
		if deleteErr != nil {
			t.Logf("pre-clean node %d delete returned before confirmation: %v", config.ID, deleteErr)
		}
		if confirmErr != nil {
			t.Fatalf("pre-clean delete node %d for %s failed: %v", config.ID, gnPrefix, confirmErr)
		}
	}
}

func assertMutationPrefixEmpty(t testing.TB, ctx context.Context, client *openplant.Client, db openplant.DatabaseName, prefix string) {
	t.Helper()
	clean := sanitizeMutationPrefix(prefix)
	gnPrefix := openplant.GN(fmt.Sprintf("%s.%s", db, clean))
	points, err := client.Metadata().FindPoints(ctx, openplant.MetadataPointQuery{
		DB:     db,
		Prefix: gnPrefix,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("post-clean query points for %s failed: %v", gnPrefix, err)
	}
	if len(points) > 0 {
		t.Fatalf("post-clean found %d leftover point(s) for %s: %#v", len(points), gnPrefix, points)
	}
	nodes, err := client.Metadata().ListNodes(ctx, openplant.MetadataNodeQuery{
		DB:     db,
		Prefix: gnPrefix,
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("post-clean query nodes for %s failed: %v", gnPrefix, err)
	}
	if len(nodes) > 0 {
		t.Fatalf("post-clean found %d leftover node(s) for %s: %#v", len(nodes), gnPrefix, nodes)
	}
}

func deleteMutationArchiveBySQL(ctx context.Context, client *openplant.Client, db openplant.DatabaseName, id openplant.PointID, begin, end time.Time) error {
	if err := db.Validate(); err != nil {
		return err
	}
	if id <= 0 {
		return nil
	}
	query := fmt.Sprintf(
		`DELETE FROM %s.Archive WHERE ID=%d AND TM BETWEEN '%s' AND '%s'`,
		db,
		id,
		begin.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"),
	)
	_, err := client.SQL().ExecUnsafe(ctx, query)
	return err
}

func deleteMutationPointBySQL(ctx context.Context, client *openplant.Client, db openplant.DatabaseName, id openplant.PointID) error {
	if err := db.Validate(); err != nil {
		return err
	}
	if id <= 0 {
		return nil
	}
	query := fmt.Sprintf(`DELETE FROM %s.Point WHERE ID=%d`, db, id)
	_, err := client.SQL().ExecUnsafe(ctx, query)
	return err
}

func deleteMutationPointAndConfirm(ctx context.Context, client *openplant.Client, db openplant.DatabaseName, id openplant.PointID) (error, error) {
	deleteCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	deleteErr := deleteMutationPointBySQL(deleteCtx, client, db, id)
	cancel()
	return deleteErr, waitForMutationPointDeleted(ctx, client, db, id)
}

func deleteMutationNodeBySQL(ctx context.Context, client *openplant.Client, db openplant.DatabaseName, id openplant.NodeID) error {
	if err := db.Validate(); err != nil {
		return err
	}
	if id <= 0 {
		return nil
	}
	query := fmt.Sprintf(`DELETE FROM %s.Node WHERE ID=%d`, db, id)
	_, err := client.SQL().ExecUnsafe(ctx, query)
	return err
}

func deleteMutationNodeAndConfirm(ctx context.Context, client *openplant.Client, db openplant.DatabaseName, id openplant.NodeID) (error, error) {
	deleteCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	deleteErr := deleteMutationNodeBySQL(deleteCtx, client, db, id)
	cancel()
	return deleteErr, waitForMutationNodeDeleted(ctx, client, db, id)
}

func waitForMutationPointDeleted(ctx context.Context, client *openplant.Client, db openplant.DatabaseName, id openplant.PointID) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastErr error
	for {
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		points, err := client.Metadata().FindPoints(queryCtx, openplant.MetadataPointQuery{
			DB:  db,
			IDs: []openplant.PointID{id},
		})
		cancel()
		if err != nil {
			lastErr = err
		} else if len(points) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("point still present or unconfirmed before timeout: %w", lastErr)
			}
			return fmt.Errorf("point still present before timeout")
		case <-ticker.C:
		}
	}
}

func waitForMutationNodeDeleted(ctx context.Context, client *openplant.Client, db openplant.DatabaseName, id openplant.NodeID) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastErr error
	for {
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		nodes, err := client.Metadata().ListNodes(queryCtx, openplant.MetadataNodeQuery{
			DB:  db,
			IDs: []openplant.NodeID{id},
		})
		cancel()
		if err != nil {
			lastErr = err
		} else if len(nodes) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("node still present or unconfirmed before timeout: %w", lastErr)
			}
			return fmt.Errorf("node still present before timeout")
		case <-ticker.C:
		}
	}
}

func mutationArchiveCleanupRange() (time.Time, time.Time) {
	return time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local), time.Date(2037, 12, 31, 23, 59, 59, 0, time.Local)
}

func waitForMutationNode(t testing.TB, ctx context.Context, client *openplant.Client, db openplant.DatabaseName, nodePN string) openplant.Node {
	t.Helper()
	gn := openplant.GN(fmt.Sprintf("%s.%s", db, nodePN))
	var node openplant.Node
	waitForCondition(t, ctx, "created node metadata", func() (bool, error) {
		nodes, err := client.Metadata().ListNodes(ctx, openplant.MetadataNodeQuery{
			DB:  db,
			GNs: []openplant.GN{gn},
		})
		if err != nil {
			return false, err
		}
		if len(nodes) == 0 {
			return false, nil
		}
		node = nodes[0]
		return node.ID > 0, nil
	})
	return node
}

func waitForMutationPoint(t testing.TB, ctx context.Context, client *openplant.Client, db openplant.DatabaseName, nodeGN openplant.GN, pointPN string) openplant.Point {
	t.Helper()
	pointGN := openplant.GN(fmt.Sprintf("%s.%s", nodeGN, pointPN))
	var point openplant.Point
	waitForCondition(t, ctx, "created point metadata", func() (bool, error) {
		points, err := client.Metadata().FindPoints(ctx, openplant.MetadataPointQuery{
			DB:  db,
			GNs: []openplant.GN{pointGN},
		})
		if err != nil {
			return false, err
		}
		if len(points) == 0 {
			return false, nil
		}
		point = points[0]
		return point.ID > 0, nil
	})
	return point
}

func waitForMutationPointDescription(t testing.TB, ctx context.Context, client *openplant.Client, db openplant.DatabaseName, gn openplant.GN, want string) {
	t.Helper()
	waitForCondition(t, ctx, "updated point description", func() (bool, error) {
		points, err := client.Metadata().FindPoints(ctx, openplant.MetadataPointQuery{
			DB:  db,
			GNs: []openplant.GN{gn},
		})
		if err != nil {
			return false, err
		}
		return len(points) == 1 && points[0].Description == want, nil
	})
}

func waitForRealtimeValue(t testing.TB, ctx context.Context, client *openplant.Client, db openplant.DatabaseName, id openplant.PointID, want float64) {
	t.Helper()
	waitForCondition(t, ctx, "realtime native value", func() (bool, error) {
		samples, err := client.Realtime().Read(ctx, openplant.RealtimeReadRequest{
			DB:  db,
			IDs: []openplant.PointID{id},
		})
		if err != nil {
			return false, err
		}
		if len(samples) != 1 || samples[0].ID != id {
			return false, nil
		}
		got, ok := samples[0].Value.Float64()
		return ok && closeFloat(got, want), nil
	})
}

func waitForSubscriptionValue(t testing.TB, ctx context.Context, client *openplant.Client, stream *openplant.SubscribeStream, db openplant.DatabaseName, id openplant.PointID, want float64) {
	t.Helper()
	writeRealtimeValue(t, ctx, client, db, id, want)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case event, ok := <-stream.Events():
			if !ok {
				t.Fatalf("subscription ended before value arrived: %v", stream.Err())
			}
			if event.IsError() {
				t.Fatalf("subscription error: %v", event.Err)
			}
			if !event.IsData() || event.Sample.ID != id {
				continue
			}
			got, ok := event.Sample.Value.Float64()
			if ok && closeFloat(got, want) {
				return
			}
		case <-ticker.C:
			writeRealtimeValue(t, ctx, client, db, id, want)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for subscription value: %v", ctx.Err())
		}
	}
}

func waitForArchiveValue(t testing.TB, ctx context.Context, client *openplant.Client, db openplant.DatabaseName, id openplant.PointID, sampleTime time.Time, want float64) {
	t.Helper()
	query := openplant.ArchiveQuery{
		DB:      db,
		IDs:     []openplant.PointID{id},
		Range:   openplant.TimeRange{Begin: sampleTime.Add(-5 * time.Second), End: sampleTime.Add(5 * time.Second)},
		Mode:    openplant.ModeRaw,
		Quality: openplant.QualityNone,
		Limit:   20,
	}
	waitForCondition(t, ctx, "archive request value", func() (bool, error) {
		samples, err := client.Archive().QueryRequest(ctx, query)
		if err != nil {
			return false, err
		}
		for _, sample := range samples {
			got, ok := sample.Value.Float64()
			if sample.ID == id && ok && closeFloat(got, want) {
				return true, nil
			}
		}
		return false, nil
	})
}

func waitForCondition(t testing.TB, ctx context.Context, label string, fn func() (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		ok, err := fn()
		if err != nil {
			lastErr = err
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				t.Fatalf("timed out waiting for %s: %v", label, lastErr)
			}
			t.Fatalf("timed out waiting for %s", label)
		case <-ticker.C:
		}
	}
}

func mutationName(prefix, kind, token string, maxLen int) string {
	clean := sanitizeMutationPrefix(prefix)
	budget := maxLen - len(kind) - len(token)
	if budget < 3 {
		clean = "SDK"
		budget = maxLen - len(kind) - len(token)
	}
	if budget < 0 {
		token = token[len(token)+budget:]
		budget = 0
	}
	if len(clean) > budget {
		clean = clean[:budget]
	}
	return clean + kind + token
}

func sanitizeMutationPrefix(prefix string) string {
	var b strings.Builder
	for _, r := range prefix {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - 'a' + 'A')
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_':
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "SDK_MUTATION_"
	}
	return b.String()
}

func closeFloat(got, want float64) bool {
	return math.Abs(got-want) < 0.000001
}
