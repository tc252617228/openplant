package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	openplant "github.com/tc252617228/openplant"
)

type namedMutation struct {
	label string
	req   openplant.TableMutation
}

func main() {
	db, dbSet := envDatabase()
	prefix, prefixSet := envPrefix()
	token := mutationToken()

	mutations := buildAdminMutations(db, prefix, token)
	for _, mutation := range mutations {
		printMutation(mutation)
	}

	if os.Getenv("OPENPLANT_TEST_APPLY_ADMIN") != "1" {
		fmt.Println("dry run only; set OPENPLANT_TEST_APPLY_ADMIN=1 and OPENPLANT_TEST_MUTATION=1 to execute Admin().MutateTable")
		return
	}
	requireMutationEnvironment(dbSet, prefixSet)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := newWritableClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	for _, mutation := range mutations {
		if err := client.Admin().MutateTable(ctx, mutation.req); err != nil {
			log.Fatalf("%s failed: %v", mutation.label, err)
		}
		fmt.Printf("applied %s\n", mutation.label)
	}
	writeRealtimeWhenExplicitlyEnabled(ctx, client, db)
}

func buildAdminMutations(db openplant.DatabaseName, prefix, token string) []namedMutation {
	nodeName := mutationName(prefix, "N", token, 24)
	nodeReq, err := openplant.BuildNodeInsert(db, []openplant.Node{{
		Name:        nodeName,
		Description: "openplant sdk mutation example node",
	}})
	if err != nil {
		log.Fatal(err)
	}
	mutations := []namedMutation{{label: "node insert", req: nodeReq}}

	if nodeID, ok := envNodeID("OPENPLANT_TEST_NODE_ID"); ok {
		pointName := mutationName(prefix, "P", token, 32)
		pointReq, err := openplant.BuildPointConfigInsert(db, []openplant.PointConfig{{
			NodeID:      nodeID,
			Name:        pointName,
			Source:      openplant.SourceDAS,
			Type:        openplant.TypeR8,
			Description: "openplant sdk mutation example point",
			Unit:        "unit",
			Format:      2,
			Archived:    true,
			RangeLower:  0,
			RangeUpper:  100,
		}})
		if err != nil {
			log.Fatal(err)
		}
		mutations = append(mutations, namedMutation{label: "point config insert", req: pointReq})
	}

	if nodeID, ok := envNodeID("OPENPLANT_TEST_SYSTEM_NODE_ID"); ok {
		systemReq, err := openplant.BuildDefaultSystemPointTemplateInsert(db, nodeID)
		if err != nil {
			log.Fatal(err)
		}
		mutations = append(mutations, namedMutation{label: "system point templates insert", req: systemReq})
	}

	if userName := os.Getenv("OPENPLANT_TEST_EXAMPLE_USER"); userName != "" {
		password := os.Getenv("OPENPLANT_TEST_EXAMPLE_PASS")
		userReq, err := openplant.BuildUserInsert(db, []openplant.UserCredential{{
			Name:     userName,
			Password: password,
		}})
		if err != nil {
			log.Fatal(err)
		}
		mutations = append(mutations, namedMutation{label: "user insert", req: userReq})
	}

	return mutations
}

func printMutation(mutation namedMutation) {
	fmt.Printf(
		"%s: db=%s table=%s action=%s rows=%d columns=%d\n",
		mutation.label,
		mutation.req.DB,
		mutation.req.Table,
		mutation.req.Action,
		len(mutation.req.Rows),
		len(mutation.req.Columns),
	)
}

func requireMutationEnvironment(dbSet, prefixSet bool) {
	if os.Getenv("OPENPLANT_TEST_MUTATION") != "1" {
		log.Fatal("set OPENPLANT_TEST_MUTATION=1 before executing mutation examples")
	}
	if !dbSet {
		log.Fatal("set OPENPLANT_TEST_DB to the isolated mutation database")
	}
	if !prefixSet {
		log.Fatal("set OPENPLANT_TEST_PREFIX to an isolated mutation object prefix")
	}
	for _, name := range []string{"OPENPLANT_TEST_HOST", "OPENPLANT_TEST_USER", "OPENPLANT_TEST_PASS"} {
		if os.Getenv(name) == "" {
			log.Fatalf("set %s before executing mutation examples", name)
		}
	}
}

func newWritableClient() (*openplant.Client, error) {
	opts := append(openplant.OptionsFromEnv("OPENPLANT_TEST"),
		openplant.WithReadOnly(false),
		openplant.WithTimeouts(5*time.Second, 20*time.Second),
		openplant.WithPool(1, 1, time.Minute, 5*time.Minute),
	)
	return openplant.New(opts...)
}

func writeRealtimeWhenExplicitlyEnabled(ctx context.Context, client *openplant.Client, db openplant.DatabaseName) {
	if os.Getenv("OPENPLANT_TEST_WRITE_REALTIME") != "1" {
		return
	}
	id, ok := envPointID()
	if !ok {
		log.Fatal("set OPENPLANT_TEST_POINT_ID before OPENPLANT_TEST_WRITE_REALTIME=1")
	}
	err := client.Realtime().WriteNative(ctx, openplant.RealtimeWriteRequest{
		DB: db,
		Values: []openplant.RealtimeWrite{{
			ID:     id,
			Type:   openplant.TypeR8,
			Time:   time.Now(),
			Status: 0,
			Value:  openplant.R8(1.0),
		}},
	})
	if err != nil {
		log.Fatalf("realtime write failed: %v", err)
	}
	fmt.Printf("wrote realtime value to point ID %d\n", id)
}

func envDatabase() (openplant.DatabaseName, bool) {
	if value := os.Getenv("OPENPLANT_TEST_DB"); value != "" {
		return openplant.DatabaseName(value), true
	}
	return "W3", false
}

func envPrefix() (string, bool) {
	if value := os.Getenv("OPENPLANT_TEST_PREFIX"); value != "" {
		return sanitizeMutationPrefix(value), true
	}
	return "SDK_MUTATION_", false
}

func envNodeID(name string) (openplant.NodeID, bool) {
	value := os.Getenv(name)
	if value == "" {
		return 0, false
	}
	id, err := strconv.Atoi(value)
	if err != nil || id < 0 {
		log.Fatalf("%s must be a non-negative node ID", name)
	}
	return openplant.NodeID(id), true
}

func envPointID() (openplant.PointID, bool) {
	value := os.Getenv("OPENPLANT_TEST_POINT_ID")
	if value == "" {
		return 0, false
	}
	id, err := strconv.Atoi(value)
	if err != nil || id <= 0 {
		log.Fatal("OPENPLANT_TEST_POINT_ID must be a positive point ID")
	}
	return openplant.PointID(id), true
}

func mutationToken() string {
	token := strings.ToUpper(fmt.Sprintf("%x", time.Now().UnixNano()))
	if len(token) > 8 {
		token = token[len(token)-8:]
	}
	return token
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
