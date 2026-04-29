package testenv

import (
	"os"
	"strconv"
	"testing"

	"github.com/tc252617228/openplant/model"
)

type Config struct {
	Host     string
	Port     int
	User     string
	Pass     string
	ReadOnly bool
	Mutation bool
	DB       string
	PointID  model.PointID
	PointGN  model.GN
	Prefix   string
}

func Load(prefix string) Config {
	if prefix == "" {
		prefix = "OPENPLANT_TEST"
	}
	port := 8200
	if raw := os.Getenv(prefix + "_PORT"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			port = parsed
		}
	}
	var pointID model.PointID
	if raw := os.Getenv(prefix + "_POINT_ID"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 32); err == nil {
			pointID = model.PointID(parsed)
		}
	}
	return Config{
		Host:     os.Getenv(prefix + "_HOST"),
		Port:     port,
		User:     os.Getenv(prefix + "_USER"),
		Pass:     os.Getenv(prefix + "_PASS"),
		ReadOnly: os.Getenv(prefix+"_READONLY") != "0",
		Mutation: os.Getenv(prefix+"_MUTATION") == "1",
		DB:       os.Getenv(prefix + "_DB"),
		PointID:  pointID,
		PointGN:  model.GN(os.Getenv(prefix + "_POINT_GN")),
		Prefix:   os.Getenv(prefix + "_PREFIX"),
	}
}

func RequireSafeReadonly(t testing.TB) Config {
	t.Helper()
	cfg := Load("OPENPLANT_TEST")
	if cfg.Host == "" || cfg.User == "" || cfg.Pass == "" {
		t.Skip("OPENPLANT_TEST_HOST/USER/PASS are required for safe readonly integration tests")
	}
	if !cfg.ReadOnly {
		t.Fatalf("safe readonly integration tests require OPENPLANT_TEST_READONLY=1 or unset")
	}
	return cfg
}

func RequireMutation(t testing.TB) Config {
	t.Helper()
	cfg := Load("OPENPLANT_TEST")
	if !cfg.Mutation {
		t.Skip("OPENPLANT_TEST_MUTATION=1 is required for mutation tests")
	}
	if cfg.Host == "" || cfg.User == "" || cfg.Pass == "" {
		t.Fatalf("mutation tests require OPENPLANT_TEST_HOST/USER/PASS")
	}
	if cfg.ReadOnly {
		t.Fatalf("mutation tests require OPENPLANT_TEST_READONLY=0")
	}
	if cfg.DB == "" || cfg.Prefix == "" {
		t.Fatalf("mutation tests require OPENPLANT_TEST_DB and OPENPLANT_TEST_PREFIX")
	}
	return cfg
}
