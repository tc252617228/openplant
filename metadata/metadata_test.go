package metadata

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tc252617228/openplant/model"
	sqlapi "github.com/tc252617228/openplant/sql"
)

type fakeQueryer struct {
	query string
	rows  []sqlapi.Row
	err   error
}

func (f *fakeQueryer) Query(ctx context.Context, query string) (sqlapi.Result, error) {
	f.query = query
	return sqlapi.Result{Rows: f.rows}, f.err
}

func TestFindPointsUsesBoundedSQL(t *testing.T) {
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(1), "UD": int64(2), "ND": int32(3),
		"PT": int8(0), "RT": int8(4),
		"PN": "P1", "AN": "A1", "ED": "desc", "KR": "K",
		"SG": []byte{0b00000011, 0, 0, 0},
		"FQ": int16(1), "LC": int16(3), "AP": int8(1), "AR": int8(1),
		"EU": "MW", "FM": int16(2), "CT": time.Unix(10, 0),
		"EX": "", "GN": "W3.N.P1",
	}}}
	svc := NewService(Options{Queryer: fake})
	points, err := svc.FindPoints(context.Background(), PointQuery{
		DB:  "W3",
		GNs: []model.GN{"W3.N.P1"},
	})
	if err != nil {
		t.Fatalf("FindPoints failed: %v", err)
	}
	if len(points) != 1 || points[0].ID != 1 || points[0].Type != model.TypeR8 || points[0].GN != "W3.N.P1" {
		t.Fatalf("unexpected points: %#v", points)
	}
	if !points[0].Security.Has(0) || !points[0].Security.Has(1) || points[0].Security.Has(2) {
		t.Fatalf("unexpected point security groups: %#v", points[0].Security)
	}
	for _, want := range []string{`FROM W3.Point`, `"GN" IN ('W3.N.P1')`, `LIMIT 1`} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestFindPointsRejectsUnboundedQuery(t *testing.T) {
	svc := NewService(Options{Queryer: &fakeQueryer{}})
	_, err := svc.FindPoints(context.Background(), PointQuery{DB: "W3"})
	if err == nil {
		t.Fatalf("expected unbounded point query to be rejected")
	}
}

func TestFindPointConfigsUsesFullPointSQL(t *testing.T) {
	configTime := time.Unix(100, 0)
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(1), "UD": int64(2), "ND": int32(3), "CD": int32(4),
		"PT": int8(1), "RT": int8(0), "PN": "P1", "AN": "A1", "ED": "desc", "KR": "K",
		"SG": []byte{0b00000001, 0, 0, 0}, "FQ": int16(60), "CP": int16(5), "HW": int32(6), "BP": int16(7),
		"SR": "4-20mA", "AD": "AI1", "LC": int16(255), "AP": int8(2), "AR": int8(1), "OF": int8(1), "FL": int32(9),
		"ST": "ON", "RS": "OFF", "EU": "%", "FM": int16(2), "IV": float32(1.5), "BV": float64(0), "TV": float64(100),
		"LL": float64(10), "HL": float64(90), "ZL": float64(5), "ZH": float64(95), "L3": float64(1), "H3": float64(99),
		"L4": float64(0), "H4": float64(100), "C1": int32(0xFF0000), "C2": int32(0xCC0000), "C3": int32(0x990000),
		"C4": int32(0x660000), "C5": int32(0xFF0000), "C6": int32(0xCC0000), "C7": int32(0x990000), "C8": int32(0x660000),
		"DB": float64(0.2), "DT": int8(1), "KZ": int8(1), "TT": int8(2), "TP": int16(60), "OT": int16(5),
		"KT": int8(1), "KO": int8(3), "FK": float64(1.25), "FB": float64(-0.5), "CT": configTime, "EX": "return 1", "GN": "W3.N.P1",
	}}}
	svc := NewService(Options{Queryer: fake})
	configs, err := svc.FindPointConfigs(context.Background(), PointQuery{
		DB:      "W3",
		GNs:     []model.GN{"W3.N.P1"},
		OrderBy: "KO DESC",
	})
	if err != nil {
		t.Fatalf("FindPointConfigs failed: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("unexpected config count: %d", len(configs))
	}
	cfg := configs[0]
	if cfg.ID != 1 || cfg.DeviceID != 4 || cfg.Source != model.SourceCalc || cfg.Type != model.TypeAX || cfg.GN != "W3.N.P1" {
		t.Fatalf("unexpected config identity: %#v", cfg)
	}
	if cfg.Processor != 5 || cfg.HardwareAddress != 6 || cfg.Channel != 7 || cfg.SignalType != "4-20mA" || cfg.SignalAddress != "AI1" {
		t.Fatalf("unexpected acquisition config: %#v", cfg)
	}
	if cfg.AlarmCode != model.AlarmLimitMask || cfg.AlarmLevel != model.AlarmPriorityYellow || !cfg.Archived || !cfg.Offline {
		t.Fatalf("unexpected alarm/archive config: %#v", cfg)
	}
	if cfg.Limits.LL != 10 || cfg.Limits.H4 != 100 || cfg.Colors.LL != 0xFF0000 || cfg.Colors.H4 != 0x660000 {
		t.Fatalf("unexpected limits/colors: %#v %#v", cfg.Limits, cfg.Colors)
	}
	if cfg.Deadband != 0.2 || cfg.DeadbandType != model.DeadbandENG || cfg.Compression != model.PointCompressionLinear {
		t.Fatalf("unexpected deadband/compression: %#v", cfg)
	}
	if cfg.StatType != 2 || cfg.StatPeriod != 60 || cfg.StatOffset != 5 || cfg.CalcType != 1 || cfg.CalcOrder != 3 {
		t.Fatalf("unexpected stat/calc config: %#v", cfg)
	}
	if cfg.ScaleFactor != 1.25 || cfg.Offset != -0.5 || cfg.ConfigTime != configTime || cfg.Expression != "return 1" {
		t.Fatalf("unexpected expression config: %#v", cfg)
	}
	light := cfg.Point()
	if light.ID != cfg.ID || light.GN != cfg.GN || light.AlarmCode != cfg.AlarmCode {
		t.Fatalf("PointConfig.Point mismatch: %#v", light)
	}
	for _, want := range []string{`FROM W3.Point`, `"FK"`, `"FB"`, `"KO"`, `"C1"`, `"C8"`, `"GN" IN ('W3.N.P1')`, `ORDER BY "KO" DESC`, `LIMIT 1`} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestFindPointConfigsRejectsUnboundedQuery(t *testing.T) {
	svc := NewService(Options{Queryer: &fakeQueryer{}})
	_, err := svc.FindPointConfigs(context.Background(), PointQuery{DB: "W3"})
	if err == nil {
		t.Fatalf("expected unbounded point config query to be rejected")
	}
}

func TestFindCalculationPointConfigsAddsCalcFilters(t *testing.T) {
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(10), "PT": int8(1), "RT": int8(0), "PN": "CALC1", "EX": `return op.value("W3.N.P1")`, "GN": "W3.CALC.P1",
	}}}
	svc := NewService(Options{Queryer: fake})
	configs, err := svc.FindCalculationPointConfigs(context.Background(), PointQuery{
		DB:     "W3",
		Prefix: "W3.CALC_",
		Limit:  5,
	})
	if err != nil {
		t.Fatalf("FindCalculationPointConfigs failed: %v", err)
	}
	if len(configs) != 1 || configs[0].Source != model.SourceCalc || configs[0].Expression == "" {
		t.Fatalf("unexpected calculation configs: %#v", configs)
	}
	for _, want := range []string{`FROM W3.Point`, `"GN" LIKE 'W3.CALC\_%' ESCAPE '\'`, `"PT" = 1`, `"EX" <> ''`, `LIMIT 5`} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestFindCalculationPointConfigsRejectsUnboundedQuery(t *testing.T) {
	svc := NewService(Options{Queryer: &fakeQueryer{}})
	_, err := svc.FindCalculationPointConfigs(context.Background(), PointQuery{DB: "W3"})
	if err == nil {
		t.Fatalf("expected unbounded calculation point query to be rejected")
	}
}

func TestFindPointsNormalizesSafeOrderBy(t *testing.T) {
	fake := &fakeQueryer{}
	svc := NewService(Options{Queryer: fake})
	_, err := svc.FindPoints(context.Background(), PointQuery{
		DB:      "W3",
		Limit:   10,
		OrderBy: `ID desc, "GN"`,
	})
	if err != nil {
		t.Fatalf("FindPoints failed: %v", err)
	}
	if !strings.Contains(fake.query, `ORDER BY "ID" DESC,"GN" ASC`) {
		t.Fatalf("order by was not normalized: %s", fake.query)
	}
}

func TestFindPointsRejectsUnsafeOrderBy(t *testing.T) {
	svc := NewService(Options{Queryer: &fakeQueryer{}})
	for _, orderBy := range []string{
		`ID DESC NULLS LAST`,
		`ID; DROP TABLE Point`,
		`PASSWORD ASC`,
	} {
		_, err := svc.FindPoints(context.Background(), PointQuery{
			DB:      "W3",
			Limit:   10,
			OrderBy: orderBy,
		})
		if err == nil {
			t.Fatalf("expected unsafe order by to be rejected: %q", orderBy)
		}
	}
}

func TestListNodesUsesBoundedSQL(t *testing.T) {
	configTime := time.Unix(123, 0)
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(2), "UD": int64(9), "ND": int32(1), "PN": "N1", "ED": "node",
		"FQ": int32(3), "LC": int32(1), "AR": int8(1), "OF": int8(0), "LO": int8(1), "CT": configTime, "GN": "W3.N",
	}}}
	svc := NewService(Options{Queryer: fake})
	nodes, err := svc.ListNodes(context.Background(), NodeQuery{
		DB:  "W3",
		GNs: []model.GN{"W3.N"},
	})
	if err != nil {
		t.Fatalf("ListNodes failed: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID != 2 || nodes[0].ParentID != 1 || nodes[0].GN != "W3.N" || !nodes[0].Archived || !nodes[0].Internal {
		t.Fatalf("unexpected nodes: %#v", nodes)
	}
	for _, want := range []string{`FROM W3.Node`, `"LO"`, `"GN" IN ('W3.N')`, `ORDER BY "ID" ASC`, `LIMIT 1`} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestFindPointsEscapesPrefixLike(t *testing.T) {
	fake := &fakeQueryer{}
	svc := NewService(Options{Queryer: fake})
	_, err := svc.FindPoints(context.Background(), PointQuery{
		DB:     "W3",
		Prefix: "W3.NODE_",
		Limit:  5,
	})
	if err != nil {
		t.Fatalf("FindPoints failed: %v", err)
	}
	want := `"GN" LIKE 'W3.NODE\_%' ESCAPE '\'`
	if !strings.Contains(fake.query, want) {
		t.Fatalf("query did not escape prefix %q: %s", want, fake.query)
	}
}

func TestListNodesRejectsUnboundedQuery(t *testing.T) {
	svc := NewService(Options{Queryer: &fakeQueryer{}})
	_, err := svc.ListNodes(context.Background(), NodeQuery{DB: "W3"})
	if err == nil {
		t.Fatalf("expected unbounded node query to be rejected")
	}
}

func TestListNodesAllowsExplicitRootID(t *testing.T) {
	fake := &fakeQueryer{}
	svc := NewService(Options{Queryer: fake})
	if _, err := svc.ListNodes(context.Background(), NodeQuery{DB: "W3", IDs: []model.NodeID{0}}); err != nil {
		t.Fatalf("ListNodes rejected root ID: %v", err)
	}
	if !strings.Contains(fake.query, `"ID" IN (0)`) {
		t.Fatalf("query missing root ID: %s", fake.query)
	}
}

func TestListDASUsesBoundedSQL(t *testing.T) {
	configTime := time.Unix(123, 0)
	updateTime := time.Unix(456, 0)
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(10), "UD": int64(11), "ND": int32(2), "PN": "DAS1", "ED": "das",
		"IP": "127.0.0.1", "PO": int32(8200), "VN": int32(5), "CT": configTime,
		"GN": "W3.DAS1", "TM": updateTime, "AS": int16(1), "AV": int32(2),
	}}}
	svc := NewService(Options{Queryer: fake})
	items, err := svc.ListDAS(context.Background(), DASQuery{
		DB:  "W3",
		GNs: []model.GN{"W3.DAS1"},
	})
	if err != nil {
		t.Fatalf("ListDAS failed: %v", err)
	}
	if len(items) != 1 || items[0].ID != 10 || items[0].NodeID != 2 || items[0].GN != "W3.DAS1" || items[0].Port != 8200 || items[0].UpdateTime != updateTime {
		t.Fatalf("unexpected DAS items: %#v", items)
	}
	for _, want := range []string{`FROM W3.DAS`, `"GN" IN ('W3.DAS1')`, `ORDER BY "ID" ASC`, `LIMIT 1`} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestListDASRejectsUnboundedQuery(t *testing.T) {
	svc := NewService(Options{Queryer: &fakeQueryer{}})
	_, err := svc.ListDAS(context.Background(), DASQuery{DB: "W3"})
	if err == nil {
		t.Fatalf("expected unbounded DAS query to be rejected")
	}
}

func TestListDevicesUsesBoundedSQL(t *testing.T) {
	configTime := time.Unix(123, 0)
	updateTime := time.Unix(456, 0)
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(20), "UD": int64(21), "ND": int32(2), "CD": int32(10),
		"PN": "DEV1", "ED": "device", "CP": int32(3), "IP": "127.0.0.2",
		"BA": "addr", "LN": "line", "CT": configTime, "GN": "W3.DEV1",
		"TM": updateTime, "AS": int16(4), "AV": int32(5),
	}}}
	svc := NewService(Options{Queryer: fake})
	items, err := svc.ListDevices(context.Background(), DeviceQuery{
		DB:     "W3",
		Prefix: "W3.DEV",
		Limit:  5,
	})
	if err != nil {
		t.Fatalf("ListDevices failed: %v", err)
	}
	if len(items) != 1 || items[0].ID != 20 || items[0].DASID != 10 || items[0].GN != "W3.DEV1" || items[0].Address != "addr" || items[0].LineName != "line" {
		t.Fatalf("unexpected device items: %#v", items)
	}
	for _, want := range []string{`FROM W3.Device`, `"GN" LIKE 'W3.DEV%'`, `ORDER BY "ID" ASC`, `LIMIT 5`} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestListDevicesRejectsUnboundedQuery(t *testing.T) {
	svc := NewService(Options{Queryer: &fakeQueryer{}})
	_, err := svc.ListDevices(context.Background(), DeviceQuery{DB: "W3"})
	if err == nil {
		t.Fatalf("expected unbounded device query to be rejected")
	}
}

func TestListProductsRootsServersAndSecurityTables(t *testing.T) {
	expire := time.Unix(999, 0)
	tests := []struct {
		name      string
		run       func(*Service) error
		rows      []sqlapi.Row
		wantQuery []string
	}{
		{
			name: "products",
			run: func(s *Service) error {
				items, err := s.ListProducts(context.Background(), 5)
				if err != nil {
					return err
				}
				if len(items) != 1 || items[0].Project != "PJ" || items[0].ExpireTime != expire {
					t.Fatalf("products=%#v", items)
				}
				return nil
			},
			rows: []sqlapi.Row{{
				"PJ": "PJ", "HO": "host", "PN": "product", "ED": "desc", "VN": "v1", "LI": "lic", "SZ": int32(1), "ET": expire, "AA": "auth",
			}},
			wantQuery: []string{`FROM "Product"`, `ORDER BY "PJ" ASC,"HO" ASC,"PN" ASC`, `LIMIT 5`},
		},
		{
			name: "roots",
			run: func(s *Service) error {
				items, err := s.ListRoots(context.Background(), 5)
				if err != nil {
					return err
				}
				if len(items) != 1 || items[0].ID != 1 || items[0].GN != "ROOT" {
					t.Fatalf("roots=%#v", items)
				}
				return nil
			},
			rows: []sqlapi.Row{{
				"ID": int32(1), "PN": "root", "ED": "desc", "IP": "127.0.0.1", "PO": int32(1), "IO": int32(2), "WT": int32(3),
				"MT": int32(4), "LG": int32(5), "SY": int32(6), "TD": int32(7), "SD": "data", "GN": "ROOT",
			}},
			wantQuery: []string{`FROM "Root"`, `WHERE "ID" >= 0`, `ORDER BY "ID" ASC`, `LIMIT 5`},
		},
		{
			name: "servers",
			run: func(s *Service) error {
				items, err := s.ListServers(context.Background(), 5)
				if err != nil {
					return err
				}
				if len(items) != 1 || items[0].ID != 1 || items[0].Port != 8200 {
					t.Fatalf("servers=%#v", items)
				}
				return nil
			},
			rows:      []sqlapi.Row{{"ID": int32(1), "PN": "server", "ED": "desc", "IP": "127.0.0.1", "PO": int32(8200)}},
			wantQuery: []string{`FROM "Server"`, `WHERE "ID" >= 0`, `ORDER BY "ID" ASC`, `LIMIT 5`},
		},
		{
			name: "users",
			run: func(s *Service) error {
				items, err := s.ListUsers(context.Background(), 5)
				if err != nil {
					return err
				}
				if len(items) != 1 || items[0].Name != "test-user" {
					t.Fatalf("users=%#v", items)
				}
				return nil
			},
			rows:      []sqlapi.Row{{"US": "test-user"}},
			wantQuery: []string{`SELECT "US" FROM "User"`, `ORDER BY "US" ASC`, `LIMIT 5`},
		},
		{
			name: "groups",
			run: func(s *Service) error {
				items, err := s.ListGroups(context.Background(), 5)
				if err != nil {
					return err
				}
				if len(items) != 1 || items[0].ID != 1 || items[0].Name != "ops" {
					t.Fatalf("groups=%#v", items)
				}
				return nil
			},
			rows:      []sqlapi.Row{{"ID": int32(1), "GP": "ops"}},
			wantQuery: []string{`FROM "Groups"`, `WHERE "ID" >= 0`, `ORDER BY "ID" ASC`, `LIMIT 5`},
		},
		{
			name: "access",
			run: func(s *Service) error {
				items, err := s.ListAccess(context.Background(), 5)
				if err != nil {
					return err
				}
				if len(items) != 1 || items[0].User != "test-user" || items[0].Group != "ops" || items[0].Privilege != "read" {
					t.Fatalf("access=%#v", items)
				}
				return nil
			},
			rows:      []sqlapi.Row{{"US": "test-user", "GP": "ops", "PL": "read"}},
			wantQuery: []string{`FROM "Access"`, `ORDER BY "US" ASC,"GP" ASC`, `LIMIT 5`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeQueryer{rows: tt.rows}
			svc := NewService(Options{Queryer: fake})
			if err := tt.run(svc); err != nil {
				t.Fatalf("query failed: %v", err)
			}
			for _, want := range tt.wantQuery {
				if !strings.Contains(fake.query, want) {
					t.Fatalf("query missing %q: %s", want, fake.query)
				}
			}
			if tt.name == "users" && strings.Contains(fake.query, `"PW"`) {
				t.Fatalf("users query must not read password field: %s", fake.query)
			}
		})
	}
}

func TestListReplicatorsAndRepItemsUseBoundedSQL(t *testing.T) {
	t.Run("replicators", func(t *testing.T) {
		fake := &fakeQueryer{rows: []sqlapi.Row{{
			"RN": "R1", "IP": "127.0.0.1", "PO": int32(1), "SP": int32(2), "SY": int32(1), "FL": int32(1), "AR": int32(0), "TL": int32(30),
		}}}
		svc := NewService(Options{Queryer: fake})
		items, err := svc.ListReplicators(context.Background(), ReplicatorQuery{DB: "W3", Names: []string{"R1"}})
		if err != nil {
			t.Fatalf("ListReplicators failed: %v", err)
		}
		if len(items) != 1 || items[0].Name != "R1" || !items[0].FilterUnchanged || items[0].ArchiveBackfill {
			t.Fatalf("replicators=%#v", items)
		}
		for _, want := range []string{`FROM W3.Replicator`, `"RN" IN ('R1')`, `ORDER BY "RN" ASC`, `LIMIT 1`} {
			if !strings.Contains(fake.query, want) {
				t.Fatalf("query missing %q: %s", want, fake.query)
			}
		}
	})
	t.Run("repitems", func(t *testing.T) {
		fake := &fakeQueryer{rows: []sqlapi.Row{{"PN": "P1", "TN": "T1", "XF": int32(7)}}}
		svc := NewService(Options{Queryer: fake})
		items, err := svc.ListRepItems(context.Background(), RepItemQuery{DB: "W3", PointNames: []string{"P1"}})
		if err != nil {
			t.Fatalf("ListRepItems failed: %v", err)
		}
		if len(items) != 1 || items[0].PointName != "P1" || items[0].TargetName != "T1" || items[0].Transform != 7 {
			t.Fatalf("repitems=%#v", items)
		}
		for _, want := range []string{`FROM W3.RepItem`, `"PN" IN ('P1')`, `ORDER BY "PN" ASC`, `LIMIT 1`} {
			if !strings.Contains(fake.query, want) {
				t.Fatalf("query missing %q: %s", want, fake.query)
			}
		}
	})
}

func TestReplicatorQueriesRejectUnbounded(t *testing.T) {
	svc := NewService(Options{Queryer: &fakeQueryer{}})
	if _, err := svc.ListReplicators(context.Background(), ReplicatorQuery{DB: "W3"}); err == nil {
		t.Fatalf("expected unbounded replicator query to be rejected")
	}
	if _, err := svc.ListRepItems(context.Background(), RepItemQuery{DB: "W3"}); err == nil {
		t.Fatalf("expected unbounded replication item query to be rejected")
	}
}

func TestListDatabases(t *testing.T) {
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(1), "UD": int64(99), "PN": "W3", "ED": "main", "GN": "W3",
	}}}
	svc := NewService(Options{Queryer: fake})
	dbs, err := svc.ListDatabases(context.Background())
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	if len(dbs) != 1 || dbs[0].Name != "W3" || dbs[0].UUID != 99 {
		t.Fatalf("unexpected dbs: %#v", dbs)
	}
	if !strings.Contains(fake.query, `FROM "Database"`) || !strings.Contains(fake.query, `LIMIT 1000`) {
		t.Fatalf("unexpected query: %s", fake.query)
	}
}
