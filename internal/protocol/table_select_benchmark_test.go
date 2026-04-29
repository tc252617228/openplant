package protocol

import "testing"

func BenchmarkTableSelectEncodeInt32Indexes(b *testing.B) {
	ids := make([]int32, 200)
	for i := range ids {
		ids[i] = int32(1000 + i)
	}
	req := TableSelectRequest{
		Table:   "W3.Realtime",
		Columns: []string{"ID", "GN", "TM", "DS", "AV"},
		Indexes: &Indexes{
			Key:   "ID",
			Int32: ids,
		},
		Props: map[string]any{
			PropAsync: int32(1),
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := req.Encode()
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func BenchmarkTableSelectEncodeStringIndexes(b *testing.B) {
	gns := make([]string, 200)
	for i := range gns {
		gns[i] = "W3.NODE.P" + string(rune('A'+i%26))
	}
	req := TableSelectRequest{
		Table:   "W3.Archive",
		Columns: []string{"ID", "GN", "TM", "DS", "AV"},
		Indexes: &Indexes{
			Key:     "GN",
			Strings: gns,
		},
		Filters: []Filter{
			{Left: "TM", Operator: OperGE, Right: "2026-01-01 00:00:00", Relation: RelationAnd},
			{Left: "TM", Operator: OperLE, Right: "2026-01-01 01:00:00", Relation: RelationAnd},
		},
		OrderBy: "ID ASC,TM ASC",
		Limit:   "200",
		Props: map[string]any{
			"mode":     "raw",
			"interval": "1",
			"qtype":    int32(0),
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := req.Encode()
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}
