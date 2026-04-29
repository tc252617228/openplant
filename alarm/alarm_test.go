package alarm

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

func TestHistoryQueryRequiresPointAndTimeBounds(t *testing.T) {
	begin := time.Now().Add(-time.Hour)
	end := time.Now()
	valid := HistoryQuery{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: end},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}
	withoutPoint := valid
	withoutPoint.IDs = nil
	if err := withoutPoint.Validate(); err == nil {
		t.Fatalf("expected missing point scope to be rejected")
	}
	withoutTime := valid
	withoutTime.Range = model.TimeRange{}
	if err := withoutTime.Validate(); err == nil {
		t.Fatalf("expected missing time range to be rejected")
	}
}

func TestActiveSQLUsesBoundedReadonlySQL(t *testing.T) {
	first := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	alarmTime := first.Add(10 * time.Minute)
	update := first.Add(20 * time.Minute)
	fake := &fakeQueryer{rows: []sqlapi.Row{{
		"ID": int32(1001), "GN": "W3.N.P1", "RT": int8(model.TypeR8), "AL": int8(2), "AC": int32(7),
		"PN": "P1", "AN": "Alias", "ED": "Description", "EU": "MW",
		"AP": int8(model.AlarmPriorityRed), "LC": int32(model.AlarmHL), "C5": int32(0xFF0000),
		"TF": first, "TA": alarmTime, "TM": update, "DS": int16(model.DSInAlarm | model.DSAlarmBit3), "AV": float64(12.5),
	}}}
	svc := NewService(Options{Queryer: fake})

	alarms, err := svc.ActiveSQL(context.Background(), "W3", 10)
	if err != nil {
		t.Fatalf("ActiveSQL failed: %v", err)
	}
	if len(alarms) != 1 {
		t.Fatalf("alarms=%d want 1", len(alarms))
	}
	alarm := alarms[0]
	value, ok := alarm.Value.Float64()
	if !ok || value != 12.5 {
		t.Fatalf("value=%#v ok=%v", alarm.Value.Interface(), ok)
	}
	if alarm.ID != 1001 || alarm.GN != "W3.N.P1" || alarm.Type != model.TypeR8 || alarm.Level != 2 || alarm.Color != 7 || alarm.UpdateTime != update {
		t.Fatalf("alarm=%#v", alarm)
	}
	if alarm.Name != "P1" || alarm.Alias != "Alias" || alarm.Description != "Description" || alarm.Unit != "MW" {
		t.Fatalf("alarm metadata=%#v", alarm)
	}
	if alarm.Priority != model.AlarmPriorityRed || alarm.ConfigCode != model.AlarmHL || alarm.ActiveCode != model.AlarmHL {
		t.Fatalf("alarm config=%#v", alarm)
	}
	for _, want := range []string{
		`FROM W3.Alarm`,
		`"PN"`,
		`"AP"`,
		`"LC"`,
		`"C8"`,
		`WHERE "ID" >= 0`,
		`ORDER BY "TM" DESC,"ID" ASC`,
		`LIMIT 10`,
	} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestAlarmFromOPConsoleProjectionDerivesActiveColor(t *testing.T) {
	update := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	row := sqlapi.Row{
		"ID": int32(1001), "GN": "W3.N.P1", "RT": int8(model.TypeR8),
		"PN": "P1", "AP": int8(model.AlarmPriorityYellow), "LC": int32(model.AlarmHL),
		"C5": int32(0xFF0000), "TM": update, "DS": int16(model.DSInAlarm | model.DSAlarmBit3), "AV": float64(12.5),
	}
	alarm := alarmFromRow(row)
	if alarm.Level != 0 {
		t.Fatalf("undocumented AL should stay zero when not projected: %#v", alarm)
	}
	if alarm.ActiveCode != model.AlarmHL {
		t.Fatalf("ActiveCode=%d want HL", alarm.ActiveCode)
	}
	if color, ok := alarm.DisplayColor(); !ok || color != 0xFF0000 {
		t.Fatalf("DisplayColor=%#x,%v", color, ok)
	}
	if alarm.Color != 0xFF0000 {
		t.Fatalf("derived Color=%#x", alarm.Color)
	}
	if alarm.DisplayLabel() != "High Limit" {
		t.Fatalf("DisplayLabel=%q", alarm.DisplayLabel())
	}
}

func TestHistorySQLUsesBoundedReadonlySQL(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := begin.Add(time.Hour)
	fake := &fakeQueryer{}
	svc := NewService(Options{Queryer: fake})

	_, err := svc.HistorySQL(context.Background(), HistoryQuery{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		GNs:   []model.GN{"W3.N.P1"},
		Range: model.TimeRange{Begin: begin, End: end},
		Limit: 20,
	})
	if err != nil {
		t.Fatalf("HistorySQL failed: %v", err)
	}
	for _, want := range []string{
		`FROM W3.AAlarm`,
		`("ID" IN (1001) OR "GN" IN ('W3.N.P1'))`,
		`"TM" BETWEEN '2026-01-02 03:04:05' AND '2026-01-02 04:04:05'`,
		`ORDER BY "ID" ASC,"TM" ASC`,
		`LIMIT 20`,
	} {
		if !strings.Contains(fake.query, want) {
			t.Fatalf("query missing %q: %s", want, fake.query)
		}
	}
}

func TestHistorySQLPreservesMillisecondTimeBounds(t *testing.T) {
	begin := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	end := time.Date(2026, 1, 2, 3, 4, 6, 987654321, time.UTC)
	fake := &fakeQueryer{}
	svc := NewService(Options{Queryer: fake})

	_, err := svc.HistorySQL(context.Background(), HistoryQuery{
		DB:    "W3",
		IDs:   []model.PointID{1001},
		Range: model.TimeRange{Begin: begin, End: end},
	})
	if err != nil {
		t.Fatalf("HistorySQL failed: %v", err)
	}
	want := `"TM" BETWEEN '2026-01-02 03:04:05.123' AND '2026-01-02 03:04:06.987'`
	if !strings.Contains(fake.query, want) {
		t.Fatalf("query missing millisecond bounds %q: %s", want, fake.query)
	}
}

func TestActiveSQLDefaultLimit(t *testing.T) {
	fake := &fakeQueryer{}
	svc := NewService(Options{Queryer: fake})
	if _, err := svc.ActiveSQL(context.Background(), "W3", 0); err != nil {
		t.Fatalf("ActiveSQL failed: %v", err)
	}
	if !strings.Contains(fake.query, `LIMIT 1000`) {
		t.Fatalf("query missing default limit: %s", fake.query)
	}
}
