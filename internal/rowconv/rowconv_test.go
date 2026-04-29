package rowconv

import (
	"testing"
	"time"
)

func TestTimeParsesMillisecondSQLTimestamp(t *testing.T) {
	got := Time("2026-01-02 03:04:05.123")
	want := time.Date(2026, 1, 2, 3, 4, 5, 123000000, time.Local)
	if !got.Equal(want) {
		t.Fatalf("time=%s want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}
