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

func TestTimePreservesSubMillisecondFloatPrecision(t *testing.T) {
	got := Time(100.0015)
	if got.Unix() != 100 {
		t.Fatalf("seconds=%d want 100", got.Unix())
	}
	if got.Nanosecond() < 1_499_000 || got.Nanosecond() > 1_501_000 {
		t.Fatalf("nanosecond=%d want about 1500000", got.Nanosecond())
	}
}
