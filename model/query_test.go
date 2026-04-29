package model

import (
	"testing"
	"time"
)

func TestTimeRangeRequiresBoundedRange(t *testing.T) {
	now := time.Now()
	if err := (TimeRange{Begin: now, End: now.Add(time.Second)}).Validate(); err != nil {
		t.Fatalf("valid range rejected: %v", err)
	}
	if err := (TimeRange{Begin: now, End: now}).Validate(); err == nil {
		t.Fatalf("expected invalid zero-length range")
	}
}

func TestIntervalValidation(t *testing.T) {
	for _, value := range []Interval{"1ms", "1s", "5m", "2h", "1d", "1w", "1q", "1y"} {
		if err := value.ValidateRequired(); err != nil {
			t.Fatalf("interval %q rejected: %v", value, err)
		}
	}
	for _, value := range []Interval{"", "0s", "1.5h", "10x"} {
		if err := value.ValidateRequired(); err == nil {
			t.Fatalf("interval %q should be rejected", value)
		}
	}
}
