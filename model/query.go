package model

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type TimeRange struct {
	Begin time.Time
	End   time.Time
}

func (r TimeRange) Validate() error {
	if r.Begin.IsZero() || r.End.IsZero() {
		return fmt.Errorf("time range requires begin and end")
	}
	if !r.End.After(r.Begin) {
		return fmt.Errorf("time range end must be after begin")
	}
	return nil
}

type Interval string

func (i Interval) ValidateRequired() error {
	if i == "" {
		return fmt.Errorf("interval is required")
	}
	return i.ValidateOptional()
}

func (i Interval) ValidateOptional() error {
	if i == "" {
		return nil
	}
	s := string(i)
	n := 0
	for n < len(s) && s[n] >= '0' && s[n] <= '9' {
		n++
	}
	if n == 0 {
		return fmt.Errorf("interval requires integer amount")
	}
	amount, err := strconv.Atoi(s[:n])
	if err != nil || amount <= 0 {
		return fmt.Errorf("interval amount must be positive")
	}
	unit := strings.ToLower(s[n:])
	switch unit {
	case "ms", "s", "m", "h", "d", "w", "q", "y":
		return nil
	default:
		return fmt.Errorf("unsupported interval unit %q", unit)
	}
}

type ArchiveMode string

const (
	ModeRaw   ArchiveMode = "raw"
	ModeArch  ArchiveMode = "arch"
	ModeSpan  ArchiveMode = "span"
	ModePlot  ArchiveMode = "plot"
	ModeFlow  ArchiveMode = "flow"
	ModeMax   ArchiveMode = "max"
	ModeMin   ArchiveMode = "min"
	ModeAvg   ArchiveMode = "avg"
	ModeMean  ArchiveMode = "mean"
	ModeStDev ArchiveMode = "stdev"
	ModeSum   ArchiveMode = "sum"
)

func (m ArchiveMode) Validate() error {
	switch m {
	case ModeRaw, ModeArch, ModeSpan, ModePlot, ModeFlow, ModeMax, ModeMin, ModeAvg, ModeMean, ModeStDev, ModeSum:
		return nil
	default:
		return fmt.Errorf("unsupported archive mode %q", m)
	}
}

func (m ArchiveMode) RequiresInterval() bool {
	switch m {
	case ModeSpan, ModePlot, ModeFlow, ModeMax, ModeMin, ModeAvg, ModeMean, ModeStDev, ModeSum:
		return true
	default:
		return false
	}
}

type QualityFilter int8

const (
	QualityNone              QualityFilter = 0
	QualityDropBad           QualityFilter = 1
	QualityDropTimeout       QualityFilter = 2
	QualityDropBadAndTimeout QualityFilter = 3
)

func (q QualityFilter) Validate() error {
	if q < QualityNone || q > QualityDropBadAndTimeout {
		return fmt.Errorf("unsupported quality filter %d", q)
	}
	return nil
}

type Sample struct {
	ID     PointID
	GN     GN
	Type   PointType
	Format int16
	Time   time.Time
	Status DS
	Value  Value
}

type StatSample struct {
	ID      PointID
	GN      GN
	Time    time.Time
	Status  DS
	Flow    float64
	Avg     float64
	Max     float64
	Min     float64
	MaxTime time.Time
	MinTime time.Time
	Mean    float64
	Sum     float64
}
