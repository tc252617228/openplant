package model

import (
	"fmt"
	"strings"
	"unicode"
)

type DatabaseName string
type GN string
type PointID int32
type NodeID int32
type DASID int32
type DeviceID int32
type UUID int64

type PointSelector struct {
	IDs []PointID
	GNs []GN
}

func (s PointSelector) Empty() bool {
	return len(s.IDs) == 0 && len(s.GNs) == 0
}

func (s PointSelector) ValidateBounded() error {
	if s.Empty() {
		return fmt.Errorf("point selector requires at least one ID or GN")
	}
	for _, id := range s.IDs {
		if id <= 0 {
			return fmt.Errorf("point ID must be positive: %d", id)
		}
	}
	for _, gn := range s.GNs {
		if err := gn.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (db DatabaseName) Validate() error {
	if db == "" {
		return fmt.Errorf("database name is required")
	}
	return validateNameSegment(string(db), "database name")
}

func (gn GN) Validate() error {
	if strings.TrimSpace(string(gn)) == "" {
		return fmt.Errorf("GN is required")
	}
	for _, r := range string(gn) {
		if unicode.IsControl(r) {
			return fmt.Errorf("GN contains control character")
		}
	}
	return nil
}

func (gn GN) Database() DatabaseName {
	parts := strings.SplitN(string(gn), ".", 2)
	if len(parts) == 0 {
		return ""
	}
	return DatabaseName(parts[0])
}

func validateNameSegment(v, label string) error {
	if v == "" {
		return fmt.Errorf("%s is required", label)
	}
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '#':
		default:
			return fmt.Errorf("%s contains invalid character %q", label, r)
		}
	}
	return nil
}
