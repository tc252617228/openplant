package model

import (
	"fmt"
	"strings"
)

type ReplicationSyncMode int32

const (
	ReplicationSyncLoose      ReplicationSyncMode = 0
	ReplicationSyncPreserveID ReplicationSyncMode = 1
)

type ReplicationTransform int32

const (
	ReplicationTransformPreserveRole ReplicationTransform = 0
	ReplicationTransformCalcAsDAS    ReplicationTransform = 1
)

const ReplicationBackfillMaxDays int32 = 30

type ReplicationIssue struct {
	Field   string
	Message string
}

type ReplicationIssues []ReplicationIssue

func (m ReplicationSyncMode) Valid() bool {
	return m == ReplicationSyncLoose || m == ReplicationSyncPreserveID
}

func (m ReplicationSyncMode) String() string {
	switch m {
	case ReplicationSyncLoose:
		return "loose"
	case ReplicationSyncPreserveID:
		return "preserve_id"
	default:
		return fmt.Sprintf("ReplicationSyncMode(%d)", m)
	}
}

func (m ReplicationTransform) Valid() bool {
	return m == ReplicationTransformPreserveRole || m == ReplicationTransformCalcAsDAS
}

func (m ReplicationTransform) String() string {
	switch m {
	case ReplicationTransformPreserveRole:
		return "preserve_role"
	case ReplicationTransformCalcAsDAS:
		return "calc_as_das"
	default:
		return fmt.Sprintf("ReplicationTransform(%d)", m)
	}
}

func (issues ReplicationIssues) Error() string {
	if len(issues) == 0 {
		return ""
	}
	parts := make([]string, 0, len(issues))
	for _, issue := range issues {
		parts = append(parts, issue.Field+": "+issue.Message)
	}
	return strings.Join(parts, "; ")
}

func (r Replicator) Issues() ReplicationIssues {
	issues := make(ReplicationIssues, 0)
	if strings.TrimSpace(r.Name) == "" {
		issues = append(issues, ReplicationIssue{Field: "RN", Message: "replicator name is required"})
	}
	if r.Port < 0 {
		issues = append(issues, ReplicationIssue{Field: "PO", Message: "target port cannot be negative"})
	}
	if r.SourcePort < 0 {
		issues = append(issues, ReplicationIssue{Field: "SP", Message: "source port cannot be negative"})
	}
	if !r.SyncMode.Valid() {
		issues = append(issues, ReplicationIssue{Field: "SY", Message: "sync mode must be 0 or 1"})
	}
	if r.TimeLimitDays < 0 {
		issues = append(issues, ReplicationIssue{Field: "TL", Message: "backfill time limit cannot be negative"})
	}
	if r.TimeLimitDays > ReplicationBackfillMaxDays {
		issues = append(issues, ReplicationIssue{Field: "TL", Message: "backfill time limit cannot exceed 30 days"})
	}
	return issues
}

func (r Replicator) Validate() error {
	if issues := r.Issues(); len(issues) > 0 {
		return issues
	}
	return nil
}

func (r RepItem) Issues() ReplicationIssues {
	issues := make(ReplicationIssues, 0)
	if strings.TrimSpace(r.PointName) == "" {
		issues = append(issues, ReplicationIssue{Field: "PN", Message: "source point name is required"})
	}
	if strings.TrimSpace(r.TargetName) == "" {
		issues = append(issues, ReplicationIssue{Field: "TN", Message: "target point name is required"})
	}
	if !r.Transform.Valid() {
		issues = append(issues, ReplicationIssue{Field: "XF", Message: "transform must be 0 or 1"})
	}
	return issues
}

func (r RepItem) Validate() error {
	if issues := r.Issues(); len(issues) > 0 {
		return issues
	}
	return nil
}
