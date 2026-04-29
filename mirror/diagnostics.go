package mirror

import (
	"fmt"
	"sort"

	"github.com/tc252617228/openplant/calc"
	"github.com/tc252617228/openplant/model"
)

type SyncKind string

const (
	SyncArchive  SyncKind = "archive"
	SyncRealtime SyncKind = "realtime"
)

type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

type Issue struct {
	Severity Severity
	Scope    string
	Name     string
	Field    string
	Message  string
}

type Config struct {
	Replicators       []model.Replicator
	Items             []model.RepItem
	CalculationPoints []model.PointConfig
}

type SyncMonitor struct {
	GN         model.GN
	Kind       SyncKind
	Expression string
	References []model.GN
}

func Diagnose(cfg Config) []Issue {
	issues := make([]Issue, 0)
	issues = append(issues, replicatorIssues(cfg.Replicators)...)
	issues = append(issues, repItemIssues(cfg.Items)...)
	if len(cfg.Items) > 0 && len(cfg.Replicators) == 0 {
		issues = append(issues, Issue{
			Severity: SeverityWarning,
			Scope:    "RepItem",
			Field:    "RN",
			Message:  "replication items are configured but no replicator records were provided",
		})
	}
	return issues
}

func SyncMonitors(points []model.PointConfig) []SyncMonitor {
	monitors := make([]SyncMonitor, 0)
	for _, point := range points {
		if point.GN == "" || point.Expression == "" {
			continue
		}
		refs := calc.FormulaReferences(point.Expression)
		if calc.UsesFunction(point.Expression, "op.ar_sync_time") {
			monitors = append(monitors, SyncMonitor{
				GN:         point.GN,
				Kind:       SyncArchive,
				Expression: point.Expression,
				References: refs,
			})
		}
		if calc.UsesFunction(point.Expression, "op.rt_sync_time") {
			monitors = append(monitors, SyncMonitor{
				GN:         point.GN,
				Kind:       SyncRealtime,
				Expression: point.Expression,
				References: refs,
			})
		}
	}
	sort.Slice(monitors, func(i, j int) bool {
		if monitors[i].GN == monitors[j].GN {
			return monitors[i].Kind < monitors[j].Kind
		}
		return monitors[i].GN < monitors[j].GN
	})
	return monitors
}

func replicatorIssues(items []model.Replicator) []Issue {
	issues := make([]Issue, 0)
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		for _, problem := range item.Issues() {
			issues = append(issues, Issue{
				Severity: SeverityError,
				Scope:    "Replicator",
				Name:     item.Name,
				Field:    problem.Field,
				Message:  problem.Message,
			})
		}
		if item.Name == "" {
			continue
		}
		if _, ok := seen[item.Name]; ok {
			issues = append(issues, Issue{
				Severity: SeverityError,
				Scope:    "Replicator",
				Name:     item.Name,
				Field:    "RN",
				Message:  "duplicate replicator name",
			})
			continue
		}
		seen[item.Name] = struct{}{}
		if item.ArchiveBackfill && item.TimeLimitDays == 0 {
			issues = append(issues, Issue{
				Severity: SeverityWarning,
				Scope:    "Replicator",
				Name:     item.Name,
				Field:    "TL",
				Message:  "archive backfill is enabled without an explicit day limit",
			})
		}
	}
	return issues
}

func repItemIssues(items []model.RepItem) []Issue {
	issues := make([]Issue, 0)
	seenSource := make(map[string]struct{}, len(items))
	seenTarget := make(map[string]string, len(items))
	for _, item := range items {
		for _, problem := range item.Issues() {
			issues = append(issues, Issue{
				Severity: SeverityError,
				Scope:    "RepItem",
				Name:     item.PointName,
				Field:    problem.Field,
				Message:  problem.Message,
			})
		}
		if item.PointName != "" {
			if _, ok := seenSource[item.PointName]; ok {
				issues = append(issues, Issue{
					Severity: SeverityError,
					Scope:    "RepItem",
					Name:     item.PointName,
					Field:    "PN",
					Message:  "duplicate replication source point",
				})
			}
			seenSource[item.PointName] = struct{}{}
		}
		if item.TargetName != "" {
			if previous, ok := seenTarget[item.TargetName]; ok && previous != item.PointName {
				issues = append(issues, Issue{
					Severity: SeverityWarning,
					Scope:    "RepItem",
					Name:     item.PointName,
					Field:    "TN",
					Message:  fmt.Sprintf("target point is also used by %s", previous),
				})
			} else {
				seenTarget[item.TargetName] = item.PointName
			}
		}
	}
	return issues
}
