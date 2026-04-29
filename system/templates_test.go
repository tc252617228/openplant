package system

import (
	"testing"

	"github.com/tc252617228/openplant/model"
)

func TestPointTemplatesMirrorSystemCatalog(t *testing.T) {
	templates, err := PointTemplates("W3")
	if err != nil {
		t.Fatalf("PointTemplates failed: %v", err)
	}
	if len(templates) != len(Metrics()) {
		t.Fatalf("templates=%d metrics=%d", len(templates), len(Metrics()))
	}
	first := templates[0]
	if first.Source != model.SourceCalc || first.Type != model.TypeAX || first.GN != "W3.SYS.CACHEQ" {
		t.Fatalf("unexpected first template: %#v", first)
	}
	if first.Expression != `return op.cacheq("W3")` {
		t.Fatalf("expression=%q", first.Expression)
	}
}

func TestLookupPointTemplateLoadAlarmLimits(t *testing.T) {
	template, ok := LookupPointTemplate(MetricLoad, "W3")
	if !ok {
		t.Fatalf("LOAD template not found")
	}
	if template.AlarmCode != model.AlarmLimitMask {
		t.Fatalf("alarm code=%d", template.AlarmCode)
	}
	if template.Limits.LL != 50 || template.Limits.H4 != 90 || template.Unit != "%" {
		t.Fatalf("unexpected load limits: %#v", template)
	}
	cfg := template.PointConfig()
	if cfg.GN != "W3.SYS.LOAD" || cfg.Expression != "return op.load()" || cfg.Source != model.SourceCalc {
		t.Fatalf("unexpected point config: %#v", cfg)
	}
}

func TestLookupPointTemplateRejectsBadInput(t *testing.T) {
	if _, ok := LookupPointTemplate("NOPE", "W3"); ok {
		t.Fatalf("unexpected NOPE template")
	}
	if _, ok := LookupPointTemplate(MetricLoad, "bad db"); ok {
		t.Fatalf("unexpected template for invalid db")
	}
	if _, err := PointTemplates("bad db"); err == nil {
		t.Fatalf("expected invalid db error")
	}
}
