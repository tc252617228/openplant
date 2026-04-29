package system

import (
	"github.com/tc252617228/openplant/model"
)

type PointTemplate struct {
	Metric       Metric
	GN           model.GN
	Name         string
	Description  string
	Source       model.PointSource
	Type         model.PointType
	Unit         string
	Format       int16
	Archived     bool
	AlarmCode    model.AlarmCode
	AlarmLevel   model.AlarmPriority
	RangeLower   float64
	RangeUpper   float64
	Limits       model.AlarmLimits
	Deadband     float64
	DeadbandType model.DeadbandType
	Compression  model.PointCompression
	CalcType     int8
	CalcOrder    int8
	ScaleFactor  float64
	Offset       float64
	Expression   string
}

func PointTemplates(db model.DatabaseName) ([]PointTemplate, error) {
	if err := db.Validate(); err != nil {
		return nil, err
	}
	out := make([]PointTemplate, 0, len(metricCatalog))
	for _, spec := range metricCatalog {
		out = append(out, pointTemplate(spec, db))
	}
	return out, nil
}

func LookupPointTemplate(metric Metric, db model.DatabaseName) (PointTemplate, bool) {
	if err := db.Validate(); err != nil {
		return PointTemplate{}, false
	}
	spec, ok := lookupMetricSpec(metric)
	if !ok {
		return PointTemplate{}, false
	}
	return pointTemplate(spec, db), true
}

func (t PointTemplate) PointConfig() model.PointConfig {
	return model.PointConfig{
		Source:       t.Source,
		Type:         t.Type,
		Name:         t.Name,
		Description:  t.Description,
		AlarmCode:    t.AlarmCode,
		AlarmLevel:   t.AlarmLevel,
		Archived:     t.Archived,
		Unit:         t.Unit,
		Format:       t.Format,
		RangeLower:   t.RangeLower,
		RangeUpper:   t.RangeUpper,
		Limits:       t.Limits,
		Deadband:     t.Deadband,
		DeadbandType: t.DeadbandType,
		Compression:  t.Compression,
		CalcType:     t.CalcType,
		CalcOrder:    t.CalcOrder,
		ScaleFactor:  t.ScaleFactor,
		Offset:       t.Offset,
		Expression:   t.Expression,
		GN:           t.GN,
	}
}

func pointTemplate(spec metricSpec, db model.DatabaseName) PointTemplate {
	info := spec.info(db)
	t := PointTemplate{
		Metric:       spec.metric,
		GN:           spec.metric.GN(db),
		Name:         string(spec.metric),
		Description:  info.Description,
		Source:       model.SourceCalc,
		Type:         model.TypeAX,
		Unit:         info.Unit,
		Archived:     true,
		RangeLower:   0,
		RangeUpper:   100,
		Deadband:     0.2,
		DeadbandType: model.DeadbandPCT,
		Compression:  model.PointCompressionDeadband,
		CalcType:     1,
		CalcOrder:    1,
		ScaleFactor:  1,
		Expression:   info.Formula,
	}
	switch spec.metric {
	case MetricDatabaseLoad:
		t.AlarmCode = model.AlarmLL | model.AlarmHL | model.AlarmZH
		t.Unit = "%"
		t.Limits = model.AlarmLimits{LL: 30, HL: 40, ZH: 50}
	case MetricLoad:
		t.AlarmCode = model.AlarmLimitMask
		t.Unit = "%"
		t.Limits = model.AlarmLimits{
			LL: 50, HL: 60,
			ZL: 40, ZH: 70,
			L3: 30, H3: 80,
			L4: 20, H4: 90,
		}
	case MetricRate:
		t.CalcType = 0
	}
	return t
}
