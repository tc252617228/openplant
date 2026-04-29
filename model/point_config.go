package model

import "time"

type DeadbandType int8

const (
	DeadbandPCT DeadbandType = 0
	DeadbandENG DeadbandType = 1
)

func (d DeadbandType) String() string {
	switch d {
	case DeadbandPCT:
		return "PCT"
	case DeadbandENG:
		return "ENG"
	default:
		return "UNKNOWN"
	}
}

func (d DeadbandType) Valid() bool {
	return d == DeadbandPCT || d == DeadbandENG
}

type PointCompression int8

const (
	PointCompressionDeadband PointCompression = 0
	PointCompressionLinear   PointCompression = 1
	PointCompressionNone     PointCompression = 2
)

func (c PointCompression) String() string {
	switch c {
	case PointCompressionDeadband:
		return "DEADBAND"
	case PointCompressionLinear:
		return "LINEAR"
	case PointCompressionNone:
		return "NONE"
	default:
		return "UNKNOWN"
	}
}

func (c PointCompression) Valid() bool {
	return c == PointCompressionDeadband || c == PointCompressionLinear || c == PointCompressionNone
}

type PointConfig struct {
	ID               PointID
	UUID             UUID
	NodeID           NodeID
	DeviceID         DeviceID
	Source           PointSource
	Type             PointType
	Name             string
	Alias            string
	Description      string
	Keyword          string
	Security         SecurityGroups
	Resolution       int16
	Processor        int16
	HardwareAddress  int32
	Channel          int16
	SignalType       string
	SignalAddress    string
	AlarmCode        AlarmCode
	AlarmLevel       AlarmPriority
	Archived         bool
	Offline          bool
	Flags            int32
	SetDescription   string
	ResetDescription string
	Unit             string
	Format           int16
	InitialValue     float64
	RangeLower       float64
	RangeUpper       float64
	Limits           AlarmLimits
	Colors           AlarmColors
	Deadband         float64
	DeadbandType     DeadbandType
	Compression      PointCompression
	StatType         int8
	StatPeriod       int16
	StatOffset       int16
	CalcType         int8
	CalcOrder        int8
	ScaleFactor      float64
	Offset           float64
	ConfigTime       time.Time
	Expression       string
	GN               GN
}

func (p PointConfig) Point() Point {
	return Point{
		ID:          p.ID,
		UUID:        p.UUID,
		NodeID:      p.NodeID,
		Source:      p.Source,
		Type:        p.Type,
		Name:        p.Name,
		Alias:       p.Alias,
		Description: p.Description,
		Keyword:     p.Keyword,
		Security:    p.Security,
		Resolution:  p.Resolution,
		AlarmCode:   p.AlarmCode,
		AlarmLevel:  p.AlarmLevel,
		Archived:    p.Archived,
		Unit:        p.Unit,
		Format:      p.Format,
		ConfigTime:  p.ConfigTime,
		Expression:  p.Expression,
		GN:          p.GN,
	}
}
