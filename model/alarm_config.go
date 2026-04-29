package model

import (
	"fmt"
	"strings"
)

const (
	AlarmLimitMask  AlarmCode = AlarmLL | AlarmHL | AlarmZL | AlarmZH | AlarmL3 | AlarmH3 | AlarmL4 | AlarmH4
	AlarmAnalogMask AlarmCode = AlarmLimitMask | AlarmChange
)

type AlarmLimits struct {
	LL float64
	HL float64
	ZL float64
	ZH float64
	L3 float64
	H3 float64
	L4 float64
	H4 float64
}

type AlarmColors struct {
	LL int32
	ZL int32
	L3 int32
	L4 int32
	HL int32
	ZH int32
	H3 int32
	H4 int32
}

func BuildAlarmCode(flags ...AlarmCode) AlarmCode {
	var code AlarmCode
	for _, flag := range flags {
		code |= flag
	}
	return code
}

func (c AlarmCode) HasAny(flag AlarmCode) bool {
	return c&flag != 0
}

func (c AlarmCode) ValidAnalog() bool {
	if c&^AlarmAnalogMask != 0 {
		return false
	}
	return !c.Has(AlarmChange) || !c.HasAny(AlarmLimitMask)
}

func (c AlarmCode) ValidDX() bool {
	return c == AlarmNone || c == DXAlarmToZero || c == DXAlarmToOne || c == DXAlarmToggle
}

func (t PointType) SupportsAnalogAlarms() bool {
	switch t {
	case TypeAX, TypeI2, TypeI4, TypeR8, TypeI8:
		return true
	default:
		return false
	}
}

func (t PointType) SupportsDXAlarms() bool {
	return t == TypeDX
}

func (t PointType) SupportsAlarms() bool {
	return t.SupportsAnalogAlarms() || t.SupportsDXAlarms()
}

func (c AlarmCode) ValidForPointType(typ PointType) bool {
	switch {
	case typ.SupportsAnalogAlarms():
		return c.ValidAnalog()
	case typ.SupportsDXAlarms():
		return c.ValidDX()
	default:
		return c == AlarmNone
	}
}

func (c AlarmCode) EnabledAnalogAlarms() []AlarmCode {
	order := []AlarmCode{AlarmLL, AlarmHL, AlarmZL, AlarmZH, AlarmL3, AlarmH3, AlarmL4, AlarmH4, AlarmChange}
	out := make([]AlarmCode, 0, len(order))
	for _, alarm := range order {
		if c.Has(alarm) {
			out = append(out, alarm)
		}
	}
	return out
}

func (c AlarmCode) AnalogName() string {
	switch c {
	case AlarmNone:
		return "None"
	case AlarmLL:
		return "LL"
	case AlarmHL:
		return "HL"
	case AlarmZL:
		return "ZL"
	case AlarmZH:
		return "ZH"
	case AlarmL3:
		return "L3"
	case AlarmH3:
		return "H3"
	case AlarmL4:
		return "L4"
	case AlarmH4:
		return "H4"
	case AlarmChange:
		return "Change"
	default:
		if c.ValidAnalog() {
			names := make([]string, 0, 9)
			for _, alarm := range c.EnabledAnalogAlarms() {
				names = append(names, alarm.AnalogName())
			}
			return strings.Join(names, "|")
		}
		return fmt.Sprintf("AlarmCode(%d)", c)
	}
}

func (c AlarmCode) AnalogLabel() string {
	switch c {
	case AlarmNone:
		return "Not alarm"
	case AlarmLL:
		return "Low Limit"
	case AlarmHL:
		return "High Limit"
	case AlarmZL:
		return "Low 2 Limit"
	case AlarmZH:
		return "High 2 Limit"
	case AlarmL3:
		return "Low 3 Limit"
	case AlarmH3:
		return "High 3 Limit"
	case AlarmL4:
		return "Low 4 Limit"
	case AlarmH4:
		return "High 4 Limit"
	case AlarmChange:
		return "Change alarm"
	default:
		if c.ValidAnalog() {
			labels := make([]string, 0, 9)
			for _, alarm := range c.EnabledAnalogAlarms() {
				labels = append(labels, alarm.AnalogLabel())
			}
			return strings.Join(labels, "|")
		}
		return fmt.Sprintf("AlarmCode(%d)", c)
	}
}

func (c AlarmCode) DXName() string {
	switch c {
	case AlarmNone:
		return "None"
	case DXAlarmToZero:
		return "ToZero"
	case DXAlarmToOne:
		return "ToOne"
	case DXAlarmToggle:
		return "Toggle"
	default:
		return fmt.Sprintf("AlarmCode(%d)", c)
	}
}

func (c AlarmCode) DXLabel() string {
	switch c {
	case AlarmNone:
		return "Not alarm"
	case DXAlarmToZero:
		return "To 0 alarm"
	case DXAlarmToOne:
		return "To 1 alarm"
	case DXAlarmToggle:
		return "Toggle alarm"
	default:
		return fmt.Sprintf("AlarmCode(%d)", c)
	}
}

func (c AlarmCode) NameForPointType(typ PointType) string {
	if typ.SupportsDXAlarms() {
		return c.DXName()
	}
	return c.AnalogName()
}

func (c AlarmCode) LabelForPointType(typ PointType) string {
	if typ.SupportsDXAlarms() {
		return c.DXLabel()
	}
	return c.AnalogLabel()
}

func (l AlarmLimits) Value(code AlarmCode) (float64, bool) {
	switch code {
	case AlarmLL:
		return l.LL, true
	case AlarmHL:
		return l.HL, true
	case AlarmZL:
		return l.ZL, true
	case AlarmZH:
		return l.ZH, true
	case AlarmL3:
		return l.L3, true
	case AlarmH3:
		return l.H3, true
	case AlarmL4:
		return l.L4, true
	case AlarmH4:
		return l.H4, true
	default:
		return 0, false
	}
}

func (c AlarmColors) Color(code AlarmCode) (int32, bool) {
	switch code {
	case AlarmLL:
		return c.LL, true
	case AlarmZL:
		return c.ZL, true
	case AlarmL3:
		return c.L3, true
	case AlarmL4:
		return c.L4, true
	case AlarmHL:
		return c.HL, true
	case AlarmZH:
		return c.ZH, true
	case AlarmH3:
		return c.H3, true
	case AlarmH4:
		return c.H4, true
	case AlarmChange:
		return 0xFF0000, true
	default:
		return 0, false
	}
}

func AlarmColorHex(color int32) string {
	return fmt.Sprintf("#%06x", uint32(color)&0x00ffffff)
}

func DefaultAlarmColors() AlarmColors {
	return AlarmColors{
		LL: 0xFF0000,
		ZL: 0xCC0000,
		L3: 0x990000,
		L4: 0x660000,
		HL: 0xFF0000,
		ZH: 0xCC0000,
		H3: 0x990000,
		H4: 0x660000,
	}
}

func ValidateAnalogAlarmLimits(code AlarmCode, limits AlarmLimits) error {
	if code&^AlarmAnalogMask != 0 {
		return fmt.Errorf("unsupported analog alarm code bits: %d", code&^AlarmAnalogMask)
	}
	if code.Has(AlarmChange) && code.HasAny(AlarmLimitMask) {
		return fmt.Errorf("change alarm cannot be combined with limit alarms")
	}
	order := []struct {
		code  AlarmCode
		name  string
		value float64
	}{
		{AlarmL4, "L4", limits.L4},
		{AlarmL3, "L3", limits.L3},
		{AlarmZL, "ZL", limits.ZL},
		{AlarmLL, "LL", limits.LL},
		{AlarmHL, "HL", limits.HL},
		{AlarmZH, "ZH", limits.ZH},
		{AlarmH3, "H3", limits.H3},
		{AlarmH4, "H4", limits.H4},
	}
	var prevName string
	var prevValue float64
	havePrev := false
	for _, limit := range order {
		if !code.Has(limit.code) {
			continue
		}
		if havePrev && limit.value <= prevValue {
			return fmt.Errorf("alarm limit %s must be greater than %s", limit.name, prevName)
		}
		prevName = limit.name
		prevValue = limit.value
		havePrev = true
	}
	return nil
}
