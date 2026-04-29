package model

import "fmt"

type DS uint16

const (
	DSDXValue      DS = 1 << 0
	DSAlarmBit1    DS = 1 << 1
	DSAlarmBit2    DS = 1 << 2
	DSAlarmBit3    DS = 1 << 3
	DSAlarmBit4    DS = 1 << 4
	DSUnackedAlarm DS = 1 << 5
	DSAlarmBlocked DS = 1 << 6
	DSInAlarm      DS = 1 << 7
	DSForced       DS = 1 << 8
	DSBadQuality   DS = 1 << 9
	DSControlBit0  DS = 1 << 10
	DSControlBit1  DS = 1 << 11
	DSHasControl   DS = 1 << 12
	DSDeviceTagged DS = 1 << 13
	DSInitial      DS = 1 << 14
	DSTimeout      DS = 1 << 15
)

const dsNilMask = DSBadQuality | DSInitial | DSTimeout

type DSQualityState uint8

const (
	DSQualityGood DSQualityState = iota
	DSQualityBad
	DSQualityForced
	DSQualityTimeout
	DSQualityNull
)

func (q DSQualityState) String() string {
	switch q {
	case DSQualityGood:
		return "Good"
	case DSQualityBad:
		return "Bad"
	case DSQualityForced:
		return "Force"
	case DSQualityTimeout:
		return "Timeout"
	case DSQualityNull:
		return "NULL"
	default:
		return "Unknown"
	}
}

type ControlState uint8

const (
	ControlNone ControlState = iota
	ControlIssued
	ControlFault
	ControlTimeout
)

func DSFromInt16(v int16) DS {
	return DS(uint16(v))
}

func (s DS) Int16() int16 {
	return int16(uint16(s))
}

func (s DS) DXValue() bool { return s&DSDXValue != 0 }
func (s DS) InAlarm() bool { return s&DSInAlarm != 0 }
func (s DS) UnackedAlarm() bool {
	return s&DSUnackedAlarm != 0
}
func (s DS) AlarmBlocked() bool { return s&DSAlarmBlocked != 0 }
func (s DS) Forced() bool       { return s&DSForced != 0 }
func (s DS) BadQuality() bool   { return s&DSBadQuality != 0 }
func (s DS) HasControl() bool   { return s&DSHasControl != 0 }
func (s DS) DeviceTagged() bool { return s&DSDeviceTagged != 0 }
func (s DS) Initial() bool      { return s&DSInitial != 0 }
func (s DS) Timeout() bool      { return s&DSTimeout != 0 }
func (s DS) NilV5() bool        { return s&dsNilMask == dsNilMask }
func (s DS) Good() bool         { return !s.BadQuality() && !s.Timeout() && !s.NilV5() }
func (s DS) AlarmBits() uint8   { return uint8((s >> 1) & 0x0f) }

func (s DS) BinaryString() string {
	raw := fmt.Sprintf("%016b", uint16(s))
	return raw[:4] + " " + raw[4:8] + " " + raw[8:12] + " " + raw[12:]
}

func (s DS) QualityState() DSQualityState {
	switch {
	case s.NilV5():
		return DSQualityNull
	case s.Timeout():
		return DSQualityTimeout
	case s.Forced():
		return DSQualityForced
	case s.BadQuality():
		return DSQualityBad
	default:
		return DSQualityGood
	}
}

func (s DS) ControlState() ControlState {
	b10 := s&DSControlBit0 != 0
	b11 := s&DSControlBit1 != 0
	switch {
	case b10 && b11:
		return ControlTimeout
	case !b10 && b11:
		return ControlIssued
	case b10 && !b11:
		return ControlFault
	default:
		return ControlNone
	}
}

type AlarmState uint8

const (
	AlarmStateNormal AlarmState = iota
	AlarmStateActive
	AlarmStateRestoredUnacked
)

func (s AlarmState) String() string {
	switch s {
	case AlarmStateNormal:
		return "Normal"
	case AlarmStateActive:
		return "Alarm"
	case AlarmStateRestoredUnacked:
		return "RestoreAlarm"
	default:
		return "Unknown"
	}
}

func (s DS) AlarmState() AlarmState {
	switch {
	case s.InAlarm():
		return AlarmStateActive
	case s.UnackedAlarm():
		return AlarmStateRestoredUnacked
	default:
		return AlarmStateNormal
	}
}

type AlarmCode uint16

const (
	AlarmNone   AlarmCode = 0
	AlarmLL     AlarmCode = 1
	AlarmHL     AlarmCode = 2
	AlarmZL     AlarmCode = 4
	AlarmZH     AlarmCode = 8
	AlarmL3     AlarmCode = 16
	AlarmH3     AlarmCode = 32
	AlarmL4     AlarmCode = 64
	AlarmH4     AlarmCode = 128
	AlarmChange AlarmCode = 256

	DXAlarmToZero AlarmCode = 1
	DXAlarmToOne  AlarmCode = 2
	DXAlarmToggle AlarmCode = 3
)

func (c AlarmCode) Has(flag AlarmCode) bool {
	return c&flag == flag
}

func (s DS) AnalogAlarm() AlarmCode {
	if !s.InAlarm() {
		return AlarmNone
	}
	switch s.AlarmBits() {
	case 2:
		return AlarmLL
	case 10:
		return AlarmZL
	case 3:
		return AlarmL3
	case 11:
		return AlarmL4
	case 4:
		return AlarmHL
	case 12:
		return AlarmZH
	case 5:
		return AlarmH3
	case 13:
		return AlarmH4
	case 15:
		return AlarmChange
	default:
		return AlarmNone
	}
}

func (s DS) DXAlarm(lc AlarmCode) AlarmCode {
	state := uint8(0)
	if s.InAlarm() {
		state |= 2
	}
	if s.DXValue() {
		state |= 1
	}
	if state < 2 || !lc.ValidDX() || lc == AlarmNone {
		return AlarmNone
	}
	if lc == DXAlarmToggle {
		return DXAlarmToggle
	}
	if state == 2 && lc == DXAlarmToZero {
		return DXAlarmToZero
	}
	if state == 3 && lc == DXAlarmToOne {
		return DXAlarmToOne
	}
	return AlarmNone
}

func (s DS) ActiveAlarm(typ PointType, lc AlarmCode) AlarmCode {
	if typ == TypeDX {
		return s.DXAlarm(lc)
	}
	return s.AnalogAlarm()
}

type AlarmPriority int8

const (
	AlarmPriorityUnset  AlarmPriority = 0
	AlarmPriorityRed    AlarmPriority = 1
	AlarmPriorityYellow AlarmPriority = 2
	AlarmPriorityWhite  AlarmPriority = 3
	AlarmPriorityGreen  AlarmPriority = 4
)

func (p AlarmPriority) Valid() bool {
	return p >= AlarmPriorityUnset && p <= AlarmPriorityGreen
}

func (p AlarmPriority) String() string {
	switch p {
	case AlarmPriorityUnset:
		return "Unset"
	case AlarmPriorityRed:
		return "Red"
	case AlarmPriorityYellow:
		return "Yellow"
	case AlarmPriorityWhite:
		return "White"
	case AlarmPriorityGreen:
		return "Green"
	default:
		return "Unknown"
	}
}
