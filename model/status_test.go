package model

import "testing"

func TestDSBits(t *testing.T) {
	status := DSDXValue | DSAlarmBit2 | DSInAlarm | DSBadQuality | DSInitial | DSTimeout
	if !status.DXValue() || !status.InAlarm() || !status.BadQuality() || !status.Initial() || !status.Timeout() {
		t.Fatalf("expected selected status bits to be set")
	}
	if got := status.AlarmBits(); got != 0b0010 {
		t.Fatalf("AlarmBits()=%04b want 0010", got)
	}
	if !status.NilV5() {
		t.Fatalf("bad quality + initial + timeout should represent V5 nil")
	}
	if status.Good() {
		t.Fatalf("nil/timeout/bad quality must not be good")
	}
	if got := (DSInAlarm | DSAlarmBit2).BinaryString(); got != "0000 0000 1000 0100" {
		t.Fatalf("BinaryString()=%q", got)
	}
}

func TestDSQualityState(t *testing.T) {
	tests := []struct {
		status DS
		want   DSQualityState
	}{
		{0, DSQualityGood},
		{DSBadQuality, DSQualityBad},
		{DSBadQuality | DSForced, DSQualityForced},
		{DSBadQuality | DSTimeout, DSQualityTimeout},
		{DSBadQuality | DSInitial | DSTimeout, DSQualityNull},
	}
	for _, tt := range tests {
		if got := tt.status.QualityState(); got != tt.want {
			t.Fatalf("QualityState(%016b)=%v want %v", tt.status, got, tt.want)
		}
	}
	if DSQualityForced.String() != "Force" {
		t.Fatalf("unexpected forced quality string: %s", DSQualityForced)
	}
}

func TestAlarmCodeHelpers(t *testing.T) {
	allLimits := BuildAlarmCode(AlarmLL, AlarmHL, AlarmZL, AlarmZH, AlarmL3, AlarmH3, AlarmL4, AlarmH4)
	if allLimits != AlarmLimitMask {
		t.Fatalf("all limit code=%d want mask %d", allLimits, AlarmLimitMask)
	}
	if allLimits != 255 {
		t.Fatalf("all limit code=%d want 255", allLimits)
	}
	if !AlarmChange.ValidAnalog() || (AlarmChange | AlarmLL).ValidAnalog() {
		t.Fatalf("analog alarm validation did not enforce change/limit separation")
	}
	if !DXAlarmToZero.ValidDX() || !DXAlarmToOne.ValidDX() || !DXAlarmToggle.ValidDX() || AlarmChange.ValidDX() {
		t.Fatalf("DX alarm validation mismatch")
	}
	got := (AlarmLL | AlarmZH | AlarmChange).EnabledAnalogAlarms()
	want := []AlarmCode{AlarmLL, AlarmZH, AlarmChange}
	if len(got) != len(want) {
		t.Fatalf("EnabledAnalogAlarms length=%d want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("EnabledAnalogAlarms[%d]=%d want %d", i, got[i], want[i])
		}
	}
}

func TestActiveAlarmDecoding(t *testing.T) {
	tests := []struct {
		name string
		ds   DS
		typ  PointType
		lc   AlarmCode
		want AlarmCode
	}{
		{"analog ll", DSInAlarm | DSAlarmBit2, TypeAX, AlarmLimitMask, AlarmLL},
		{"analog zl", DSInAlarm | DSAlarmBit2 | DSAlarmBit4, TypeR8, AlarmLimitMask, AlarmZL},
		{"analog h4", DSInAlarm | DSAlarmBit1 | DSAlarmBit3 | DSAlarmBit4, TypeI4, AlarmLimitMask, AlarmH4},
		{"analog change", DSInAlarm | DSAlarmBit1 | DSAlarmBit2 | DSAlarmBit3 | DSAlarmBit4, TypeAX, AlarmChange, AlarmChange},
		{"normal", DSAlarmBit2, TypeAX, AlarmLimitMask, AlarmNone},
		{"dx to zero", DSInAlarm, TypeDX, DXAlarmToZero, DXAlarmToZero},
		{"dx to one", DSInAlarm | DSDXValue, TypeDX, DXAlarmToOne, DXAlarmToOne},
		{"dx toggle zero", DSInAlarm, TypeDX, DXAlarmToggle, DXAlarmToggle},
		{"dx toggle one", DSInAlarm | DSDXValue, TypeDX, DXAlarmToggle, DXAlarmToggle},
		{"dx lc none", DSInAlarm, TypeDX, AlarmNone, AlarmNone},
		{"dx lc mismatch", DSInAlarm, TypeDX, DXAlarmToOne, AlarmNone},
	}
	for _, tt := range tests {
		if got := tt.ds.ActiveAlarm(tt.typ, tt.lc); got != tt.want {
			t.Fatalf("%s ActiveAlarm=%d want %d", tt.name, got, tt.want)
		}
	}
}

func TestValidateAnalogAlarmLimits(t *testing.T) {
	limits := AlarmLimits{
		L4: -40,
		L3: -30,
		ZL: -20,
		LL: -10,
		HL: 10,
		ZH: 20,
		H3: 30,
		H4: 40,
	}
	if err := ValidateAnalogAlarmLimits(AlarmLimitMask, limits); err != nil {
		t.Fatalf("valid limits rejected: %v", err)
	}
	badHigh := limits
	badHigh.H4 = 30
	if err := ValidateAnalogAlarmLimits(AlarmH3|AlarmH4, badHigh); err == nil {
		t.Fatalf("expected H4 <= H3 to be rejected")
	}
	badLow := limits
	badLow.HL = -10
	if err := ValidateAnalogAlarmLimits(AlarmLL|AlarmHL, badLow); err == nil {
		t.Fatalf("expected HL <= LL to be rejected")
	}
	badLowerSide := limits
	badLowerSide.L3 = -50
	if err := ValidateAnalogAlarmLimits(AlarmL4|AlarmL3, badLowerSide); err == nil {
		t.Fatalf("expected L3 <= L4 to be rejected")
	}
	if err := ValidateAnalogAlarmLimits(AlarmChange|AlarmLL, limits); err == nil {
		t.Fatalf("expected change+limit to be rejected")
	}
}

func TestAlarmNamesAndLabels(t *testing.T) {
	if AlarmChange.AnalogName() != "Change" || AlarmChange.AnalogLabel() != "Change alarm" {
		t.Fatalf("unexpected change alarm text: %s %s", AlarmChange.AnalogName(), AlarmChange.AnalogLabel())
	}
	if DXAlarmToZero.DXName() != "ToZero" || DXAlarmToZero.DXLabel() != "To 0 alarm" {
		t.Fatalf("unexpected DX to zero text: %s %s", DXAlarmToZero.DXName(), DXAlarmToZero.DXLabel())
	}
	if (AlarmLL | AlarmHL).AnalogName() != "LL|HL" {
		t.Fatalf("unexpected combined alarm name: %s", (AlarmLL | AlarmHL).AnalogName())
	}
	if !AlarmLimitMask.ValidForPointType(TypeAX) || !DXAlarmToggle.ValidForPointType(TypeDX) {
		t.Fatalf("valid alarm code rejected for point type")
	}
	if AlarmChange.ValidForPointType(TypeDX) || AlarmLL.ValidForPointType(TypeTX) {
		t.Fatalf("invalid alarm code accepted for point type")
	}
	if !TypeR8.SupportsAnalogAlarms() || TypeDX.SupportsAnalogAlarms() || !TypeDX.SupportsDXAlarms() {
		t.Fatalf("point alarm support helpers returned unexpected values")
	}
	colors := DefaultAlarmColors()
	if color, ok := colors.Color(AlarmZH); !ok || color != 0xCC0000 {
		t.Fatalf("Color(AlarmZH)=%#x,%v", color, ok)
	}
	if got := AlarmColorHex(0xFF0000); got != "#ff0000" {
		t.Fatalf("AlarmColorHex=%q", got)
	}
}

func TestAlarmPriorityValid(t *testing.T) {
	for _, priority := range []AlarmPriority{
		AlarmPriorityUnset,
		AlarmPriorityRed,
		AlarmPriorityYellow,
		AlarmPriorityWhite,
		AlarmPriorityGreen,
	} {
		if !priority.Valid() {
			t.Fatalf("priority %d should be valid", priority)
		}
	}
	if AlarmPriority(5).Valid() {
		t.Fatalf("priority 5 should be invalid")
	}
	if AlarmPriorityRed.String() != "Red" || AlarmPriority(9).String() != "Unknown" {
		t.Fatalf("unexpected priority strings")
	}
}

func TestControlState(t *testing.T) {
	tests := []struct {
		ds   DS
		want ControlState
	}{
		{0, ControlNone},
		{DSControlBit1, ControlIssued},
		{DSControlBit0, ControlFault},
		{DSControlBit0 | DSControlBit1, ControlTimeout},
	}
	for _, tt := range tests {
		if got := tt.ds.ControlState(); got != tt.want {
			t.Fatalf("ControlState(%016b)=%v want %v", tt.ds, got, tt.want)
		}
	}
}

func TestAlarmState(t *testing.T) {
	tests := []struct {
		ds   DS
		want AlarmState
	}{
		{0, AlarmStateNormal},
		{DSInAlarm, AlarmStateActive},
		{DSUnackedAlarm, AlarmStateRestoredUnacked},
		{DSInAlarm | DSUnackedAlarm, AlarmStateActive},
	}
	for _, tt := range tests {
		if got := tt.ds.AlarmState(); got != tt.want {
			t.Fatalf("AlarmState(%016b)=%v want %v", tt.ds, got, tt.want)
		}
	}
	if AlarmStateRestoredUnacked.String() != "RestoreAlarm" {
		t.Fatalf("unexpected restored alarm string: %s", AlarmStateRestoredUnacked)
	}
}
