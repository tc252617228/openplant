package model

import "fmt"

type PointSource int8

const (
	SourceDAS  PointSource = 0
	SourceCalc PointSource = 1
)

func (s PointSource) String() string {
	switch s {
	case SourceDAS:
		return "DAS"
	case SourceCalc:
		return "CALC"
	default:
		return fmt.Sprintf("PointSource(%d)", s)
	}
}

func (s PointSource) Valid() bool {
	return s == SourceDAS || s == SourceCalc
}

type PointType int8

const (
	TypeUnknown PointType = -1
	TypeAX      PointType = 0
	TypeDX      PointType = 1
	TypeI2      PointType = 2
	TypeI4      PointType = 3
	TypeR8      PointType = 4
	TypeI8      PointType = 5
	TypeTX      PointType = 6
	TypeBN      PointType = 7
)

func (t PointType) String() string {
	switch t {
	case TypeUnknown:
		return "UNKNOWN"
	case TypeAX:
		return "AX"
	case TypeDX:
		return "DX"
	case TypeI2:
		return "I2"
	case TypeI4:
		return "I4"
	case TypeR8:
		return "R8"
	case TypeI8:
		return "LONG"
	case TypeTX:
		return "TEXT"
	case TypeBN:
		return "BLOB"
	default:
		return fmt.Sprintf("PointType(%d)", t)
	}
}

func (t PointType) Valid() bool {
	return t >= TypeAX && t <= TypeBN
}

func (t PointType) Numeric() bool {
	switch t {
	case TypeAX, TypeDX, TypeI2, TypeI4, TypeR8, TypeI8:
		return true
	default:
		return false
	}
}

func ParsePointType(s string) (PointType, bool) {
	switch s {
	case "AX", "ax":
		return TypeAX, true
	case "DX", "dx":
		return TypeDX, true
	case "I2", "i2":
		return TypeI2, true
	case "I4", "i4":
		return TypeI4, true
	case "R8", "r8":
		return TypeR8, true
	case "LONG", "long", "I8", "i8":
		return TypeI8, true
	case "TEXT", "text", "TX", "tx":
		return TypeTX, true
	case "BLOB", "blob", "BN", "bn":
		return TypeBN, true
	default:
		return TypeUnknown, false
	}
}

type Value struct {
	typ PointType
	ax  float32
	dx  bool
	i2  int16
	i4  int32
	r8  float64
	i8  int64
	tx  string
	bn  []byte
}

func AX(v float32) Value { return Value{typ: TypeAX, ax: v} }
func DX(v bool) Value    { return Value{typ: TypeDX, dx: v} }
func I2(v int16) Value   { return Value{typ: TypeI2, i2: v} }
func I4(v int32) Value   { return Value{typ: TypeI4, i4: v} }
func R8(v float64) Value { return Value{typ: TypeR8, r8: v} }
func I8(v int64) Value   { return Value{typ: TypeI8, i8: v} }
func TX(v string) Value  { return Value{typ: TypeTX, tx: v} }

func BN(v []byte) Value {
	cp := make([]byte, len(v))
	copy(cp, v)
	return Value{typ: TypeBN, bn: cp}
}

func (v Value) Type() PointType { return v.typ }

func (v Value) Interface() any {
	switch v.typ {
	case TypeAX:
		return v.ax
	case TypeDX:
		return v.dx
	case TypeI2:
		return v.i2
	case TypeI4:
		return v.i4
	case TypeR8:
		return v.r8
	case TypeI8:
		return v.i8
	case TypeTX:
		return v.tx
	case TypeBN:
		cp := make([]byte, len(v.bn))
		copy(cp, v.bn)
		return cp
	default:
		return nil
	}
}

func (v Value) Float32() (float32, bool) { return v.ax, v.typ == TypeAX }
func (v Value) Bool() (bool, bool)       { return v.dx, v.typ == TypeDX }
func (v Value) Int16() (int16, bool)     { return v.i2, v.typ == TypeI2 }
func (v Value) Int32() (int32, bool)     { return v.i4, v.typ == TypeI4 }
func (v Value) Float64() (float64, bool) { return v.r8, v.typ == TypeR8 }
func (v Value) Int64() (int64, bool)     { return v.i8, v.typ == TypeI8 }
func (v Value) StringValue() (string, bool) {
	return v.tx, v.typ == TypeTX
}

func (v Value) Bytes() ([]byte, bool) {
	if v.typ != TypeBN {
		return nil, false
	}
	cp := make([]byte, len(v.bn))
	copy(cp, v.bn)
	return cp, true
}

func InferPointType(v any) (PointType, bool) {
	switch v.(type) {
	case float32:
		return TypeAX, true
	case bool:
		return TypeDX, true
	case int16:
		return TypeI2, true
	case int32:
		return TypeI4, true
	case float64:
		return TypeR8, true
	case int64:
		return TypeI8, true
	case string:
		return TypeTX, true
	case []byte:
		return TypeBN, true
	default:
		return TypeUnknown, false
	}
}
