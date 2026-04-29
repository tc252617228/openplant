package model

import "testing"

func TestPointTypeUnknownDoesNotCollideWithAX(t *testing.T) {
	if TypeAX != 0 {
		t.Fatalf("AX must stay wire-compatible with RT=0, got %d", TypeAX)
	}
	if TypeUnknown == TypeAX {
		t.Fatalf("unknown type must not collide with AX")
	}
	if !TypeAX.Valid() {
		t.Fatalf("AX must be valid")
	}
	if TypeUnknown.Valid() {
		t.Fatalf("unknown must not be a valid concrete point type")
	}
}

func TestValueCopiesBinary(t *testing.T) {
	src := []byte{1, 2, 3}
	v := BN(src)
	src[0] = 9
	got, ok := v.Bytes()
	if !ok {
		t.Fatalf("expected BLOB value")
	}
	if got[0] != 1 {
		t.Fatalf("binary value was not copied on input")
	}
	got[0] = 8
	again, _ := v.Bytes()
	if again[0] != 1 {
		t.Fatalf("binary value was not copied on output")
	}
}

func TestInferPointType(t *testing.T) {
	tests := []struct {
		value any
		want  PointType
	}{
		{float32(1), TypeAX},
		{true, TypeDX},
		{int16(1), TypeI2},
		{int32(1), TypeI4},
		{float64(1), TypeR8},
		{int64(1), TypeI8},
		{"x", TypeTX},
		{[]byte{1}, TypeBN},
	}
	for _, tt := range tests {
		got, ok := InferPointType(tt.value)
		if !ok || got != tt.want {
			t.Fatalf("InferPointType(%T) = %v,%v want %v,true", tt.value, got, ok, tt.want)
		}
	}
	if got, ok := InferPointType(1); ok || got != TypeUnknown {
		t.Fatalf("plain int should not be inferred implicitly, got %v,%v", got, ok)
	}
}

func TestPointConfigEnums(t *testing.T) {
	if !DeadbandENG.Valid() || DeadbandENG.String() != "ENG" {
		t.Fatalf("unexpected deadband type: %v %s", DeadbandENG.Valid(), DeadbandENG)
	}
	if DeadbandType(9).Valid() {
		t.Fatalf("invalid deadband type accepted")
	}
	if !PointCompressionLinear.Valid() || PointCompressionLinear.String() != "LINEAR" {
		t.Fatalf("unexpected point compression: %v %s", PointCompressionLinear.Valid(), PointCompressionLinear)
	}
	if PointCompression(9).Valid() {
		t.Fatalf("invalid point compression accepted")
	}
}
