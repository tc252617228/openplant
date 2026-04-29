package codec

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/tc252617228/openplant/model"
)

func TestFrameWriterReaderRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), MaxFramePayload+25)
	var wire bytes.Buffer
	if err := NewFrameWriter(&wire, CompressionNone).WriteMessage(payload); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}
	got, err := NewFrameReader(&wire).ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch len=%d want=%d", len(got), len(payload))
	}
}

func TestFrameReaderUnsupportedCompression(t *testing.T) {
	wire := []byte{1, byte(CompressionBlock), 0, 1, 0xff}
	_, err := NewFrameReader(bytes.NewReader(wire)).ReadMessage()
	if err == nil {
		t.Fatalf("expected unsupported compression error")
	}
}

func TestMsgpackRoundTrip(t *testing.T) {
	input := map[string]any{
		"Action": "Select",
		"Async":  int32(1),
		"IDs":    []any{int32(1), int32(2)},
		"Blob":   []byte{1, 2, 3},
		"OK":     true,
	}
	data, err := MarshalValue(input)
	if err != nil {
		t.Fatalf("MarshalValue failed: %v", err)
	}
	got, err := UnmarshalValue(data)
	if err != nil {
		t.Fatalf("UnmarshalValue failed: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("decoded value is %T", got)
	}
	if m["Action"] != "Select" || m["OK"] != true {
		t.Fatalf("decoded scalar mismatch: %#v", m)
	}
	if !reflect.DeepEqual(m["Blob"], []byte{1, 2, 3}) {
		t.Fatalf("decoded blob mismatch: %#v", m["Blob"])
	}
}

func TestTSValueRoundTrip(t *testing.T) {
	values := []model.Value{
		model.AX(1.25),
		model.DX(true),
		model.I2(2),
		model.I4(4),
		model.R8(8.5),
		model.I8(64),
		model.TX("text"),
		model.BN([]byte{9, 8}),
	}
	for _, value := range values {
		var buf bytes.Buffer
		if err := EncodeTSValue(&buf, value); err != nil {
			t.Fatalf("EncodeTSValue(%s) failed: %v", value.Type(), err)
		}
		got, err := DecodeTSValue(&buf, value.Type())
		if err != nil && err != io.EOF {
			t.Fatalf("DecodeTSValue(%s) failed: %v", value.Type(), err)
		}
		if !reflect.DeepEqual(got.Interface(), value.Interface()) {
			t.Fatalf("roundtrip %s got %#v want %#v", value.Type(), got.Interface(), value.Interface())
		}
	}
}

func TestDataSetRoundTrip(t *testing.T) {
	columns := []Column{
		{Name: "ID", Type: VtInt32},
		{Name: "PN", Type: VtString},
		{Name: "AV", Type: VtObject},
		{Name: "Blob", Type: VtBinary},
	}
	data, err := EncodeDataSet(columns, []map[string]any{{
		"ID":   int32(7),
		"PN":   "POINT7",
		"AV":   float64(9.5),
		"Blob": []byte{1, 2},
	}})
	if err != nil {
		t.Fatalf("EncodeDataSet failed: %v", err)
	}
	rows, err := DecodeDataSet(data, columns)
	if err != nil {
		t.Fatalf("DecodeDataSet failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d want 1", len(rows))
	}
	row := rows[0]
	if row["ID"] != int32(7) || row["PN"] != "POINT7" || row["AV"] != float64(9.5) {
		t.Fatalf("unexpected row: %#v", row)
	}
	if !reflect.DeepEqual(row["Blob"], []byte{1, 2}) {
		t.Fatalf("unexpected blob: %#v", row["Blob"])
	}
}

func TestRowRoundTripFixedBinary(t *testing.T) {
	columns := []Column{
		{Name: "SG", Type: VtBinary, Length: 4},
		{Name: "FQ", Type: VtInt16},
	}
	row, err := EncodeRow(columns, map[string]any{
		"SG": []byte{1, 2, 3, 4},
		"FQ": int16(30),
	})
	if err != nil {
		t.Fatalf("EncodeRow failed: %v", err)
	}
	if len(row) != 7 {
		t.Fatalf("fixed binary row length=%d want 7", len(row))
	}
	got, err := DecodeRow(row, columns)
	if err != nil {
		t.Fatalf("DecodeRow failed: %v", err)
	}
	if !reflect.DeepEqual(got["SG"], []byte{1, 2, 3, 4}) || got["FQ"] != int16(30) {
		t.Fatalf("unexpected row: %#v", got)
	}
}

func TestDataSetAcceptsExtensionRows(t *testing.T) {
	columns := []Column{
		{Name: "ID", Type: VtInt32},
		{Name: "PN", Type: VtString},
	}
	row, err := EncodeRow(columns, map[string]any{
		"ID": int32(7),
		"PN": "POINT7",
	})
	if err != nil {
		t.Fatalf("EncodeRow failed: %v", err)
	}
	var data bytes.Buffer
	if err := NewEncoder(&data).EncodeArray([]any{Extension{Type: 0, Data: row}}); err != nil {
		t.Fatalf("EncodeArray failed: %v", err)
	}
	if err := NewEncoder(&data).EncodeValue(nil); err != nil {
		t.Fatalf("Encode nil failed: %v", err)
	}

	rows, err := DecodeDataSet(data.Bytes(), columns)
	if err != nil {
		t.Fatalf("DecodeDataSet failed: %v", err)
	}
	if len(rows) != 1 || rows[0]["ID"] != int32(7) || rows[0]["PN"] != "POINT7" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}
