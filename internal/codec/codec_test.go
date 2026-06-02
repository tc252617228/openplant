package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/pierrec/lz4"
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

func TestFrameWriterReaderCompressionRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("openplant-compressible-payload;"), 4096)
	for _, mode := range []CompressionMode{CompressionFrame, CompressionBlock} {
		t.Run(mode.String(), func(t *testing.T) {
			var wire bytes.Buffer
			if err := NewFrameWriter(&wire, mode).WriteMessage(payload); err != nil {
				t.Fatalf("WriteMessage failed: %v", err)
			}
			reader := NewFrameReader(&wire)
			got, err := reader.ReadMessage()
			if err != nil {
				t.Fatalf("ReadMessage failed: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload mismatch len=%d want=%d", len(got), len(payload))
			}
			if reader.LastCompression() != mode {
				t.Fatalf("last compression=%d want=%d", reader.LastCompression(), mode)
			}
		})
	}
}

func TestFrameWriterUsesOpenPlantFrameShape(t *testing.T) {
	payload := bytes.Repeat([]byte("frame-shape;"), 1024)
	var wire bytes.Buffer
	if err := NewFrameWriter(&wire, CompressionFrame).WriteMessage(payload); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}
	raw := wire.Bytes()
	if len(raw) < 4+len(lz4FrameHeader)+4+8 {
		t.Fatalf("wire too short: %d", len(raw))
	}
	if raw[0] != 1 || CompressionMode(raw[1]) != CompressionFrame {
		t.Fatalf("unexpected frame header: %v", raw[:4])
	}
	size := int(raw[2])<<8 | int(raw[3])
	body := raw[4 : 4+size]
	if !bytes.HasPrefix(body, lz4FrameHeader) {
		t.Fatalf("missing OpenPlant LZ4 frame header: %x", body[:len(lz4FrameHeader)])
	}
	blockSize := binary.LittleEndian.Uint32(body[len(lz4FrameHeader) : len(lz4FrameHeader)+4])
	if int(blockSize) != len(body)-len(lz4FrameHeader)-4-8 {
		t.Fatalf("block size=%d body len=%d", blockSize, len(body))
	}
	checksum := binary.LittleEndian.Uint32(body[len(body)-4:])
	if checksum != xxh32Zero(payload) {
		t.Fatalf("checksum=%08x want %08x", checksum, xxh32Zero(payload))
	}
}

func TestFrameWriterUsesOpenPlantBlockShape(t *testing.T) {
	payload := bytes.Repeat([]byte("block-shape;"), 1024)
	var wire bytes.Buffer
	if err := NewFrameWriter(&wire, CompressionBlock).WriteMessage(payload); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}
	raw := wire.Bytes()
	if raw[0] != 1 || CompressionMode(raw[1]) != CompressionBlock {
		t.Fatalf("unexpected frame header: %v", raw[:4])
	}
	size := int(raw[2])<<8 | int(raw[3])
	body := raw[4 : 4+size]
	dst := make([]byte, MaxFramePayload)
	n, err := lz4.UncompressBlock(body, dst)
	if err != nil {
		t.Fatalf("raw block decompress failed: %v", err)
	}
	if !bytes.Equal(dst[:n], payload) {
		t.Fatalf("payload mismatch len=%d want=%d", n, len(payload))
	}
}

func TestFrameWriterCompressionIsStrict(t *testing.T) {
	payload := []byte("small")
	for _, mode := range []CompressionMode{CompressionFrame, CompressionBlock} {
		t.Run(mode.String(), func(t *testing.T) {
			var wire bytes.Buffer
			err := NewFrameWriter(&wire, mode).WriteMessage(payload)
			if !errors.Is(err, ErrCompressionFailed) {
				t.Fatalf("expected compression failure, got %v", err)
			}
			if wire.Len() != 0 {
				t.Fatalf("writer emitted %d bytes after compression failure", wire.Len())
			}
		})
	}
}

func TestFrameWriterDoesNotEmitPartialCompressedMessage(t *testing.T) {
	payload := append(bytes.Repeat([]byte("x"), MaxFramePayload), []byte("small")...)
	var wire bytes.Buffer
	err := NewFrameWriter(&wire, CompressionFrame).WriteMessage(payload)
	if !errors.Is(err, ErrCompressionFailed) {
		t.Fatalf("expected compression failure, got %v", err)
	}
	if wire.Len() != 0 {
		t.Fatalf("writer emitted %d bytes after multi-frame compression failure", wire.Len())
	}
}

func TestFrameReaderUnsupportedCompression(t *testing.T) {
	wire := []byte{1, 3, 0, 1, 0xff}
	_, err := NewFrameReader(bytes.NewReader(wire)).ReadMessage()
	if err == nil {
		t.Fatalf("expected unsupported compression error")
	}
}

func TestFrameReaderRejectsOversizedBlockPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), MaxFramePayload+1)
	dst := make([]byte, lz4.CompressBlockBound(len(payload)))
	n, err := lz4.CompressBlock(payload, dst, nil)
	if err != nil {
		t.Fatalf("CompressBlock failed: %v", err)
	}
	if n <= 0 || n > MaxFramePayload {
		t.Fatalf("unexpected compressed size: %d", n)
	}
	wire := []byte{1, byte(CompressionBlock), byte(n >> 8), byte(n)}
	wire = append(wire, dst[:n]...)

	_, err = NewFrameReader(bytes.NewReader(wire)).ReadMessage()
	if !errors.Is(err, ErrDecompressionFailed) {
		t.Fatalf("expected decompression failure, got %v", err)
	}
}

func TestDecodeObjectPayloadRejectsTruncatedValues(t *testing.T) {
	for _, typ := range []uint8{VtBool, VtInt8, VtInt16, VtInt32, VtInt64, VtFloat, VtDouble, VtDateTime} {
		_, err := decodeObjectPayload([]byte{typ})
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("type %d expected unexpected EOF, got %v", typ, err)
		}
	}
}

func TestDateTimePreservesSubMillisecondPrecision(t *testing.T) {
	var raw [8]byte
	PutFloat64(raw[:], 100.0015)
	got := DateTime(raw[:])
	if got.Unix() != 100 {
		t.Fatalf("seconds=%d want 100", got.Unix())
	}
	if got.Nanosecond() < 1_499_000 || got.Nanosecond() > 1_501_000 {
		t.Fatalf("nanosecond=%d want about 1500000", got.Nanosecond())
	}
}

func TestPutDateTimePreservesSubMillisecondPrecision(t *testing.T) {
	want := time.Unix(100, 1_500_000)
	var raw [8]byte
	PutDateTime(raw[:], want)
	got := DateTime(raw[:])
	if got.Sub(want) < -time.Microsecond || got.Sub(want) > time.Microsecond {
		t.Fatalf("roundtrip=%s want %s", got, want)
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
		if err != nil {
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
