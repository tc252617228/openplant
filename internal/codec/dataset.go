package codec

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	VtNull      uint8 = 0
	VtBool      uint8 = 1
	VtInt8      uint8 = 2
	VtInt16     uint8 = 3
	VtInt32     uint8 = 4
	VtInt64     uint8 = 5
	VtFloat     uint8 = 6
	VtDouble    uint8 = 7
	VtDateTime  uint8 = 8
	VtString    uint8 = 9
	VtBinary    uint8 = 10
	VtObject    uint8 = 11
	VtMap       uint8 = 12
	VtStructure uint8 = 13
	VtSlice     uint8 = 14
)

type Column struct {
	Name   string
	Type   uint8
	Length uint8
	Ext    []byte

	cell   uint32
	offset uint32
	end    uint32
}

func DecodeColumns(value any) ([]Column, error) {
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("openplant codec: Columns is %T, want []any", value)
	}
	columns := make([]Column, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("openplant codec: Column is %T, want map[string]any", item)
		}
		col := Column{}
		if name, ok := m["Name"].(string); ok {
			col.Name = name
		}
		if typ, ok := uint8Value(m["Type"]); ok {
			col.Type = typ
		}
		if length, ok := uint8Value(m["Length"]); ok {
			col.Length = length
		}
		if ext, ok := m["Ext"].([]byte); ok {
			col.Ext = append([]byte(nil), ext...)
		} else if ext, ok := m["Ext"].(Extension); ok {
			col.Ext = append([]byte(nil), ext.Data...)
		}
		columns = append(columns, col)
	}
	layoutColumns(columns)
	return columns, nil
}

func EncodeColumns(columns []Column) []any {
	out := make([]any, 0, len(columns))
	for _, col := range columns {
		m := map[string]any{
			"Name":   col.Name,
			"Type":   col.Type,
			"Length": col.Length,
		}
		if len(col.Ext) > 0 {
			m["Ext"] = append([]byte(nil), col.Ext...)
		}
		out = append(out, m)
	}
	return out
}

func DecodeDataSet(data []byte, columns []Column) ([]map[string]any, error) {
	if len(data) == 0 || len(columns) == 0 {
		return nil, nil
	}
	rowDecoder := NewRowDecoder(columns)

	dec := NewDecoder(bytes.NewReader(data))
	rows := make([]map[string]any, 0)
	for {
		value, err := dec.DecodeValue()
		if err != nil {
			if err == io.EOF {
				return rows, nil
			}
			return nil, err
		}
		if value == nil {
			return rows, nil
		}
		chunk, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("openplant codec: dataset chunk is %T, want []any", value)
		}
		for _, item := range chunk {
			rowBytes, ok := dataSetItemBytes(item)
			if !ok {
				return nil, fmt.Errorf("openplant codec: dataset row is %T, want []byte", item)
			}
			row, err := rowDecoder.Decode(rowBytes)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}
}

func dataSetItemBytes(item any) ([]byte, bool) {
	switch x := item.(type) {
	case []byte:
		return x, true
	case Extension:
		return x.Data, true
	default:
		return nil, false
	}
}

type RowDecoder struct {
	columns []Column
	layout  tableLayout
}

func NewRowDecoder(columns []Column) *RowDecoder {
	columns = append([]Column(nil), columns...)
	layout := layoutColumns(columns)
	return &RowDecoder{columns: columns, layout: layout}
}

func DecodeRow(row []byte, columns []Column) (map[string]any, error) {
	return NewRowDecoder(columns).Decode(row)
}

func (d *RowDecoder) Decode(row []byte) (map[string]any, error) {
	if d == nil {
		return nil, fmt.Errorf("openplant codec: row decoder is nil")
	}
	layout := d.layout
	if len(row) < int(layout.fixedLength+layout.bitLength) {
		return nil, fmt.Errorf("openplant codec: row too short: %d", len(row))
	}
	out := make(map[string]any, len(d.columns))
	varBuf := row[layout.fixedLength+layout.bitLength:]
	for i, col := range d.columns {
		if !columnSet(row[layout.fixedLength:layout.fixedLength+layout.bitLength], i) {
			out[col.Name] = nil
			continue
		}
		value, err := decodeColumnValue(row, varBuf, col)
		if err != nil {
			return nil, fmt.Errorf("openplant codec: decode column %s: %w", col.Name, err)
		}
		out[col.Name] = value
	}
	return out, nil
}

func EncodeDataSet(columns []Column, rows []map[string]any) ([]byte, error) {
	columns = append([]Column(nil), columns...)
	layoutColumns(columns)
	encodedRows := make([]any, 0, len(rows))
	for _, row := range rows {
		raw, err := EncodeRow(columns, row)
		if err != nil {
			return nil, err
		}
		encodedRows = append(encodedRows, raw)
	}
	var buf bytes.Buffer
	if err := NewEncoder(&buf).EncodeArray(encodedRows); err != nil {
		return nil, err
	}
	if err := NewEncoder(&buf).EncodeValue(nil); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func EncodeRow(columns []Column, values map[string]any) ([]byte, error) {
	columns = append([]Column(nil), columns...)
	layout := layoutColumns(columns)
	row := make([]byte, layout.fixedLength+layout.bitLength)
	varParts := make([][]byte, layout.variableCount)
	for i, col := range columns {
		value, ok := values[col.Name]
		if !ok || value == nil {
			continue
		}
		setColumn(row[layout.fixedLength:layout.fixedLength+layout.bitLength], i)
		switch col.Type {
		case VtBool:
			if boolValue(value) {
				row[col.offset] = 1
			}
		case VtInt8:
			row[col.offset] = byte(int64Value(value))
		case VtInt16:
			PutInt16(row[col.offset:col.end], int16(int64Value(value)))
		case VtInt32:
			PutInt32(row[col.offset:col.end], int32(int64Value(value)))
		case VtInt64:
			PutInt64(row[col.offset:col.end], int64Value(value))
		case VtFloat:
			PutFloat32(row[col.offset:col.end], float32(float64Value(value)))
		case VtDouble:
			PutFloat64(row[col.offset:col.end], float64Value(value))
		case VtDateTime:
			if t, ok := value.(time.Time); ok {
				PutDateTime(row[col.offset:col.end], t)
			} else {
				PutFloat64(row[col.offset:col.end], float64Value(value))
			}
		case VtString:
			s := fmt.Sprint(value)
			if col.Length == 0 {
				varParts[col.cell] = encodeBinaryPayload([]byte(s))
			} else {
				copy(row[col.offset:col.end], []byte(s))
			}
		case VtBinary:
			blob, ok := value.([]byte)
			if !ok {
				return nil, fmt.Errorf("column %s expects []byte", col.Name)
			}
			if col.Length == 0 {
				varParts[col.cell] = encodeBinaryPayload(blob)
			} else {
				copy(row[col.offset:col.end], blob)
			}
		case VtSlice, VtMap, VtStructure:
			blob, ok := value.([]byte)
			if !ok {
				return nil, fmt.Errorf("column %s expects []byte", col.Name)
			}
			varParts[col.cell] = encodeBinaryPayload(blob)
		case VtObject:
			varParts[col.cell] = encodeBinaryPayload(encodeObjectPayload(value))
		default:
			return nil, fmt.Errorf("unsupported column type %d", col.Type)
		}
	}
	for _, part := range varParts {
		row = append(row, part...)
	}
	return row, nil
}

type tableLayout struct {
	fixedLength   uint32
	bitLength     uint32
	variableCount uint32
}

func layoutColumns(columns []Column) tableLayout {
	var layout tableLayout
	for i := range columns {
		col := &columns[i]
		switch col.Type {
		case VtBool, VtInt8:
			col.Length = 1
		case VtInt16:
			col.Length = 2
		case VtInt32, VtFloat:
			col.Length = 4
		case VtInt64, VtDouble, VtDateTime:
			col.Length = 8
		case VtString:
		case VtBinary:
		case VtSlice, VtMap, VtStructure, VtObject, VtNull:
			col.Length = 0
		default:
			col.Length = 0
		}
		if col.Length == 0 {
			col.cell = layout.variableCount
			layout.variableCount++
		}
		col.offset = layout.fixedLength
		col.end = col.offset + uint32(col.Length)
		fixedString := uint32(0)
		if col.Type == VtString && col.Length > 0 {
			fixedString = 1
		}
		layout.fixedLength = col.end + fixedString
		layout.bitLength = uint32((len(columns) + 7) >> 3)
	}
	return layout
}

func columnSet(bitBuf []byte, index int) bool {
	if len(bitBuf) == 0 {
		return false
	}
	return bitBuf[index>>3]&(1<<byte(index&7)) != 0
}

func setColumn(bitBuf []byte, index int) {
	bitBuf[index>>3] |= 1 << byte(index&7)
}

func decodeColumnValue(row []byte, varBuf []byte, col Column) (any, error) {
	switch col.Type {
	case VtNull:
		return nil, nil
	case VtBool:
		return row[col.offset] != 0, nil
	case VtInt8:
		return int8(row[col.offset]), nil
	case VtInt16:
		return Int16(row[col.offset:col.end]), nil
	case VtInt32:
		return Int32(row[col.offset:col.end]), nil
	case VtInt64:
		return Int64(row[col.offset:col.end]), nil
	case VtFloat:
		return Float32(row[col.offset:col.end]), nil
	case VtDouble:
		return Float64(row[col.offset:col.end]), nil
	case VtDateTime:
		return DateTime(row[col.offset:col.end]), nil
	case VtString:
		if col.Length == 0 {
			blob, err := variablePayload(varBuf, col.cell)
			return strings.TrimRight(string(blob), "\x00"), err
		}
		return strings.TrimRight(string(row[col.offset:col.end]), "\x00"), nil
	case VtBinary:
		if col.Length > 0 {
			return append([]byte(nil), row[col.offset:col.end]...), nil
		}
		blob, err := variablePayload(varBuf, col.cell)
		if err != nil {
			return nil, err
		}
		return append([]byte(nil), blob...), nil
	case VtSlice, VtMap, VtStructure:
		blob, err := variablePayload(varBuf, col.cell)
		if err != nil {
			return nil, err
		}
		return append([]byte(nil), blob...), nil
	case VtObject:
		blob, err := variablePayload(varBuf, col.cell)
		if err != nil {
			return nil, err
		}
		return decodeObjectPayload(blob)
	default:
		return nil, fmt.Errorf("unsupported column type %d", col.Type)
	}
}

func variablePayload(varBuf []byte, cell uint32) ([]byte, error) {
	offset := 0
	for i := uint32(0); i <= cell; i++ {
		if offset >= len(varBuf) {
			return nil, io.ErrUnexpectedEOF
		}
		size, next, err := binaryPayloadSize(varBuf[offset:])
		if err != nil {
			return nil, err
		}
		offset += next
		if offset+size > len(varBuf) {
			return nil, io.ErrUnexpectedEOF
		}
		if i == cell {
			return varBuf[offset : offset+size], nil
		}
		offset += size
	}
	return nil, nil
}

func binaryPayloadSize(data []byte) (size int, header int, err error) {
	switch data[0] {
	case mpBin8:
		if len(data) < 2 {
			return 0, 0, io.ErrUnexpectedEOF
		}
		return int(data[1]), 2, nil
	case mpBin16:
		if len(data) < 3 {
			return 0, 0, io.ErrUnexpectedEOF
		}
		return int(Uint16(data[1:3])), 3, nil
	case mpBin32:
		if len(data) < 5 {
			return 0, 0, io.ErrUnexpectedEOF
		}
		return int(Uint32(data[1:5])), 5, nil
	default:
		return 0, 0, fmt.Errorf("invalid binary payload prefix 0x%x", data[0])
	}
}

func encodeBinaryPayload(blob []byte) []byte {
	var buf bytes.Buffer
	_ = NewEncoder(&buf).EncodeBytes(blob)
	return buf.Bytes()
}

func encodeObjectPayload(value any) []byte {
	switch v := value.(type) {
	case nil:
		return []byte{VtNull}
	case bool:
		if v {
			return []byte{VtBool, 1}
		}
		return []byte{VtBool, 0}
	case int8:
		return []byte{VtInt8, byte(v)}
	case int16:
		out := []byte{VtInt16, 0, 0}
		PutInt16(out[1:], v)
		return out
	case int32:
		out := []byte{VtInt32, 0, 0, 0, 0}
		PutInt32(out[1:], v)
		return out
	case int64:
		out := []byte{VtInt64, 0, 0, 0, 0, 0, 0, 0, 0}
		PutInt64(out[1:], v)
		return out
	case float32:
		out := []byte{VtFloat, 0, 0, 0, 0}
		PutFloat32(out[1:], v)
		return out
	case float64:
		out := []byte{VtDouble, 0, 0, 0, 0, 0, 0, 0, 0}
		PutFloat64(out[1:], v)
		return out
	case time.Time:
		out := []byte{VtDateTime, 0, 0, 0, 0, 0, 0, 0, 0}
		PutDateTime(out[1:], v)
		return out
	case string:
		return append([]byte{VtString}, []byte(v)...)
	case []byte:
		return append([]byte{VtBinary}, v...)
	default:
		return append([]byte{VtString}, []byte(fmt.Sprint(value))...)
	}
}

func decodeObjectPayload(blob []byte) (any, error) {
	if len(blob) == 0 {
		return nil, nil
	}
	switch blob[0] {
	case VtNull:
		return nil, nil
	case VtBool:
		return len(blob) > 1 && blob[1] != 0, nil
	case VtInt8:
		return int8(blob[1]), nil
	case VtInt16:
		return Int16(blob[1:]), nil
	case VtInt32:
		return Int32(blob[1:]), nil
	case VtInt64:
		return Int64(blob[1:]), nil
	case VtFloat:
		return Float32(blob[1:]), nil
	case VtDouble:
		return Float64(blob[1:]), nil
	case VtDateTime:
		return DateTime(blob[1:]), nil
	case VtString:
		return strings.TrimRight(string(blob[1:]), "\x00"), nil
	case VtBinary:
		return append([]byte(nil), blob[1:]...), nil
	default:
		return append([]byte(nil), blob...), nil
	}
}

func uint8Value(v any) (uint8, bool) {
	switch x := v.(type) {
	case uint8:
		return x, true
	case uint16:
		return uint8(x), true
	case uint32:
		return uint8(x), true
	case uint64:
		return uint8(x), true
	case int8:
		return uint8(x), true
	case int16:
		return uint8(x), true
	case int32:
		return uint8(x), true
	case int64:
		return uint8(x), true
	case int:
		return uint8(x), true
	case uint:
		return uint8(x), true
	default:
		return 0, false
	}
}

func int64Value(v any) int64 {
	switch x := v.(type) {
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	case uint:
		return int64(x)
	default:
		return 0
	}
}

func float64Value(v any) float64 {
	switch x := v.(type) {
	case float32:
		return float64(x)
	case float64:
		return x
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint:
		return float64(int64Value(x))
	default:
		return 0
	}
}

func boolValue(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint:
		return int64Value(x) != 0
	default:
		return false
	}
}
