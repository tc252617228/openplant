package codec

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"sort"
)

const (
	mpFixMapMin    = 0x80
	mpFixArrayMin  = 0x90
	mpFixStrMin    = 0xa0
	mpNil          = 0xc0
	mpFalse        = 0xc2
	mpTrue         = 0xc3
	mpBin8         = 0xc4
	mpBin16        = 0xc5
	mpBin32        = 0xc6
	mpExt8         = 0xc7
	mpExt16        = 0xc8
	mpExt32        = 0xc9
	mpFloat        = 0xca
	mpDouble       = 0xcb
	mpUint8        = 0xcc
	mpUint16       = 0xcd
	mpUint32       = 0xce
	mpUint64       = 0xcf
	mpInt8         = 0xd0
	mpInt16        = 0xd1
	mpInt32        = 0xd2
	mpInt64        = 0xd3
	mpFixExt1      = 0xd4
	mpFixExt2      = 0xd5
	mpFixExt4      = 0xd6
	mpFixExt8      = 0xd7
	mpFixExt16     = 0xd8
	mpStr8         = 0xd9
	mpStr16        = 0xda
	mpStr32        = 0xdb
	mpArray16      = 0xdc
	mpArray32      = 0xdd
	mpMap16        = 0xde
	mpMap32        = 0xdf
	mpNegFixNumMin = 0xe0
)

type Encoder struct {
	w *Writer
}

type Extension struct {
	Type uint8
	Data []byte
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: NewWriter(w)}
}

func MarshalValue(v any) ([]byte, error) {
	var buf bytes.Buffer
	err := NewEncoder(&buf).EncodeValue(v)
	return buf.Bytes(), err
}

func (e *Encoder) EncodeValue(v any) error {
	switch x := v.(type) {
	case nil:
		return e.w.WriteByte(mpNil)
	case bool:
		if x {
			return e.w.WriteByte(mpTrue)
		}
		return e.w.WriteByte(mpFalse)
	case int8:
		return e.encodeInt(int64(x))
	case uint8:
		return e.encodeUint(uint64(x))
	case int16:
		return e.encodeInt(int64(x))
	case uint16:
		return e.encodeUint(uint64(x))
	case int32:
		return e.encodeInt(int64(x))
	case uint32:
		return e.encodeUint(uint64(x))
	case int64:
		return e.encodeInt(x)
	case uint64:
		return e.encodeUint(x)
	case int:
		return e.encodeInt(int64(x))
	case uint:
		return e.encodeUint(uint64(x))
	case float32:
		if err := e.w.WriteByte(mpFloat); err != nil {
			return err
		}
		return e.w.WriteFloat32(x)
	case float64:
		if err := e.w.WriteByte(mpDouble); err != nil {
			return err
		}
		return e.w.WriteFloat64(x)
	case string:
		return e.EncodeString(x)
	case []byte:
		return e.EncodeBytes(x)
	case []any:
		return e.EncodeArray(x)
	case map[string]any:
		return e.EncodeMap(x)
	case Extension:
		return e.EncodeExtension(x.Type, x.Data)
	default:
		return fmt.Errorf("openplant codec: unsupported value type %T", v)
	}
}

func (e *Encoder) EncodeArrayStart(n int) error {
	switch {
	case n < 16:
		return e.w.WriteByte(mpFixArrayMin | byte(n))
	case n <= math.MaxUint16:
		if err := e.w.WriteByte(mpArray16); err != nil {
			return err
		}
		return e.w.WriteInt16(int16(n))
	default:
		if err := e.w.WriteByte(mpArray32); err != nil {
			return err
		}
		return e.w.WriteInt32(int32(n))
	}
}

func (e *Encoder) EncodeMapStart(n int) error {
	switch {
	case n < 16:
		return e.w.WriteByte(mpFixMapMin | byte(n))
	case n <= math.MaxUint16:
		if err := e.w.WriteByte(mpMap16); err != nil {
			return err
		}
		return e.w.WriteInt16(int16(n))
	default:
		if err := e.w.WriteByte(mpMap32); err != nil {
			return err
		}
		return e.w.WriteInt32(int32(n))
	}
}

func (e *Encoder) EncodeInt32(v int32) error {
	if err := e.w.WriteByte(mpInt32); err != nil {
		return err
	}
	return e.w.WriteInt32(v)
}

func (e *Encoder) EncodeInt64(v int64) error {
	if err := e.w.WriteByte(mpInt64); err != nil {
		return err
	}
	return e.w.WriteInt64(v)
}

func (e *Encoder) EncodeUint8(v uint8) error {
	if v <= 127 {
		return e.w.WriteByte(v)
	}
	if err := e.w.WriteByte(mpUint8); err != nil {
		return err
	}
	return e.w.WriteByte(v)
}

func (e *Encoder) EncodeExtension(tag uint8, payload []byte) error {
	n := len(payload)
	switch {
	case n <= math.MaxUint8:
		if err := e.w.WriteByte(mpExt8); err != nil {
			return err
		}
		if err := e.w.WriteByte(byte(n)); err != nil {
			return err
		}
	case n <= math.MaxUint16:
		if err := e.w.WriteByte(mpExt16); err != nil {
			return err
		}
		if err := e.w.WriteInt16(int16(n)); err != nil {
			return err
		}
	default:
		if err := e.w.WriteByte(mpExt32); err != nil {
			return err
		}
		if err := e.w.WriteInt32(int32(n)); err != nil {
			return err
		}
	}
	if err := e.w.WriteByte(tag); err != nil {
		return err
	}
	_, err := e.w.w.Write(payload)
	return err
}

func (e *Encoder) EncodeString(v string) error {
	n := len(v)
	switch {
	case n < 32:
		if err := e.w.WriteByte(mpFixStrMin | byte(n)); err != nil {
			return err
		}
	case n <= math.MaxUint8:
		if err := e.w.WriteByte(mpStr8); err != nil {
			return err
		}
		if err := e.w.WriteByte(byte(n)); err != nil {
			return err
		}
	case n <= math.MaxUint16:
		if err := e.w.WriteByte(mpStr16); err != nil {
			return err
		}
		if err := e.w.WriteInt16(int16(n)); err != nil {
			return err
		}
	default:
		if err := e.w.WriteByte(mpStr32); err != nil {
			return err
		}
		if err := e.w.WriteInt32(int32(n)); err != nil {
			return err
		}
	}
	_, err := e.w.w.Write([]byte(v))
	return err
}

func (e *Encoder) EncodeBytes(v []byte) error {
	n := len(v)
	switch {
	case n <= math.MaxUint8:
		if err := e.w.WriteByte(mpBin8); err != nil {
			return err
		}
		if err := e.w.WriteByte(byte(n)); err != nil {
			return err
		}
	case n <= math.MaxUint16:
		if err := e.w.WriteByte(mpBin16); err != nil {
			return err
		}
		if err := e.w.WriteInt16(int16(n)); err != nil {
			return err
		}
	default:
		if err := e.w.WriteByte(mpBin32); err != nil {
			return err
		}
		if err := e.w.WriteInt32(int32(n)); err != nil {
			return err
		}
	}
	_, err := e.w.w.Write(v)
	return err
}

func (e *Encoder) EncodeArray(v []any) error {
	n := len(v)
	if err := e.EncodeArrayStart(n); err != nil {
		return err
	}
	for _, item := range v {
		if err := e.EncodeValue(item); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) EncodeMap(v map[string]any) error {
	n := len(v)
	if err := e.EncodeMapStart(n); err != nil {
		return err
	}
	keys := make([]string, 0, len(v))
	for key := range v {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := e.EncodeString(key); err != nil {
			return err
		}
		if err := e.EncodeValue(v[key]); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) encodeInt(v int64) error {
	switch {
	case v >= -32 && v <= 127:
		return e.w.WriteByte(byte(int8(v)))
	case v >= math.MinInt8 && v <= math.MaxInt8:
		if err := e.w.WriteByte(mpInt8); err != nil {
			return err
		}
		return e.w.WriteInt8(int8(v))
	case v >= math.MinInt16 && v <= math.MaxInt16:
		if err := e.w.WriteByte(mpInt16); err != nil {
			return err
		}
		return e.w.WriteInt16(int16(v))
	case v >= math.MinInt32 && v <= math.MaxInt32:
		if err := e.w.WriteByte(mpInt32); err != nil {
			return err
		}
		return e.w.WriteInt32(int32(v))
	default:
		if err := e.w.WriteByte(mpInt64); err != nil {
			return err
		}
		return e.w.WriteInt64(v)
	}
}

func (e *Encoder) encodeUint(v uint64) error {
	switch {
	case v <= 127:
		return e.w.WriteByte(byte(v))
	case v <= math.MaxUint8:
		if err := e.w.WriteByte(mpUint8); err != nil {
			return err
		}
		return e.w.WriteByte(byte(v))
	case v <= math.MaxUint16:
		if err := e.w.WriteByte(mpUint16); err != nil {
			return err
		}
		return e.w.WriteInt16(int16(v))
	case v <= math.MaxUint32:
		if err := e.w.WriteByte(mpUint32); err != nil {
			return err
		}
		return e.w.WriteInt32(int32(v))
	default:
		if err := e.w.WriteByte(mpUint64); err != nil {
			return err
		}
		return e.w.WriteInt64(int64(v))
	}
}

type Decoder struct {
	r *Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: NewReader(r)}
}

func UnmarshalValue(data []byte) (any, error) {
	return NewDecoder(bytes.NewReader(data)).DecodeValue()
}

func (d *Decoder) DecodeValue() (any, error) {
	typ, err := d.r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch {
	case typ <= 0x7f:
		return int64(typ), nil
	case typ >= mpNegFixNumMin:
		return int64(int8(typ)), nil
	case typ&0xf0 == mpFixMapMin:
		return d.decodeMap(int(typ & 0x0f))
	case typ&0xf0 == mpFixArrayMin:
		return d.decodeArray(int(typ & 0x0f))
	case typ&0xe0 == mpFixStrMin:
		return d.decodeString(int(typ & 0x1f))
	}
	switch typ {
	case mpNil:
		return nil, nil
	case mpFalse:
		return false, nil
	case mpTrue:
		return true, nil
	case mpUint8:
		v, err := d.r.ReadByte()
		return uint64(v), err
	case mpUint16:
		v, err := d.r.ReadInt16()
		return uint64(uint16(v)), err
	case mpUint32:
		v, err := d.r.ReadInt32()
		return uint64(uint32(v)), err
	case mpUint64:
		v, err := d.r.ReadInt64()
		return uint64(v), err
	case mpInt8:
		v, err := d.r.ReadInt8()
		return int64(v), err
	case mpInt16:
		v, err := d.r.ReadInt16()
		return int64(v), err
	case mpInt32:
		v, err := d.r.ReadInt32()
		return int64(v), err
	case mpInt64:
		return d.r.ReadInt64()
	case mpFloat:
		return d.r.ReadFloat32()
	case mpDouble:
		return d.r.ReadFloat64()
	case mpStr8:
		n, err := d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		return d.decodeString(int(n))
	case mpStr16:
		n, err := d.r.ReadInt16()
		if err != nil {
			return nil, err
		}
		return d.decodeString(int(uint16(n)))
	case mpStr32:
		n, err := d.r.ReadInt32()
		if err != nil {
			return nil, err
		}
		return d.decodeString(int(uint32(n)))
	case mpBin8:
		n, err := d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		return d.decodeBytes(int(n))
	case mpBin16:
		n, err := d.r.ReadInt16()
		if err != nil {
			return nil, err
		}
		return d.decodeBytes(int(uint16(n)))
	case mpBin32:
		n, err := d.r.ReadInt32()
		if err != nil {
			return nil, err
		}
		return d.decodeBytes(int(uint32(n)))
	case mpExt8:
		n, err := d.r.ReadByte()
		if err != nil {
			return nil, err
		}
		return d.decodeExtension(int(n))
	case mpExt16:
		n, err := d.r.ReadInt16()
		if err != nil {
			return nil, err
		}
		return d.decodeExtension(int(uint16(n)))
	case mpExt32:
		n, err := d.r.ReadInt32()
		if err != nil {
			return nil, err
		}
		return d.decodeExtension(int(uint32(n)))
	case mpFixExt1:
		return d.decodeExtension(1)
	case mpFixExt2:
		return d.decodeExtension(2)
	case mpFixExt4:
		return d.decodeExtension(4)
	case mpFixExt8:
		return d.decodeExtension(8)
	case mpFixExt16:
		return d.decodeExtension(16)
	case mpArray16:
		n, err := d.r.ReadInt16()
		if err != nil {
			return nil, err
		}
		return d.decodeArray(int(uint16(n)))
	case mpArray32:
		n, err := d.r.ReadInt32()
		if err != nil {
			return nil, err
		}
		return d.decodeArray(int(uint32(n)))
	case mpMap16:
		n, err := d.r.ReadInt16()
		if err != nil {
			return nil, err
		}
		return d.decodeMap(int(uint16(n)))
	case mpMap32:
		n, err := d.r.ReadInt32()
		if err != nil {
			return nil, err
		}
		return d.decodeMap(int(uint32(n)))
	default:
		return nil, fmt.Errorf("openplant codec: unsupported msgpack type 0x%x", typ)
	}
}

func (d *Decoder) decodeExtension(n int) (Extension, error) {
	tag, err := d.r.ReadByte()
	if err != nil {
		return Extension{}, err
	}
	blob := make([]byte, n)
	if n > 0 {
		_, err = io.ReadFull(d.r.r, blob)
	}
	return Extension{Type: tag, Data: blob}, err
}

func (d *Decoder) decodeString(n int) (string, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(d.r.r, buf)
	return string(buf), err
}

func (d *Decoder) decodeBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(d.r.r, buf)
	return buf, err
}

func (d *Decoder) decodeArray(n int) ([]any, error) {
	out := make([]any, n)
	for i := 0; i < n; i++ {
		v, err := d.DecodeValue()
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func (d *Decoder) decodeMap(n int) (map[string]any, error) {
	out := make(map[string]any, n)
	for i := 0; i < n; i++ {
		key, err := d.DecodeValue()
		if err != nil {
			return nil, err
		}
		keyStr, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("openplant codec: map key is %T, want string", key)
		}
		value, err := d.DecodeValue()
		if err != nil {
			return nil, err
		}
		out[keyStr] = value
	}
	return out, nil
}
