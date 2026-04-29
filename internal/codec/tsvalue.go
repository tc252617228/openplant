package codec

import (
	"bytes"
	"fmt"
	"io"

	"github.com/tc252617228/openplant/model"
)

func EncodeTSValue(w io.Writer, v model.Value) error {
	if buf, ok := w.(*bytes.Buffer); ok {
		return encodeTSValueBuffer(buf, v)
	}
	bw := NewWriter(w)
	switch v.Type() {
	case model.TypeAX:
		x, _ := v.Float32()
		return bw.WriteFloat32(x)
	case model.TypeDX:
		x, _ := v.Bool()
		if x {
			return bw.WriteInt8(1)
		}
		return bw.WriteInt8(0)
	case model.TypeI2:
		x, _ := v.Int16()
		return bw.WriteInt16(x)
	case model.TypeI4:
		x, _ := v.Int32()
		return bw.WriteInt32(x)
	case model.TypeR8:
		x, _ := v.Float64()
		return bw.WriteFloat64(x)
	case model.TypeI8:
		x, _ := v.Int64()
		return bw.WriteInt64(x)
	case model.TypeTX:
		x, _ := v.StringValue()
		return NewEncoder(w).EncodeString(x)
	case model.TypeBN:
		x, _ := v.Bytes()
		return NewEncoder(w).EncodeBytes(x)
	default:
		return fmt.Errorf("openplant codec: unsupported point type %s", v.Type())
	}
}

func encodeTSValueBuffer(buf *bytes.Buffer, v model.Value) error {
	switch v.Type() {
	case model.TypeAX:
		x, _ := v.Float32()
		var raw [4]byte
		PutFloat32(raw[:], x)
		_, err := buf.Write(raw[:])
		return err
	case model.TypeDX:
		x, _ := v.Bool()
		if x {
			return buf.WriteByte(1)
		}
		return buf.WriteByte(0)
	case model.TypeI2:
		x, _ := v.Int16()
		var raw [2]byte
		PutInt16(raw[:], x)
		_, err := buf.Write(raw[:])
		return err
	case model.TypeI4:
		x, _ := v.Int32()
		var raw [4]byte
		PutInt32(raw[:], x)
		_, err := buf.Write(raw[:])
		return err
	case model.TypeR8:
		x, _ := v.Float64()
		var raw [8]byte
		PutFloat64(raw[:], x)
		_, err := buf.Write(raw[:])
		return err
	case model.TypeI8:
		x, _ := v.Int64()
		var raw [8]byte
		PutInt64(raw[:], x)
		_, err := buf.Write(raw[:])
		return err
	case model.TypeTX:
		x, _ := v.StringValue()
		return NewEncoder(buf).EncodeString(x)
	case model.TypeBN:
		x, _ := v.Bytes()
		return NewEncoder(buf).EncodeBytes(x)
	default:
		return fmt.Errorf("openplant codec: unsupported point type %s", v.Type())
	}
}

func DecodeTSValue(r io.Reader, typ model.PointType) (model.Value, error) {
	if br, ok := r.(*Reader); ok {
		return decodeTSValueReader(br, r, typ)
	}
	return decodeTSValueReader(NewReader(r), r, typ)
}

func decodeTSValueReader(br *Reader, raw io.Reader, typ model.PointType) (model.Value, error) {
	switch typ {
	case model.TypeAX:
		v, err := br.ReadFloat32()
		return model.AX(v), err
	case model.TypeDX:
		v, err := br.ReadInt8()
		return model.DX(v != 0), err
	case model.TypeI2:
		v, err := br.ReadInt16()
		return model.I2(v), err
	case model.TypeI4:
		v, err := br.ReadInt32()
		return model.I4(v), err
	case model.TypeR8:
		v, err := br.ReadFloat64()
		return model.R8(v), err
	case model.TypeI8:
		v, err := br.ReadInt64()
		return model.I8(v), err
	case model.TypeTX:
		v, err := NewDecoder(raw).DecodeValue()
		if err != nil {
			return model.Value{}, err
		}
		s, ok := v.(string)
		if !ok {
			return model.Value{}, fmt.Errorf("openplant codec: decoded TX is %T", v)
		}
		return model.TX(s), nil
	case model.TypeBN:
		v, err := NewDecoder(raw).DecodeValue()
		if err != nil {
			return model.Value{}, err
		}
		blob, ok := v.([]byte)
		if !ok {
			return model.Value{}, fmt.Errorf("openplant codec: decoded BN is %T", v)
		}
		return model.BN(blob), nil
	default:
		return model.Value{}, fmt.Errorf("openplant codec: unsupported point type %s", typ)
	}
}
