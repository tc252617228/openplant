package rowconv

import (
	"fmt"
	"time"

	"github.com/tc252617228/openplant/model"
)

func Int16(v any) int16 { return int16(Int64(v)) }
func Int32(v any) int32 { return int32(Int64(v)) }

func Int64(v any) int64 {
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
	case float32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}

func Float64(v any) float64 {
	switch x := v.(type) {
	case float32:
		return float64(x)
	case float64:
		return x
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint:
		return float64(Int64(x))
	default:
		return 0
	}
}

func String(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}
}

func Time(v any) time.Time {
	switch x := v.(type) {
	case time.Time:
		return x
	case int32:
		return time.Unix(int64(x), 0)
	case int64:
		return time.Unix(x, 0)
	case int:
		return time.Unix(int64(x), 0)
	case float64:
		sec := int64(x)
		nsec := int64((x - float64(sec)) * 1e9)
		return time.Unix(sec, nsec)
	case string:
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999999",
			"2006-01-02T15:04:05.999999999",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
		} {
			if tm, err := time.ParseInLocation(layout, x, time.Local); err == nil {
				return tm
			}
		}
		return time.Time{}
	default:
		return time.Time{}
	}
}

func Value(v any) (model.Value, model.PointType) {
	if value, ok := v.(model.Value); ok {
		return value, value.Type()
	}
	if typ, ok := model.InferPointType(v); ok {
		value, _ := ValueForType(typ, v)
		return value, typ
	}
	return model.Value{}, model.TypeUnknown
}

func ValueForType(typ model.PointType, v any) (model.Value, bool) {
	switch typ {
	case model.TypeAX:
		return model.AX(float32(Float64(v))), true
	case model.TypeDX:
		switch x := v.(type) {
		case bool:
			return model.DX(x), true
		default:
			return model.DX(Int64(v) != 0), true
		}
	case model.TypeI2:
		return model.I2(int16(Int64(v))), true
	case model.TypeI4:
		return model.I4(int32(Int64(v))), true
	case model.TypeR8:
		return model.R8(Float64(v)), true
	case model.TypeI8:
		return model.I8(Int64(v)), true
	case model.TypeTX:
		return model.TX(String(v)), true
	case model.TypeBN:
		blob, ok := v.([]byte)
		if !ok {
			return model.Value{}, false
		}
		return model.BN(blob), true
	default:
		return model.Value{}, false
	}
}
