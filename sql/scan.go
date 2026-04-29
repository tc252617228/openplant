package sql

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/tc252617228/openplant/internal/rowconv"
)

var (
	scanPlanCache sync.Map
	timeType      = reflect.TypeOf(time.Time{})
)

type scanPlan struct {
	fields []scanField
}

type scanField struct {
	index []int
	names []string
}

func ScanRow[T any](row Row) (T, error) {
	var out T
	value := reflect.ValueOf(&out).Elem()
	if value.Kind() != reflect.Struct {
		return out, fmt.Errorf("openplant sql: ScanRow target %s is not a struct", value.Type())
	}
	plan := cachedScanPlan(value.Type())
	for _, field := range plan.fields {
		raw, ok := lookupScanValue(row, field.names)
		if !ok || raw == nil {
			continue
		}
		if err := setScanField(value.FieldByIndex(field.index), raw); err != nil {
			return out, fmt.Errorf("openplant sql: scan %s: %w", field.names[0], err)
		}
	}
	return out, nil
}

func ScanRows[T any](rows []Row) ([]T, error) {
	out := make([]T, 0, len(rows))
	for i, row := range rows {
		item, err := ScanRow[T](row)
		if err != nil {
			return nil, fmt.Errorf("openplant sql: scan row %d: %w", i, err)
		}
		out = append(out, item)
	}
	return out, nil
}

func cachedScanPlan(typ reflect.Type) *scanPlan {
	if cached, ok := scanPlanCache.Load(typ); ok {
		return cached.(*scanPlan)
	}
	plan := buildScanPlan(typ)
	actual, _ := scanPlanCache.LoadOrStore(typ, plan)
	return actual.(*scanPlan)
}

func buildScanPlan(typ reflect.Type) *scanPlan {
	plan := &scanPlan{fields: make([]scanField, 0, typ.NumField())}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tag := field.Tag.Get("openplant")
		if tag == "-" {
			continue
		}
		name := strings.TrimSpace(strings.Split(tag, ",")[0])
		if name == "" {
			name = field.Name
		}
		names := []string{name}
		if tag == "" {
			upper := strings.ToUpper(field.Name)
			if upper != name {
				names = append(names, upper)
			}
		}
		plan.fields = append(plan.fields, scanField{index: field.Index, names: names})
	}
	return plan
}

func lookupScanValue(row Row, names []string) (any, bool) {
	for _, name := range names {
		value, ok := row[name]
		if ok {
			return value, true
		}
	}
	return nil, false
}

func setScanField(dst reflect.Value, raw any) error {
	if !dst.CanSet() {
		return nil
	}
	if dst.Kind() == reflect.Pointer {
		value := reflect.New(dst.Type().Elem())
		if err := setScanField(value.Elem(), raw); err != nil {
			return err
		}
		dst.Set(value)
		return nil
	}
	rawValue := reflect.ValueOf(raw)
	if rawValue.IsValid() {
		if rawValue.Type().AssignableTo(dst.Type()) {
			dst.Set(rawValue)
			return nil
		}
	}
	if dst.Type() == timeType {
		dst.Set(reflect.ValueOf(rowconv.Time(raw)))
		return nil
	}
	switch dst.Kind() {
	case reflect.Bool:
		dst.SetBool(scanBool(raw))
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value := rowconv.Int64(raw)
		if dst.OverflowInt(value) {
			return fmt.Errorf("%d overflows %s", value, dst.Type())
		}
		dst.SetInt(value)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		value := rowconv.Int64(raw)
		if value < 0 {
			return fmt.Errorf("%d overflows %s", value, dst.Type())
		}
		if dst.OverflowUint(uint64(value)) {
			return fmt.Errorf("%d overflows %s", value, dst.Type())
		}
		dst.SetUint(uint64(value))
		return nil
	case reflect.Float32, reflect.Float64:
		value := rowconv.Float64(raw)
		if dst.OverflowFloat(value) {
			return fmt.Errorf("%f overflows %s", value, dst.Type())
		}
		dst.SetFloat(value)
		return nil
	case reflect.String:
		dst.SetString(rowconv.String(raw))
		return nil
	case reflect.Slice:
		if dst.Type().Elem().Kind() != reflect.Uint8 {
			break
		}
		switch value := raw.(type) {
		case []byte:
			setByteSlice(dst, append([]byte(nil), value...))
			return nil
		case string:
			setByteSlice(dst, []byte(value))
			return nil
		}
	}
	if rawValue.IsValid() && rawValue.Type().ConvertibleTo(dst.Type()) {
		dst.Set(rawValue.Convert(dst.Type()))
		return nil
	}
	return fmt.Errorf("cannot assign %T to %s", raw, dst.Type())
}

func setByteSlice(dst reflect.Value, value []byte) {
	raw := reflect.ValueOf(value)
	if raw.Type().AssignableTo(dst.Type()) {
		dst.Set(raw)
		return
	}
	dst.Set(raw.Convert(dst.Type()))
}

func scanBool(raw any) bool {
	switch value := raw.(type) {
	case bool:
		return value
	default:
		return rowconv.Int64(raw) != 0
	}
}
