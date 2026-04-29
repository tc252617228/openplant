package protocol

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/operror"
)

const (
	IndexInt32Array  uint8 = 20
	IndexInt64Array  uint8 = 21
	IndexStringArray uint8 = 25
	RowExtension     uint8 = 32

	OperEQ uint8 = 0
	OperGE uint8 = 4
	OperLE uint8 = 5
	OperIn uint8 = 6

	RelationAnd uint8 = 0
	RelationOr  uint8 = 1
)

type Filter struct {
	Left     string
	Operator uint8
	Right    string
	Relation uint8
}

type Indexes struct {
	Key     string
	Int32   []int32
	Int64   []int64
	Strings []string
}

type TableSelectRequest struct {
	DB      string
	Table   string
	Columns []string
	Indexes *Indexes
	Filters []Filter
	OrderBy string
	Limit   string
	Props   map[string]any
}

func (r TableSelectRequest) Encode() ([]byte, error) {
	if r.Table == "" {
		return nil, operror.Validation("protocol.TableSelectRequest.Encode", "table is required")
	}
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf)
	propCount := 4
	if r.DB != "" {
		propCount++
	}
	if r.Indexes != nil {
		propCount += 2
	}
	if len(r.Filters) > 0 {
		propCount++
	}
	if r.OrderBy != "" {
		propCount++
	}
	if r.Limit != "" {
		propCount++
	}
	for key := range r.Props {
		switch key {
		case PropService, PropAction, PropTable, PropColumns, PropDB, PropKey, PropIndexes, PropFilters, PropOrderBy, PropLimit:
			continue
		default:
			propCount++
		}
	}
	if err := enc.EncodeMapStart(propCount); err != nil {
		return nil, err
	}
	for _, item := range []struct {
		key string
		fn  func() error
	}{
		{PropTable, func() error { return enc.EncodeString(r.Table) }},
		{PropService, func() error { return enc.EncodeString("openplant") }},
		{PropAction, func() error { return enc.EncodeString(ActionSelect) }},
		{PropColumns, func() error { return encodeColumns(enc, r.Columns) }},
	} {
		if err := enc.EncodeString(item.key); err != nil {
			return nil, err
		}
		if err := item.fn(); err != nil {
			return nil, err
		}
	}
	if r.DB != "" {
		if err := encodeStringProp(enc, PropDB, r.DB); err != nil {
			return nil, err
		}
	}
	if r.Indexes != nil {
		if err := encodeStringProp(enc, PropKey, r.Indexes.Key); err != nil {
			return nil, err
		}
		if err := enc.EncodeString(PropIndexes); err != nil {
			return nil, err
		}
		if err := encodeIndexes(enc, *r.Indexes); err != nil {
			return nil, err
		}
	}
	if len(r.Filters) > 0 {
		if err := enc.EncodeString(PropFilters); err != nil {
			return nil, err
		}
		if err := encodeFilters(enc, r.Filters); err != nil {
			return nil, err
		}
	}
	if r.OrderBy != "" {
		if err := encodeStringProp(enc, PropOrderBy, r.OrderBy); err != nil {
			return nil, err
		}
	}
	if r.Limit != "" {
		if err := encodeStringProp(enc, PropLimit, r.Limit); err != nil {
			return nil, err
		}
	}
	keys := make([]string, 0, len(r.Props))
	for key := range r.Props {
		switch key {
		case PropService, PropAction, PropTable, PropColumns, PropDB, PropKey, PropIndexes, PropFilters, PropOrderBy, PropLimit:
			continue
		default:
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := enc.EncodeString(key); err != nil {
			return nil, err
		}
		if err := enc.EncodeValue(r.Props[key]); err != nil {
			return nil, err
		}
	}
	if err := enc.EncodeValue(nil); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeStringProp(enc *codec.Encoder, key, value string) error {
	if err := enc.EncodeString(key); err != nil {
		return err
	}
	return enc.EncodeString(value)
}

func encodeColumns(enc *codec.Encoder, columns []string) error {
	if len(columns) == 0 {
		columns = []string{"*"}
	}
	if err := enc.EncodeArrayStart(len(columns)); err != nil {
		return err
	}
	for _, name := range columns {
		if err := enc.EncodeMapStart(3); err != nil {
			return err
		}
		for _, item := range []struct {
			key string
			fn  func() error
		}{
			{"Name", func() error { return enc.EncodeString(name) }},
			{"Type", func() error { return enc.EncodeUint8(codec.VtNull) }},
			{"Length", func() error { return enc.EncodeUint8(0) }},
		} {
			if err := enc.EncodeString(item.key); err != nil {
				return err
			}
			if err := item.fn(); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodeIndexes(enc *codec.Encoder, indexes Indexes) error {
	if countIndexValueKinds(indexes) != 1 {
		return fmt.Errorf("protocol: indexes require exactly one value type")
	}
	switch {
	case len(indexes.Int32) > 0:
		payload, err := encodeInt32Array(indexes.Int32)
		if err != nil {
			return err
		}
		return enc.EncodeExtension(IndexInt32Array, payload)
	case len(indexes.Int64) > 0:
		payload, err := encodeInt64Array(indexes.Int64)
		if err != nil {
			return err
		}
		return enc.EncodeExtension(IndexInt64Array, payload)
	case len(indexes.Strings) > 0:
		payload, err := encodeStringArray(indexes.Strings)
		if err != nil {
			return err
		}
		return enc.EncodeExtension(IndexStringArray, payload)
	default:
		return fmt.Errorf("protocol: indexes are empty")
	}
}

func countIndexValueKinds(indexes Indexes) int {
	kinds := 0
	if len(indexes.Int32) > 0 {
		kinds++
	}
	if len(indexes.Int64) > 0 {
		kinds++
	}
	if len(indexes.Strings) > 0 {
		kinds++
	}
	return kinds
}

func encodeInt32Array(values []int32) ([]byte, error) {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf)
	if err := enc.EncodeArrayStart(len(values)); err != nil {
		return nil, err
	}
	for _, value := range values {
		if err := enc.EncodeInt32(value); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func encodeInt64Array(values []int64) ([]byte, error) {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf)
	if err := enc.EncodeArrayStart(len(values)); err != nil {
		return nil, err
	}
	for _, value := range values {
		if err := enc.EncodeInt64(value); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func encodeStringArray(values []string) ([]byte, error) {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf)
	if err := enc.EncodeArrayStart(len(values)); err != nil {
		return nil, err
	}
	for _, value := range values {
		if err := enc.EncodeString(value); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func encodeFilters(enc *codec.Encoder, filters []Filter) error {
	if err := enc.EncodeArrayStart(len(filters)); err != nil {
		return err
	}
	for _, filter := range filters {
		if err := enc.EncodeMapStart(4); err != nil {
			return err
		}
		for _, item := range []struct {
			key string
			fn  func() error
		}{
			{"L", func() error { return enc.EncodeString(filter.Left) }},
			{"O", func() error { return enc.EncodeUint8(filter.Operator) }},
			{"R", func() error { return enc.EncodeString(filter.Right) }},
			{"Or", func() error { return enc.EncodeUint8(filter.Relation) }},
		} {
			if err := enc.EncodeString(item.key); err != nil {
				return err
			}
			if err := item.fn(); err != nil {
				return err
			}
		}
	}
	return nil
}
