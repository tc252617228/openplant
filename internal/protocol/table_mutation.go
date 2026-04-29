package protocol

import (
	"bytes"
	"fmt"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/operror"
)

type TableMutationRequest struct {
	DB      string
	Table   string
	Action  string
	Key     string
	Indexes *Indexes
	Filters []Filter
	Columns []codec.Column
	Rows    []map[string]any
}

func (r TableMutationRequest) Encode() ([]byte, error) {
	if r.Table == "" {
		return nil, operror.Validation("protocol.TableMutationRequest.Encode", "table is required")
	}
	if r.Action == "" {
		return nil, operror.Validation("protocol.TableMutationRequest.Encode", "action is required")
	}
	props := map[string]any{
		PropService: "openplant",
		PropAction:  r.Action,
		PropTable:   r.Table,
	}
	if r.DB != "" {
		props[PropDB] = r.DB
	}
	if len(r.Columns) > 0 {
		props[PropColumns] = codec.EncodeColumns(r.Columns)
	}
	if r.Indexes != nil {
		key := r.Indexes.Key
		if key == "" {
			key = r.Key
		}
		props[PropKey] = key
		indexes, err := encodeMutationIndexes(*r.Indexes)
		if err != nil {
			return nil, err
		}
		props[PropIndexes] = indexes
	}
	if len(r.Filters) > 0 {
		props[PropFilters] = mutationFilters(r.Filters)
	}

	var buf bytes.Buffer
	if err := codec.NewEncoder(&buf).EncodeMap(props); err != nil {
		return nil, operror.Wrap(operror.KindProtocol, "protocol.TableMutationRequest.Encode", err)
	}
	if len(r.Rows) > 0 {
		if len(r.Columns) == 0 {
			return nil, operror.Validation("protocol.TableMutationRequest.Encode", "rows require columns")
		}
		if err := encodeMutationRows(&buf, r.Columns, r.Rows); err != nil {
			return nil, err
		}
	}
	if err := codec.NewEncoder(&buf).EncodeValue(nil); err != nil {
		return nil, operror.Wrap(operror.KindProtocol, "protocol.TableMutationRequest.Encode", err)
	}
	return buf.Bytes(), nil
}

func encodeMutationIndexes(indexes Indexes) (codec.Extension, error) {
	if countIndexValueKinds(indexes) != 1 {
		return codec.Extension{}, fmt.Errorf("protocol: indexes require exactly one value type")
	}
	switch {
	case len(indexes.Int32) > 0:
		payload, err := encodeInt32Array(indexes.Int32)
		return codec.Extension{Type: IndexInt32Array, Data: payload}, err
	case len(indexes.Int64) > 0:
		payload, err := encodeInt64Array(indexes.Int64)
		return codec.Extension{Type: IndexInt64Array, Data: payload}, err
	case len(indexes.Strings) > 0:
		payload, err := encodeStringArray(indexes.Strings)
		return codec.Extension{Type: IndexStringArray, Data: payload}, err
	}
	return codec.Extension{}, fmt.Errorf("protocol: indexes are empty")
}

func mutationFilters(filters []Filter) []any {
	out := make([]any, 0, len(filters))
	for _, filter := range filters {
		out = append(out, map[string]any{
			"L":  filter.Left,
			"O":  filter.Operator,
			"R":  filter.Right,
			"Or": filter.Relation,
		})
	}
	return out
}

func encodeMutationRows(buf *bytes.Buffer, columns []codec.Column, rows []map[string]any) error {
	enc := codec.NewEncoder(buf)
	if err := enc.EncodeArrayStart(len(rows)); err != nil {
		return operror.Wrap(operror.KindProtocol, "protocol.TableMutationRequest.rows", err)
	}
	for _, row := range rows {
		raw, err := codec.EncodeRow(columns, row)
		if err != nil {
			return operror.Wrap(operror.KindProtocol, "protocol.TableMutationRequest.row", err)
		}
		if err := enc.EncodeExtension(RowExtension, raw); err != nil {
			return operror.Wrap(operror.KindProtocol, "protocol.TableMutationRequest.row", err)
		}
	}
	return nil
}
