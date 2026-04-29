package openplant

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/tc252617228/openplant/admin"
	"github.com/tc252617228/openplant/archive"
	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/realtime"
)

func (c *Client) WriteRealtimeNative(ctx context.Context, req realtime.WriteRequest) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if c.options.ReadOnly {
		return operror.ReadOnly("openplant.Client.WriteRealtimeNative", "realtime write blocked in readonly mode")
	}
	if err := req.ValidateNative(); err != nil {
		return err
	}
	groups := make(map[model.PointType][]realtime.Write)
	order := make([]model.PointType, 0, 8)
	for _, value := range req.Values {
		if _, ok := groups[value.Type]; !ok {
			order = append(order, value.Type)
		}
		groups[value.Type] = append(groups[value.Type], value)
	}
	chunkSize := requestChunkSize(req.ChunkSize, c.options.ChunkSize)
	for _, typ := range order {
		for _, chunk := range chunkRealtimeWrites(groups[typ], chunkSize) {
			payload, err := encodeRealtimeWriteNativeRequest(chunk, typ)
			if err != nil {
				return err
			}
			echo, err := c.rawEchoRoundTrip(ctx, payload)
			if err != nil {
				return err
			}
			if err := decodeNativeWriteEcho(echo, "openplant.Client.WriteRealtimeNative"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) WriteArchiveNative(ctx context.Context, req archive.WriteRequest) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if c.options.ReadOnly {
		return operror.ReadOnly("openplant.Client.WriteArchiveNative", "archive write blocked in readonly mode")
	}
	if err := req.ValidateNative(); err != nil {
		return err
	}
	blocks, err := archiveWriteBlocks(req)
	if err != nil {
		return err
	}
	chunkSize := requestChunkSize(req.ChunkSize, c.options.ChunkSize)
	for _, chunk := range chunkArchiveWriteBlocks(blocks, chunkSize) {
		payload, err := encodeArchiveWriteNativeRequest(chunk, req.Cache)
		if err != nil {
			return err
		}
		echo, err := c.rawEchoRoundTrip(ctx, payload)
		if err != nil {
			return err
		}
		if err := decodeNativeWriteEcho(echo, "openplant.Client.WriteArchiveNative"); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) DeleteArchiveNative(ctx context.Context, req archive.DeleteRequest) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if c.options.ReadOnly {
		return operror.ReadOnly("openplant.Client.DeleteArchiveNative", "archive delete blocked in readonly mode")
	}
	if err := req.ValidateNative(); err != nil {
		return err
	}
	ids := uniquePointIDs(req.IDs)
	chunkSize := requestChunkSize(req.ChunkSize, c.options.ChunkSize)
	for _, chunk := range chunkPointIDs(ids, chunkSize) {
		payload, err := encodeArchiveDeleteNativeRequest(chunk, req.Range.Begin, req.Range.End)
		if err != nil {
			return err
		}
		echo, err := c.rawEchoRoundTrip(ctx, payload)
		if err != nil {
			return err
		}
		if err := decodeNativeWriteEcho(echo, "openplant.Client.DeleteArchiveNative"); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) MutateTable(ctx context.Context, req admin.TableMutation) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if c.options.ReadOnly {
		return operror.ReadOnly("openplant.Client.MutateTable", "table mutation blocked in readonly mode")
	}
	if err := req.Validate(); err != nil {
		return err
	}
	payload, err := encodeTableMutationRequest(req)
	if err != nil {
		return err
	}
	raw, err := c.rawRoundTrip(ctx, payload)
	if err != nil {
		return err
	}
	if err := decodeTableMutationResponse(raw); err != nil {
		return err
	}
	c.invalidateMutationCaches(req)
	return nil
}

type archiveWriteBlock struct {
	ID      model.PointID
	Type    model.PointType
	Samples []model.Sample
}

func archiveWriteBlocks(req archive.WriteRequest) ([]archiveWriteBlock, error) {
	samples := append([]model.Sample(nil), req.Samples...)
	type blockKey struct {
		id  model.PointID
		typ model.PointType
	}
	blocksByKey := make(map[blockKey]int)
	blocks := make([]archiveWriteBlock, 0)
	for _, sample := range samples {
		key := blockKey{id: sample.ID, typ: sample.Type}
		idx, ok := blocksByKey[key]
		if !ok {
			idx = len(blocks)
			blocksByKey[key] = idx
			blocks = append(blocks, archiveWriteBlock{ID: sample.ID, Type: sample.Type})
		}
		blocks[idx].Samples = append(blocks[idx].Samples, sample)
	}
	return blocks, nil
}

func encodeRealtimeWriteNativeRequest(values []realtime.Write, typ model.PointType) ([]byte, error) {
	var buf bytes.Buffer
	w := codec.NewWriter(&buf)
	if err := w.WriteInt32(protocol.Magic); err != nil {
		return nil, err
	}
	if err := w.WriteInt32(int32(protocol.CommandInsert)); err != nil {
		return nil, err
	}
	if err := w.WriteInt32(int32(protocol.URLDynamic)); err != nil {
		return nil, err
	}
	if err := w.WriteInt16(0); err != nil {
		return nil, err
	}
	if err := w.WriteInt16(protocol.FlagWall); err != nil {
		return nil, err
	}
	if err := w.WriteInt32(int32(len(values))); err != nil {
		return nil, err
	}
	if err := w.WriteInt8(int8(typ)); err != nil {
		return nil, err
	}
	for _, value := range values {
		if err := encodeNativeTimedValue(&buf, int32(value.ID), value.Time, value.Status, value.Value); err != nil {
			return nil, err
		}
	}
	if err := w.WriteInt32(protocol.Magic); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeArchiveWriteNativeRequest(blocks []archiveWriteBlock, cache bool) ([]byte, error) {
	var buf bytes.Buffer
	w := codec.NewWriter(&buf)
	flag := protocol.FlagWall
	if cache {
		flag |= protocol.FlagCache
	}
	if err := w.WriteInt32(protocol.Magic); err != nil {
		return nil, err
	}
	if err := w.WriteInt32(int32(protocol.CommandInsert)); err != nil {
		return nil, err
	}
	if err := w.WriteInt32(int32(protocol.URLArchive)); err != nil {
		return nil, err
	}
	if err := w.WriteInt16(0); err != nil {
		return nil, err
	}
	if err := w.WriteInt16(flag); err != nil {
		return nil, err
	}
	if err := w.WriteInt32(int32(len(blocks))); err != nil {
		return nil, err
	}
	for _, block := range blocks {
		if err := w.WriteInt32(int32(block.ID)); err != nil {
			return nil, err
		}
		if err := w.WriteInt8(int8(block.Type)); err != nil {
			return nil, err
		}
		if err := w.WriteInt32(int32(len(block.Samples))); err != nil {
			return nil, err
		}
		for _, sample := range block.Samples {
			if err := encodeNativeTimedValue(&buf, 0, sample.Time, sample.Status, sample.Value); err != nil {
				return nil, err
			}
		}
	}
	if err := w.WriteInt32(protocol.Magic); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeArchiveDeleteNativeRequest(ids []model.PointID, begin, end time.Time) ([]byte, error) {
	beginUnix, err := timeUnix32("openplant.encodeArchiveDeleteNativeRequest.begin", begin)
	if err != nil {
		return nil, err
	}
	endUnix, err := timeUnix32("openplant.encodeArchiveDeleteNativeRequest.end", end)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	w := codec.NewWriter(&buf)
	if err := w.WriteInt32(protocol.Magic); err != nil {
		return nil, err
	}
	if err := w.WriteInt32(int32(protocol.CommandDelete)); err != nil {
		return nil, err
	}
	if err := w.WriteInt32(int32(protocol.URLArchive)); err != nil {
		return nil, err
	}
	if err := w.WriteInt16(0); err != nil {
		return nil, err
	}
	if err := w.WriteInt16(protocol.FlagWall); err != nil {
		return nil, err
	}
	if err := w.WriteInt32(int32(len(ids))); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := w.WriteInt32(int32(id)); err != nil {
			return nil, err
		}
	}
	if err := w.WriteInt32(beginUnix); err != nil {
		return nil, err
	}
	if err := w.WriteInt32(endUnix); err != nil {
		return nil, err
	}
	if err := w.WriteInt32(protocol.Magic); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeNativeTimedValue(buf *bytes.Buffer, id int32, tm time.Time, status model.DS, value model.Value) error {
	tmUnix, err := timeUnix32("openplant.encodeNativeTimedValue", tm)
	if err != nil {
		return err
	}
	w := codec.NewWriter(buf)
	if id > 0 {
		if err := w.WriteInt32(id); err != nil {
			return err
		}
	}
	if err := w.WriteInt32(tmUnix); err != nil {
		return err
	}
	if err := w.WriteInt16(status.Int16()); err != nil {
		return err
	}
	return codec.EncodeTSValue(buf, value)
}

func decodeNativeWriteEcho(code int8, op string) error {
	if code != 0 {
		return operror.Server(op, int32(code), "native write failed")
	}
	return nil
}

func encodeTableMutationRequest(req admin.TableMutation) ([]byte, error) {
	columns := make([]codec.Column, 0, len(req.Columns))
	for _, column := range req.Columns {
		columns = append(columns, codec.Column{
			Name:   column.Name,
			Type:   uint8(column.Type),
			Length: column.Length,
		})
	}
	rows := make([]map[string]any, 0, len(req.Rows))
	for _, row := range req.Rows {
		copyRow := make(map[string]any, len(row))
		for key, value := range row {
			copyRow[key] = value
		}
		rows = append(rows, copyRow)
	}
	request := protocol.TableMutationRequest{
		DB:      string(req.DB),
		Table:   req.Table,
		Action:  mutationAction(req.Action),
		Key:     req.Key,
		Indexes: mutationIndexes(req.Indexes),
		Filters: mutationFilters(req.Filters),
		Columns: columns,
		Rows:    rows,
	}
	return request.Encode()
}

func decodeTableMutationResponse(raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	_, err := protocol.DecodeResponse(raw)
	return err
}

func mutationAction(action admin.MutationAction) string {
	switch action {
	case admin.MutationInsert:
		return protocol.ActionInsert
	case admin.MutationUpdate:
		return protocol.ActionUpdate
	case admin.MutationReplace:
		return protocol.ActionReplace
	case admin.MutationDelete:
		return protocol.ActionDelete
	default:
		return string(action)
	}
}

func mutationIndexes(indexes *admin.Indexes) *protocol.Indexes {
	if indexes == nil {
		return nil
	}
	return &protocol.Indexes{
		Key:     indexes.Key,
		Int32:   append([]int32(nil), indexes.Int32...),
		Int64:   append([]int64(nil), indexes.Int64...),
		Strings: append([]string(nil), indexes.Strings...),
	}
}

func mutationFilters(filters []admin.Filter) []protocol.Filter {
	out := make([]protocol.Filter, 0, len(filters))
	for _, filter := range filters {
		out = append(out, protocol.Filter{
			Left:     filter.Left,
			Operator: uint8(filter.Operator),
			Right:    filter.Right,
			Relation: uint8(filter.Relation),
		})
	}
	return out
}

func chunkRealtimeWrites(values []realtime.Write, size int) [][]realtime.Write {
	if len(values) == 0 {
		return nil
	}
	if size <= 0 || size >= len(values) {
		return [][]realtime.Write{values}
	}
	out := make([][]realtime.Write, 0, (len(values)+size-1)/size)
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		out = append(out, values[start:end])
	}
	return out
}

func chunkArchiveWriteBlocks(blocks []archiveWriteBlock, size int) [][]archiveWriteBlock {
	if len(blocks) == 0 {
		return nil
	}
	if size <= 0 || size >= len(blocks) {
		return [][]archiveWriteBlock{blocks}
	}
	out := make([][]archiveWriteBlock, 0, (len(blocks)+size-1)/size)
	for start := 0; start < len(blocks); start += size {
		end := start + size
		if end > len(blocks) {
			end = len(blocks)
		}
		out = append(out, blocks[start:end])
	}
	return out
}

func timeUnix32(op string, tm time.Time) (int32, error) {
	unix := tm.Unix()
	if unix < -1<<31 || unix > 1<<31-1 {
		return 0, operror.Validation(op, "timestamp is outside native protocol int32 range")
	}
	return int32(unix), nil
}

func (c *Client) invalidateMutationCaches(req admin.TableMutation) {
	if c == nil || c.pointCache == nil || !sameBaseTable(req.Table, "point") {
		return
	}
	if req.DB != "" {
		c.pointCache.InvalidateDB(req.DB)
		return
	}
	c.pointCache.Clear()
}

func sameBaseTable(table, want string) bool {
	parts := strings.Split(table, ".")
	if len(parts) == 0 {
		return false
	}
	return strings.EqualFold(parts[len(parts)-1], want)
}
