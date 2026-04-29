package openplant

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tc252617228/openplant/archive"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/internal/rowconv"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/realtime"
	"github.com/tc252617228/openplant/sql"
	"github.com/tc252617228/openplant/stat"
)

func (c *Client) QueryRealtimeByRequest(ctx context.Context, req realtime.ReadRequest) ([]model.Sample, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	chunkSize := requestChunkSize(0, c.options.ChunkSize)
	out := make([]model.Sample, 0)
	for _, chunk := range chunkPointIDs(req.IDs, chunkSize) {
		rows, err := c.selectTableByRequest(ctx, protocol.TableSelectRequest{
			Table:   string(req.DB) + ".Realtime",
			Columns: []string{"ID", "GN", "TM", "DS", "AV"},
			Indexes: &protocol.Indexes{Key: "ID", Int32: pointIDsToInt32(chunk)},
		})
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			out = append(out, sampleFromTableRow(row))
		}
	}
	for _, chunk := range chunkGNs(req.GNs, chunkSize) {
		rows, err := c.selectTableByRequest(ctx, protocol.TableSelectRequest{
			Table:   string(req.DB) + ".Realtime",
			Columns: []string{"ID", "GN", "TM", "DS", "AV"},
			Indexes: &protocol.Indexes{Key: "GN", Strings: gnsToStrings(chunk)},
		})
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			out = append(out, sampleFromTableRow(row))
		}
	}
	return out, nil
}

func (c *Client) QueryArchiveByRequest(ctx context.Context, q archive.Query) ([]model.Sample, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	mode, err := archiveRequestMode(q.Mode)
	if err != nil {
		return nil, err
	}
	interval, err := requestIntervalSeconds(q.Interval)
	if err != nil {
		return nil, err
	}
	chunkSize := requestChunkSize(q.ChunkSize, c.options.ChunkSize)
	out := make([]model.Sample, 0)
	for _, chunk := range chunkPointIDs(q.IDs, chunkSize) {
		rows, err := c.selectTableByRequest(ctx, protocol.TableSelectRequest{
			Table:   string(q.DB) + ".Archive",
			Columns: []string{"ID", "GN", "TM", "DS", "AV"},
			Indexes: &protocol.Indexes{Key: "ID", Int32: pointIDsToInt32(chunk)},
			Filters: timeFilters(q.Range.Begin, q.Range.End),
			Limit:   limitString(q.Limit),
			Props: map[string]any{
				"mode":     mode,
				"interval": interval,
				"qtype":    int32(q.Quality),
			},
		})
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			out = append(out, sampleFromTableRow(row))
		}
	}
	for _, chunk := range chunkGNs(q.GNs, chunkSize) {
		rows, err := c.selectTableByRequest(ctx, protocol.TableSelectRequest{
			Table:   string(q.DB) + ".Archive",
			Columns: []string{"ID", "GN", "TM", "DS", "AV"},
			Indexes: &protocol.Indexes{Key: "GN", Strings: gnsToStrings(chunk)},
			Filters: timeFilters(q.Range.Begin, q.Range.End),
			Limit:   limitString(q.Limit),
			Props: map[string]any{
				"mode":     mode,
				"interval": interval,
				"qtype":    int32(q.Quality),
			},
		})
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			out = append(out, sampleFromTableRow(row))
		}
	}
	return out, nil
}

func (c *Client) QueryStatByRequest(ctx context.Context, q stat.Query) ([]model.StatSample, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	interval, err := requestIntervalSeconds(q.Interval)
	if err != nil {
		return nil, err
	}
	chunkSize := requestChunkSize(q.ChunkSize, c.options.ChunkSize)
	out := make([]model.StatSample, 0)
	for _, chunk := range chunkPointIDs(q.IDs, chunkSize) {
		rows, err := c.selectTableByRequest(ctx, protocol.TableSelectRequest{
			Table:   string(q.DB) + ".Stat",
			Columns: []string{"ID", "GN", "TM", "DS", "FLOW", "AVGV", "MAXV", "MINV", "MAXTIME", "MINTIME"},
			Indexes: &protocol.Indexes{Key: "ID", Int32: pointIDsToInt32(chunk)},
			Filters: timeFilters(q.Range.Begin, q.Range.End),
			Limit:   limitString(q.Limit),
			Props: map[string]any{
				"interval": interval,
				"qtype":    int32(q.Quality),
			},
		})
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			out = append(out, statSampleFromTableRow(row))
		}
	}
	for _, chunk := range chunkGNs(q.GNs, chunkSize) {
		rows, err := c.selectTableByRequest(ctx, protocol.TableSelectRequest{
			Table:   string(q.DB) + ".Stat",
			Columns: []string{"ID", "GN", "TM", "DS", "FLOW", "AVGV", "MAXV", "MINV", "MAXTIME", "MINTIME"},
			Indexes: &protocol.Indexes{Key: "GN", Strings: gnsToStrings(chunk)},
			Filters: timeFilters(q.Range.Begin, q.Range.End),
			Limit:   limitString(q.Limit),
			Props: map[string]any{
				"interval": interval,
				"qtype":    int32(q.Quality),
			},
		})
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			out = append(out, statSampleFromTableRow(row))
		}
	}
	return out, nil
}

func (c *Client) selectTableByRequest(ctx context.Context, req protocol.TableSelectRequest) ([]sql.Row, error) {
	payload, err := req.Encode()
	if err != nil {
		return nil, err
	}
	raw, err := c.rawRoundTrip(ctx, payload)
	if err != nil {
		return nil, err
	}
	resp, err := protocol.DecodeResponse(raw)
	if err != nil {
		return nil, err
	}
	rows, err := resp.Rows()
	if err != nil {
		return nil, operror.Wrap(operror.KindDecode, "openplant.Client.selectTableByRequest", err)
	}
	out := make([]sql.Row, 0, len(rows))
	for _, row := range rows {
		out = append(out, sql.Row(row))
	}
	return out, nil
}

func archiveRequestMode(mode model.ArchiveMode) (string, error) {
	if mode == "" {
		mode = model.ModeRaw
	}
	switch mode {
	case model.ModeRaw, model.ModeArch, model.ModeSpan, model.ModePlot, model.ModeFlow, model.ModeMax, model.ModeMin, model.ModeAvg, model.ModeMean, model.ModeStDev, model.ModeSum:
		return string(mode), nil
	default:
		return "", operror.Unsupported("openplant.archiveRequestMode", fmt.Sprintf("request archive query does not support mode %q yet", mode))
	}
}

func requestIntervalSeconds(interval model.Interval) (string, error) {
	if interval == "" {
		return "1", nil
	}
	raw := string(interval)
	idx := 0
	for idx < len(raw) && raw[idx] >= '0' && raw[idx] <= '9' {
		idx++
	}
	if idx == 0 {
		return "", operror.Validation("openplant.requestIntervalSeconds", "interval requires integer amount")
	}
	amount, err := strconv.Atoi(raw[:idx])
	if err != nil || amount <= 0 {
		return "", operror.Validation("openplant.requestIntervalSeconds", "interval amount must be positive")
	}
	unit := strings.ToLower(raw[idx:])
	var seconds int
	switch unit {
	case "ms":
		if amount%1000 != 0 {
			return "", operror.Unsupported("openplant.requestIntervalSeconds", "request interval does not support sub-second precision")
		}
		seconds = amount / 1000
	case "s":
		seconds = amount
	case "m":
		seconds = amount * 60
	case "h":
		seconds = amount * 3600
	case "d":
		seconds = amount * 86400
	case "w":
		seconds = amount * 7 * 86400
	default:
		return "", operror.Unsupported("openplant.requestIntervalSeconds", fmt.Sprintf("request interval unit %q is not supported yet", unit))
	}
	if seconds <= 0 {
		return "", operror.Unsupported("openplant.requestIntervalSeconds", "request interval must be at least one second")
	}
	return strconv.Itoa(seconds), nil
}

func timeFilters(begin, end time.Time) []protocol.Filter {
	return []protocol.Filter{
		{Left: "TM", Operator: protocol.OperGE, Right: timeFilterLiteral(begin), Relation: protocol.RelationAnd},
		{Left: "TM", Operator: protocol.OperLE, Right: timeFilterLiteral(end), Relation: protocol.RelationAnd},
	}
}

func timeFilterLiteral(tm time.Time) string {
	tm = tm.Truncate(time.Millisecond)
	layout := "2006-01-02 15:04:05"
	if tm.Nanosecond() != 0 {
		layout = "2006-01-02 15:04:05.000"
	}
	return tm.Format(layout)
}

func requestChunkSize(querySize, optionSize int) int {
	switch {
	case querySize > 0:
		return querySize
	case optionSize > 0:
		return optionSize
	default:
		return 200
	}
}

func chunkPointIDs(ids []model.PointID, size int) [][]model.PointID {
	ids = uniquePointIDs(ids)
	if len(ids) == 0 {
		return nil
	}
	if size <= 0 || size >= len(ids) {
		return [][]model.PointID{ids}
	}
	out := make([][]model.PointID, 0, (len(ids)+size-1)/size)
	for start := 0; start < len(ids); start += size {
		end := start + size
		if end > len(ids) {
			end = len(ids)
		}
		out = append(out, ids[start:end])
	}
	return out
}

func chunkGNs(gns []model.GN, size int) [][]model.GN {
	gns = uniqueGNsForRequest(gns)
	if len(gns) == 0 {
		return nil
	}
	if size <= 0 || size >= len(gns) {
		return [][]model.GN{gns}
	}
	out := make([][]model.GN, 0, (len(gns)+size-1)/size)
	for start := 0; start < len(gns); start += size {
		end := start + size
		if end > len(gns) {
			end = len(gns)
		}
		out = append(out, gns[start:end])
	}
	return out
}

func pointIDsToInt32(ids []model.PointID) []int32 {
	out := make([]int32, 0, len(ids))
	for _, id := range ids {
		out = append(out, int32(id))
	}
	return out
}

func gnsToStrings(gns []model.GN) []string {
	out := make([]string, 0, len(gns))
	for _, gn := range gns {
		out = append(out, string(gn))
	}
	return out
}

func uniqueGNsForRequest(gns []model.GN) []model.GN {
	out := make([]model.GN, 0, len(gns))
	seen := make(map[model.GN]struct{}, len(gns))
	for _, gn := range gns {
		if _, ok := seen[gn]; ok {
			continue
		}
		seen[gn] = struct{}{}
		out = append(out, gn)
	}
	return out
}

func limitString(limit int) string {
	if limit <= 0 {
		return ""
	}
	return strconv.Itoa(limit)
}

func sampleFromTableRow(row sql.Row) model.Sample {
	value, typ := rowconv.Value(row["AV"])
	if rt := rowconv.Int64(row["RT"]); rt != 0 || row["RT"] != nil {
		if typedValue, ok := rowconv.ValueForType(model.PointType(rt), row["AV"]); ok {
			value = typedValue
			typ = model.PointType(rt)
		}
	}
	return model.Sample{
		ID:     model.PointID(rowconv.Int32(row["ID"])),
		GN:     model.GN(rowconv.String(row["GN"])),
		Type:   typ,
		Format: rowconv.Int16(row["FM"]),
		Time:   rowconv.Time(row["TM"]),
		Status: model.DSFromInt16(rowconv.Int16(row["DS"])),
		Value:  value,
	}
}

func statSampleFromTableRow(row sql.Row) model.StatSample {
	return model.StatSample{
		ID:      model.PointID(rowconv.Int32(row["ID"])),
		GN:      model.GN(rowconv.String(row["GN"])),
		Time:    rowconv.Time(row["TM"]),
		Status:  model.DSFromInt16(rowconv.Int16(row["DS"])),
		Flow:    rowconv.Float64(row["FLOW"]),
		Avg:     rowconv.Float64(row["AVGV"]),
		Max:     rowconv.Float64(row["MAXV"]),
		Min:     rowconv.Float64(row["MINV"]),
		MaxTime: rowconv.Time(row["MAXTIME"]),
		MinTime: rowconv.Time(row["MINTIME"]),
		Mean:    rowconv.Float64(row["MEAN"]),
		Sum:     rowconv.Float64(row["SUM"]),
	}
}
