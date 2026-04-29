package openplant

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/internal/transport"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/realtime"
	"github.com/tc252617228/openplant/sql"
)

type clientSQLExecutor struct {
	client *Client
}

func (e clientSQLExecutor) QuerySQL(ctx context.Context, query string) (sql.Result, error) {
	if e.client == nil {
		return sql.Result{}, operror.ErrClosed
	}
	return e.client.execSQL(ctx, query)
}

func (e clientSQLExecutor) ExecSQL(ctx context.Context, query string) (sql.Result, error) {
	if e.client == nil {
		return sql.Result{}, operror.ErrClosed
	}
	return e.client.execSQL(ctx, query)
}

func (c *Client) execSQL(ctx context.Context, query string) (sql.Result, error) {
	var zero sql.Result
	payload, err := (protocol.Request{
		Props: map[string]any{
			protocol.PropService: "openplant",
			protocol.PropAction:  protocol.ActionExecSQL,
			protocol.PropSQL:     query,
		},
	}).Encode()
	if err != nil {
		return zero, err
	}
	raw, err := c.rawRoundTrip(ctx, payload)
	if err != nil {
		return zero, err
	}
	resp, err := protocol.DecodeResponse(raw)
	if err != nil {
		return zero, err
	}
	rows, err := resp.Rows()
	if err != nil {
		return zero, operror.Wrap(operror.KindDecode, "openplant.Client.execSQL", err)
	}
	out := sql.Result{Rows: make([]sql.Row, 0, len(rows))}
	for _, row := range rows {
		out.Rows = append(out.Rows, sql.Row(row))
	}
	return out, nil
}

func (c *Client) ReadRealtime(ctx context.Context, req realtime.ReadRequest) ([]model.Sample, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if err := req.ValidateNative(); err != nil {
		return nil, err
	}
	plan, err := c.planRealtimeRead(req)
	if err != nil {
		return nil, err
	}
	samplesByID := make(map[model.PointID]model.Sample, len(plan.uniqueIDs))
	chunkSize := c.options.ChunkSize
	if chunkSize <= 0 {
		chunkSize = len(plan.uniqueIDs)
	}
	for start := 0; start < len(plan.uniqueIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(plan.uniqueIDs) {
			end = len(plan.uniqueIDs)
		}
		samples, err := c.readRealtimeChunk(ctx, plan.uniqueIDs[start:end])
		if err != nil {
			return nil, err
		}
		for _, sample := range samples {
			samplesByID[sample.ID] = sample
		}
	}
	out := make([]model.Sample, 0, len(plan.requestedIDs))
	for _, id := range plan.requestedIDs {
		sample, ok := samplesByID[id]
		if !ok {
			return nil, operror.New(operror.KindProtocol, "openplant.Client.ReadRealtime", fmt.Sprintf("missing realtime sample for ID %d", id))
		}
		out = append(out, sample)
	}
	return out, nil
}

type realtimeReadPlan struct {
	requestedIDs []model.PointID
	uniqueIDs    []model.PointID
}

func (c *Client) planRealtimeRead(req realtime.ReadRequest) (realtimeReadPlan, error) {
	plan := realtimeReadPlan{
		requestedIDs: make([]model.PointID, 0, len(req.IDs)),
	}
	for _, id := range req.IDs {
		plan.requestedIDs = append(plan.requestedIDs, id)
	}
	if len(plan.requestedIDs) == 0 {
		return realtimeReadPlan{}, operror.Validation("openplant.Client.ReadRealtime", "at least one point ID is required")
	}
	plan.uniqueIDs = uniquePointIDs(plan.requestedIDs)
	return plan, nil
}

func uniquePointIDs(ids []model.PointID) []model.PointID {
	out := make([]model.PointID, 0, len(ids))
	seen := make(map[model.PointID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func (c *Client) readRealtimeChunk(ctx context.Context, ids []model.PointID) ([]model.Sample, error) {
	if len(ids) == 0 {
		return nil, operror.Validation("openplant.Client.ReadRealtime", "at least one point ID is required")
	}
	payload, err := encodeRealtimeRead(ids)
	if err != nil {
		return nil, err
	}
	raw, err := c.rawRoundTrip(ctx, payload)
	if err != nil {
		return nil, err
	}
	return decodeRealtimeRead(raw, ids)
}

func encodeRealtimeRead(ids []model.PointID) ([]byte, error) {
	out := make([]byte, 0, 24+len(ids)*4)
	out = codec.AppendInt32(out, protocol.Magic)
	out = codec.AppendInt32(out, int32(protocol.CommandSelect))
	out = codec.AppendInt32(out, int32(protocol.URLDynamic))
	out = codec.AppendInt16(out, 0)
	out = codec.AppendInt16(out, 0)
	out = codec.AppendInt32(out, int32(len(ids)))
	for _, id := range ids {
		out = codec.AppendInt32(out, int32(id))
	}
	out = codec.AppendInt32(out, protocol.Magic)
	return out, nil
}

func decodeRealtimeRead(raw []byte, ids []model.PointID) ([]model.Sample, error) {
	r := codec.NewReader(bytes.NewReader(raw))
	magic, err := r.ReadInt32()
	if err != nil {
		return nil, operror.Wrap(operror.KindDecode, "openplant.decodeRealtimeRead", err)
	}
	if magic != protocol.Magic {
		return nil, operror.New(operror.KindProtocol, "openplant.decodeRealtimeRead", "invalid realtime response magic")
	}
	if _, err := r.ReadInt32(); err != nil {
		return nil, operror.Wrap(operror.KindDecode, "openplant.decodeRealtimeRead", err)
	}
	count, err := r.ReadInt32()
	if err != nil {
		return nil, operror.Wrap(operror.KindDecode, "openplant.decodeRealtimeRead", err)
	}
	if int(count) != len(ids) {
		return nil, operror.New(operror.KindProtocol, "openplant.decodeRealtimeRead", "realtime response count mismatch")
	}
	samples := make([]model.Sample, 0, len(ids))
	for _, id := range ids {
		rtRaw, err := r.ReadInt8()
		if err != nil {
			return nil, operror.Wrap(operror.KindDecode, "openplant.decodeRealtimeRead", err)
		}
		if rtRaw < 0 {
			if _, err := r.ReadInt32(); err != nil {
				return nil, operror.Wrap(operror.KindDecode, "openplant.decodeRealtimeRead", err)
			}
			samples = append(samples, model.Sample{ID: id, Type: model.TypeUnknown})
			continue
		}
		tm, err := r.ReadInt32()
		if err != nil {
			return nil, operror.Wrap(operror.KindDecode, "openplant.decodeRealtimeRead", err)
		}
		ds, err := r.ReadInt16()
		if err != nil {
			return nil, operror.Wrap(operror.KindDecode, "openplant.decodeRealtimeRead", err)
		}
		typ := model.PointType(rtRaw & 15)
		value, err := codec.DecodeTSValue(r, typ)
		if err != nil {
			return nil, operror.Wrap(operror.KindDecode, "openplant.decodeRealtimeRead", err)
		}
		samples = append(samples, model.Sample{
			ID:     id,
			Type:   typ,
			Time:   time.Unix(int64(tm), 0),
			Status: model.DSFromInt16(ds),
			Value:  value,
		})
	}
	tail, err := r.ReadInt32()
	if err != nil {
		return nil, operror.Wrap(operror.KindDecode, "openplant.decodeRealtimeRead", err)
	}
	if tail != protocol.Magic {
		return nil, operror.New(operror.KindProtocol, "openplant.decodeRealtimeRead", "invalid realtime response tail magic")
	}
	return samples, nil
}

func (c *Client) rawRoundTrip(ctx context.Context, payload []byte) ([]byte, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if c.pool == nil {
		return nil, operror.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := c.withRequestTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, classifyAcquireError(err)
	}
	var opErr error
	defer func() {
		c.pool.Release(conn, opErr)
	}()
	var raw []byte
	raw, opErr = conn.Request(ctx, payload)
	if opErr != nil {
		return nil, opErr
	}
	return raw, nil
}

func (c *Client) rawEchoRoundTrip(ctx context.Context, payload []byte) (int8, error) {
	if err := c.ensureOpen(); err != nil {
		return 0, err
	}
	if c.pool == nil {
		return 0, operror.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := c.withRequestTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return 0, classifyAcquireError(err)
	}
	var opErr error
	var releaseErr error
	defer func() {
		c.pool.Release(conn, releaseErr)
	}()
	var echo int8
	echo, opErr = conn.RequestEcho(ctx, payload)
	if opErr != nil {
		releaseErr = opErr
		return 0, opErr
	}
	if echo != 0 {
		releaseErr = operror.New(operror.KindProtocol, "openplant.Client.rawEchoRoundTrip", "non-zero native echo")
	}
	return echo, nil
}

var errStreamStopped = errors.New("openplant: stream stopped")

func (c *Client) rawRoundTripStream(ctx context.Context, payload []byte, fn func(io.Reader) error) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if c.pool == nil {
		return operror.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := c.withRequestTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return classifyAcquireError(err)
	}
	var opErr error
	defer func() {
		c.pool.Release(conn, opErr)
	}()
	opErr = conn.RequestStream(ctx, payload, fn)
	if errors.Is(opErr, errStreamStopped) {
		opErr = operror.New(operror.KindCanceled, "openplant.Client.rawRoundTripStream", "stream stopped before response was drained")
		return nil
	}
	return opErr
}

func (c *Client) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || c.options.RequestTimeout <= 0 {
		return ctx, nil
	}
	return context.WithTimeout(ctx, c.options.RequestTimeout)
}

func classifyAcquireError(err error) error {
	if err == nil {
		return nil
	}
	if err == context.Canceled {
		return operror.Wrap(operror.KindCanceled, "openplant.Client.roundTrip.acquire", err)
	}
	if err == context.DeadlineExceeded {
		return operror.Wrap(operror.KindTimeout, "openplant.Client.roundTrip.acquire", err)
	}
	if err == operror.ErrClosed {
		return err
	}
	if transport.ShouldDrop(err) {
		return err
	}
	return fmt.Errorf("openplant: acquire connection: %w", err)
}

func rejectNativeGNs(op string, gns []model.GN) error {
	if len(gns) == 0 {
		return nil
	}
	return operror.Unsupported(op, "native path requires point IDs; resolve GNs explicitly with Metadata or use a SQL/request path")
}
