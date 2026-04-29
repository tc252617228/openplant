package openplant

import (
	"context"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/internal/rowconv"
	"github.com/tc252617228/openplant/internal/transport"
	"github.com/tc252617228/openplant/metadata"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/subscription"
)

const subscriptionBackoffJitterRatio = 0.2

type subscriptionIDSource struct {
	cfg        transport.Config
	backoffMin time.Duration
	backoffMax time.Duration
}

type closedSubscriptionIDSource struct {
	err error
}

func (s closedSubscriptionIDSource) SubscribeIDs(ctx context.Context, db model.DatabaseName, ids []model.PointID, emit func(subscription.Event) bool) (subscription.IDStream, error) {
	if s.err != nil {
		return nil, s.err
	}
	return nil, operror.ErrClosed
}

func (s closedSubscriptionIDSource) SubscribeTable(ctx context.Context, req subscription.TableRequest, emit func(subscription.TableEvent) bool) error {
	if s.err != nil {
		return s.err
	}
	return operror.ErrClosed
}

func (s *subscriptionIDSource) ValidateRequest(req subscription.Request) error {
	if err := req.Validate(); err != nil {
		return err
	}
	return rejectNativeGNs("openplant.subscriptionIDSource.Subscribe", req.GNs)
}

func (s *subscriptionIDSource) Subscribe(ctx context.Context, req subscription.Request, emit func(subscription.Event) bool) error {
	if err := s.ValidateRequest(req); err != nil {
		return err
	}
	stream, err := s.SubscribeIDs(ctx, req.DB, req.IDs, emit)
	if err != nil {
		return err
	}
	defer stream.Close()
	return waitSubscriptionIDStream(ctx, stream)
}

func (s *subscriptionIDSource) SubscribeTable(ctx context.Context, req subscription.TableRequest, emit func(subscription.TableEvent) bool) error {
	if err := req.Validate(); err != nil {
		return err
	}
	stream, err := s.subscribeTableRows(ctx, req, emit)
	if err != nil {
		return err
	}
	defer stream.Close()
	return waitSubscriptionTableStream(ctx, stream)
}

func (s *subscriptionIDSource) SubscribeIDs(ctx context.Context, db model.DatabaseName, ids []model.PointID, emit func(subscription.Event) bool) (subscription.IDStream, error) {
	if err := db.Validate(); err != nil {
		return nil, err
	}
	ids = uniqueSubscriptionIDs(ids)
	if len(ids) == 0 {
		return nil, operror.Validation("openplant.subscriptionIDSource.SubscribeIDs", "at least one point ID is required")
	}
	conn, err := transport.Dial(ctx, s.cfg)
	if err != nil {
		return nil, err
	}
	stream := &wireIDStream{
		cfg:        s.cfg,
		conn:       conn,
		db:         db,
		emit:       emit,
		done:       make(chan struct{}),
		ids:        pointIDSetForSubscription(ids),
		backoffMin: normalizeSubscriptionBackoff(s.backoffMin, 200*time.Millisecond),
		backoffMax: normalizeSubscriptionBackoff(s.backoffMax, 30*time.Second),
	}
	if stream.backoffMax < stream.backoffMin {
		stream.backoffMax = stream.backoffMin
	}
	if err := stream.writeSubscribeToConn(ctx, conn, ids, -1); err != nil {
		_ = conn.Close()
		return nil, err
	}
	go stream.readLoop(ctx)
	return stream, nil
}

func (s *subscriptionIDSource) subscribeTableRows(ctx context.Context, req subscription.TableRequest, emit func(subscription.TableEvent) bool) (*wireTableStream, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	conn, err := transport.Dial(ctx, s.cfg)
	if err != nil {
		return nil, err
	}
	stream := &wireTableStream{
		cfg:        s.cfg,
		conn:       conn,
		req:        req,
		emit:       emit,
		done:       make(chan struct{}),
		backoffMin: normalizeSubscriptionBackoff(s.backoffMin, 200*time.Millisecond),
		backoffMax: normalizeSubscriptionBackoff(s.backoffMax, 30*time.Second),
	}
	if stream.backoffMax < stream.backoffMin {
		stream.backoffMax = stream.backoffMin
	}
	if err := stream.writeSubscribeToConn(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	go stream.readLoop(ctx)
	return stream, nil
}

func waitSubscriptionIDStream(ctx context.Context, stream subscription.IDStream) error {
	if stream == nil {
		return operror.Unsupported("openplant.waitSubscriptionIDStream", "ID stream is nil")
	}
	done := stream.Done()
	if done == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return stream.Err()
	}
}

func waitSubscriptionTableStream(ctx context.Context, stream *wireTableStream) error {
	if stream == nil {
		return operror.Unsupported("openplant.waitSubscriptionTableStream", "table stream is nil")
	}
	done := stream.Done()
	if done == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return stream.Err()
	}
}

func (c *Client) ResolvePoints(ctx context.Context, db model.DatabaseName, gns []model.GN) ([]model.Point, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if err := db.Validate(); err != nil {
		return nil, err
	}
	if len(gns) == 0 {
		return nil, nil
	}
	if c.metadata == nil {
		return nil, operror.Unsupported("openplant.Client.ResolvePoints", "metadata service is not configured")
	}
	points, err := c.metadata.FindPoints(ctx, metadata.PointQuery{DB: db, GNs: gns})
	if err != nil {
		return nil, err
	}
	if c.pointCache != nil {
		c.pointCache.Store(db, points)
	}
	return points, nil
}

type wireIDStream struct {
	cfg        transport.Config
	conn       *transport.Conn
	db         model.DatabaseName
	emit       func(subscription.Event) bool
	done       chan struct{}
	closeOnce  sync.Once
	doneOnce   sync.Once
	ctrlMu     sync.Mutex
	mu         sync.RWMutex
	err        error
	closed     bool
	ids        map[model.PointID]struct{}
	backoffMin time.Duration
	backoffMax time.Duration
}

type wireTableStream struct {
	cfg        transport.Config
	conn       *transport.Conn
	req        subscription.TableRequest
	emit       func(subscription.TableEvent) bool
	done       chan struct{}
	closeOnce  sync.Once
	doneOnce   sync.Once
	ctrlMu     sync.Mutex
	mu         sync.RWMutex
	err        error
	closed     bool
	backoffMin time.Duration
	backoffMax time.Duration
}

func (s *wireIDStream) AddIDs(ctx context.Context, ids []model.PointID) error {
	ids = uniqueSubscriptionIDs(ids)
	if len(ids) == 0 {
		return nil
	}
	s.ctrlMu.Lock()
	defer s.ctrlMu.Unlock()
	conn, err := s.currentConn()
	if err != nil {
		return err
	}
	if err := s.writeSubscribeToConn(ctx, conn, ids, 1); err != nil {
		return err
	}
	s.mu.Lock()
	for _, id := range ids {
		s.ids[id] = struct{}{}
	}
	s.mu.Unlock()
	return nil
}

func (s *wireIDStream) RemoveIDs(ctx context.Context, ids []model.PointID) error {
	ids = uniqueSubscriptionIDs(ids)
	if len(ids) == 0 {
		return nil
	}
	s.ctrlMu.Lock()
	defer s.ctrlMu.Unlock()
	conn, err := s.currentConn()
	if err != nil {
		return err
	}
	if err := s.writeSubscribeToConn(ctx, conn, ids, 0); err != nil {
		return err
	}
	s.mu.Lock()
	for _, id := range ids {
		delete(s.ids, id)
	}
	s.mu.Unlock()
	return nil
}

func (s *wireIDStream) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		conn := s.conn
		s.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
	})
}

func (s *wireIDStream) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *wireIDStream) Err() error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *wireTableStream) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		conn := s.conn
		s.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
	})
}

func (s *wireTableStream) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *wireTableStream) Err() error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *wireIDStream) readLoop(ctx context.Context) {
	defer s.finish(nil)
	for {
		conn, err := s.currentConn()
		if err != nil {
			return
		}
		raw, err := conn.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || s.isClosed() {
				return
			}
			if s.emit != nil && !s.emit(subscription.Event{Kind: subscription.EventReconnecting, Err: err}) {
				return
			}
			if err := s.reconnect(ctx); err != nil {
				if ctx.Err() == nil && !s.isClosed() {
					s.finish(err)
				}
				return
			}
			if s.emit != nil && !s.emit(subscription.Event{Kind: subscription.EventReconnected}) {
				return
			}
			continue
		}
		events, err := decodeSubscriptionEvents(raw)
		if err != nil {
			if s.emit != nil && !s.emit(subscription.Event{Kind: subscription.EventReconnecting, Err: err}) {
				return
			}
			if err := s.reconnect(ctx); err != nil {
				if ctx.Err() == nil && !s.isClosed() {
					s.finish(err)
				}
				return
			}
			if s.emit != nil && !s.emit(subscription.Event{Kind: subscription.EventReconnected}) {
				return
			}
			continue
		}
		for _, event := range events {
			if s.emit != nil && !s.emit(event) {
				return
			}
		}
	}
}

func (s *wireTableStream) readLoop(ctx context.Context) {
	defer s.finish(nil)
	for {
		conn, err := s.currentConn()
		if err != nil {
			return
		}
		raw, err := conn.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || s.isClosed() {
				return
			}
			if s.emit != nil && !s.emit(subscription.TableEvent{Kind: subscription.EventReconnecting, Err: err}) {
				return
			}
			if err := s.reconnect(ctx); err != nil {
				if ctx.Err() == nil && !s.isClosed() {
					s.finish(err)
				}
				return
			}
			if s.emit != nil && !s.emit(subscription.TableEvent{Kind: subscription.EventReconnected}) {
				return
			}
			continue
		}
		events, err := decodeSubscriptionTableEvents(raw)
		if err != nil {
			if s.emit != nil && !s.emit(subscription.TableEvent{Kind: subscription.EventReconnecting, Err: err}) {
				return
			}
			if err := s.reconnect(ctx); err != nil {
				if ctx.Err() == nil && !s.isClosed() {
					s.finish(err)
				}
				return
			}
			if s.emit != nil && !s.emit(subscription.TableEvent{Kind: subscription.EventReconnected}) {
				return
			}
			continue
		}
		for _, event := range events {
			if s.emit != nil && !s.emit(event) {
				return
			}
		}
	}
}

func (s *wireIDStream) finish(err error) {
	if err != nil {
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
	}
	s.Close()
	s.doneOnce.Do(func() {
		close(s.done)
	})
}

func (s *wireTableStream) finish(err error) {
	if err != nil {
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
	}
	s.Close()
	s.doneOnce.Do(func() {
		close(s.done)
	})
}

func (s *wireIDStream) reconnect(ctx context.Context) error {
	s.ctrlMu.Lock()
	defer s.ctrlMu.Unlock()
	delay := s.backoffMin
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if s.isClosed() {
			return operror.ErrClosed
		}
		ids := s.currentIDs()
		if len(ids) == 0 {
			return operror.Validation("openplant.wireIDStream.reconnect", "cannot resubscribe an empty ID set")
		}
		conn, err := transport.Dial(ctx, s.cfg)
		if err == nil {
			err = s.writeSubscribeToConn(ctx, conn, ids, -1)
			if err == nil {
				s.replaceConn(conn)
				return nil
			}
			_ = conn.Close()
		}
		if err := sleepSubscriptionBackoff(ctx, jitterSubscriptionBackoff(delay, rand.Float64())); err != nil {
			return err
		}
		if delay <= 0 {
			delay = s.backoffMin
		} else {
			delay *= 2
			if delay > s.backoffMax {
				delay = s.backoffMax
			}
		}
	}
}

func (s *wireTableStream) reconnect(ctx context.Context) error {
	s.ctrlMu.Lock()
	defer s.ctrlMu.Unlock()
	delay := s.backoffMin
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if s.isClosed() {
			return operror.ErrClosed
		}
		conn, err := transport.Dial(ctx, s.cfg)
		if err == nil {
			err = s.writeSubscribeToConn(ctx, conn)
			if err == nil {
				s.replaceConn(conn)
				return nil
			}
			_ = conn.Close()
		}
		if err := sleepSubscriptionBackoff(ctx, jitterSubscriptionBackoff(delay, rand.Float64())); err != nil {
			return err
		}
		if delay <= 0 {
			delay = s.backoffMin
		} else {
			delay *= 2
			if delay > s.backoffMax {
				delay = s.backoffMax
			}
		}
	}
}

func (s *wireIDStream) writeSubscribeToConn(ctx context.Context, conn *transport.Conn, ids []model.PointID, change int) error {
	req := protocol.TableSelectRequest{
		Table:   string(s.db) + ".Realtime",
		Columns: []string{"ID", "GN", "TM", "DS", "AV"},
		Indexes: &protocol.Indexes{
			Key:   "ID",
			Int32: subscriptionPointIDsToInt32(ids),
		},
		Props: map[string]any{
			protocol.PropAsync: int32(1),
		},
	}
	if change >= 0 {
		req.Props[protocol.PropSubscribe] = int32(change)
	}
	payload, err := req.Encode()
	if err != nil {
		return err
	}
	return conn.WriteMessage(ctx, payload)
}

func (s *wireTableStream) writeSubscribeToConn(ctx context.Context, conn *transport.Conn) error {
	req := protocol.TableSelectRequest{
		Table:   string(s.req.DB) + "." + s.req.Table,
		Columns: append([]string(nil), s.req.Columns...),
		Indexes: tableSubscriptionIndexes(s.req),
		Props: map[string]any{
			protocol.PropAsync:    int32(1),
			protocol.PropSnapshot: s.req.Snapshot,
		},
	}
	payload, err := req.Encode()
	if err != nil {
		return err
	}
	return conn.WriteMessage(ctx, payload)
}

func (s *wireIDStream) currentConn() (*transport.Conn, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.conn == nil {
		return nil, operror.ErrClosed
	}
	return s.conn, nil
}

func (s *wireTableStream) currentConn() (*transport.Conn, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.conn == nil {
		return nil, operror.ErrClosed
	}
	return s.conn, nil
}

func (s *wireIDStream) currentIDs() []model.PointID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]model.PointID, 0, len(s.ids))
	for id := range s.ids {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (s *wireIDStream) replaceConn(conn *transport.Conn) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = conn.Close()
		return
	}
	old := s.conn
	s.conn = conn
	s.mu.Unlock()
	if old != nil && old != conn {
		_ = old.Close()
	}
}

func (s *wireTableStream) replaceConn(conn *transport.Conn) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = conn.Close()
		return
	}
	old := s.conn
	s.conn = conn
	s.mu.Unlock()
	if old != nil && old != conn {
		_ = old.Close()
	}
}

func (s *wireIDStream) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

func (s *wireTableStream) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

func decodeSubscriptionEvents(raw []byte) ([]subscription.Event, error) {
	resp, err := protocol.DecodeResponse(raw)
	if err != nil {
		return nil, err
	}
	rows, err := resp.Rows()
	if err != nil {
		return nil, operror.Wrap(operror.KindDecode, "openplant.decodeSubscriptionEvents", err)
	}
	events := make([]subscription.Event, 0, len(rows))
	for _, row := range rows {
		events = append(events, subscription.Event{Kind: subscription.EventData, Sample: sampleFromSubscriptionRow(row)})
	}
	return events, nil
}

func decodeSubscriptionTableEvents(raw []byte) ([]subscription.TableEvent, error) {
	resp, err := protocol.DecodeResponse(raw)
	if err != nil {
		return nil, err
	}
	rows, err := resp.Rows()
	if err != nil {
		return nil, operror.Wrap(operror.KindDecode, "openplant.decodeSubscriptionTableEvents", err)
	}
	events := make([]subscription.TableEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, subscription.TableEvent{Kind: subscription.EventData, Row: cloneSubscriptionRow(row)})
	}
	return events, nil
}

func sampleFromSubscriptionRow(row map[string]any) model.Sample {
	value, typ := rowconv.Value(row["AV"])
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

func cloneSubscriptionRow(row map[string]any) map[string]any {
	out := make(map[string]any, len(row))
	for key, value := range row {
		out[key] = value
	}
	return out
}

func tableSubscriptionIndexes(req subscription.TableRequest) *protocol.Indexes {
	indexes := &protocol.Indexes{Key: req.Key}
	switch {
	case len(req.Int32) > 0:
		indexes.Int32 = append([]int32(nil), req.Int32...)
	case len(req.Int64) > 0:
		indexes.Int64 = append([]int64(nil), req.Int64...)
	case len(req.Strings) > 0:
		indexes.Strings = append([]string(nil), req.Strings...)
	}
	return indexes
}

func uniqueSubscriptionIDs(ids []model.PointID) []model.PointID {
	out := make([]model.PointID, 0, len(ids))
	seen := make(map[model.PointID]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

func subscriptionPointIDsToInt32(ids []model.PointID) []int32 {
	out := make([]int32, 0, len(ids))
	for _, id := range ids {
		out = append(out, int32(id))
	}
	return out
}

func pointIDSetForSubscription(ids []model.PointID) map[model.PointID]struct{} {
	out := make(map[model.PointID]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

func normalizeSubscriptionBackoff(value, defaultValue time.Duration) time.Duration {
	if value <= 0 {
		return defaultValue
	}
	return value
}

func sleepSubscriptionBackoff(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func jitterSubscriptionBackoff(delay time.Duration, random float64) time.Duration {
	if delay <= 0 || subscriptionBackoffJitterRatio <= 0 {
		return delay
	}
	if random < 0 {
		random = 0
	}
	if random > 1 {
		random = 1
	}
	factor := 1 + ((random*2 - 1) * subscriptionBackoffJitterRatio)
	jittered := time.Duration(float64(delay) * factor)
	if jittered < 0 {
		return 0
	}
	return jittered
}

func subscriptionTransportConfig(cfg Options) transport.Config {
	return transport.Config{
		Host:           cfg.Host,
		Port:           cfg.Port,
		User:           cfg.User,
		Password:       cfg.Password,
		DialTimeout:    cfg.DialTimeout,
		RequestTimeout: cfg.RequestTimeout,
		PoolSize:       1,
		MaxIdle:        1,
		IdleTimeout:    cfg.IdleTimeout,
		MaxLifetime:    cfg.MaxLifetime,
		Compression:    codec.CompressionMode(cfg.Compression),
	}
}
