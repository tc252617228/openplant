package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	openplant "github.com/tc252617228/openplant"
)

type tableSubscriber interface {
	subscribeTable(context.Context, openplant.TableSubscribeRequest) (tableStream, error)
}

type openplantTableSubscriber interface {
	SubscribeTable(context.Context, openplant.TableSubscribeRequest) (*openplant.TableSubscribeStream, error)
}

type tableStream interface {
	Events() <-chan openplant.TableSubscribeEvent
	Done() <-chan struct{}
	Err() error
	Close()
}

type PointTableSubscriptionRequest struct {
	DB       openplant.DatabaseName
	IDs      []openplant.PointID
	GNs      []openplant.GN
	Snapshot bool
}

type AlarmTableSubscriptionRequest struct {
	DB       openplant.DatabaseName
	IDs      []openplant.PointID
	GNs      []openplant.GN
	Snapshot bool
}

type openplantTableSubscriberAdapter struct {
	service openplantTableSubscriber
}

type TypedTableEvent[T any] struct {
	Kind   openplant.SubscribeEventKind
	Record T
	Err    error
}

type TypedTableStream[T any] struct {
	events <-chan TypedTableEvent[T]
	done   <-chan struct{}
	close  func()
	mu     sync.RWMutex
	err    error
}

func SubscribePointRecords(ctx context.Context, sub tableSubscriber, req PointTableSubscriptionRequest) (*TypedTableStream[openplant.Point], error) {
	rawReq, err := pointTableRequest(req)
	if err != nil {
		return nil, err
	}
	return subscribeTypedTable(ctx, sub, rawReq, pointFromTableRow)
}

func SubscribePointRecordsFromService(ctx context.Context, service openplantTableSubscriber, req PointTableSubscriptionRequest) (*TypedTableStream[openplant.Point], error) {
	return SubscribePointRecords(ctx, openplantTableSubscriberAdapter{service: service}, req)
}

func SubscribeAlarmRecords(ctx context.Context, sub tableSubscriber, req AlarmTableSubscriptionRequest) (*TypedTableStream[openplant.AlarmRecord], error) {
	rawReq, err := alarmTableRequest(req)
	if err != nil {
		return nil, err
	}
	return subscribeTypedTable(ctx, sub, rawReq, alarmFromTableRow)
}

func SubscribeAlarmRecordsFromService(ctx context.Context, service openplantTableSubscriber, req AlarmTableSubscriptionRequest) (*TypedTableStream[openplant.AlarmRecord], error) {
	return SubscribeAlarmRecords(ctx, openplantTableSubscriberAdapter{service: service}, req)
}

func (a openplantTableSubscriberAdapter) subscribeTable(ctx context.Context, req openplant.TableSubscribeRequest) (tableStream, error) {
	return a.service.SubscribeTable(ctx, req)
}

func (s *TypedTableStream[T]) Events() <-chan TypedTableEvent[T] {
	if s == nil {
		return nil
	}
	return s.events
}

func (s *TypedTableStream[T]) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

func (s *TypedTableStream[T]) Err() error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *TypedTableStream[T]) Close() {
	if s != nil && s.close != nil {
		s.close()
	}
}

func subscribeTypedTable[T any](ctx context.Context, sub tableSubscriber, req openplant.TableSubscribeRequest, convert func(map[string]any) (T, error)) (*TypedTableStream[T], error) {
	raw, err := sub.subscribeTable(ctx, req)
	if err != nil {
		return nil, err
	}
	events := make(chan TypedTableEvent[T], 16)
	done := make(chan struct{})
	stream := &TypedTableStream[T]{
		events: events,
		done:   done,
		close:  raw.Close,
	}
	go func() {
		defer close(done)
		defer close(events)
		defer raw.Close()
		for event := range raw.Events() {
			if event.IsError() || event.Err != nil {
				stream.setErr(event.Err)
				events <- TypedTableEvent[T]{Kind: openplant.SubscribeEventError, Err: event.Err}
				return
			}
			if !event.IsData() {
				events <- TypedTableEvent[T]{Kind: event.Kind}
				continue
			}
			record, err := convert(event.Row)
			if err != nil {
				stream.setErr(err)
				events <- TypedTableEvent[T]{Kind: openplant.SubscribeEventError, Err: err}
				return
			}
			events <- TypedTableEvent[T]{Kind: openplant.SubscribeEventData, Record: record}
		}
		if err := raw.Err(); err != nil {
			stream.setErr(err)
		}
	}()
	return stream, nil
}

func (s *TypedTableStream[T]) setErr(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func pointTableRequest(req PointTableSubscriptionRequest) (openplant.TableSubscribeRequest, error) {
	raw := openplant.TableSubscribeRequest{
		DB:       req.DB,
		Table:    "Point",
		Columns:  []string{"ID", "GN", "PN", "AN", "ED", "RT", "PT", "ND", "LC", "AP", "AR", "EU", "FM", "FQ", "EX"},
		Snapshot: req.Snapshot,
	}
	switch {
	case len(req.IDs) > 0 && len(req.GNs) == 0:
		raw.Key = "ID"
		raw.Int32 = pointIDsToInt32(req.IDs)
	case len(req.GNs) > 0 && len(req.IDs) == 0:
		raw.Key = "GN"
		raw.Strings = gnsToStrings(req.GNs)
	default:
		return openplant.TableSubscribeRequest{}, fmt.Errorf("point table subscription requires IDs or GNs, but not both")
	}
	return raw, raw.Validate()
}

func alarmTableRequest(req AlarmTableSubscriptionRequest) (openplant.TableSubscribeRequest, error) {
	raw := openplant.TableSubscribeRequest{
		DB:    req.DB,
		Table: "Alarm",
		Columns: []string{
			"ID", "GN", "PN", "AN", "ED", "EU",
			"TM", "TA", "TF", "AV", "DS", "RT",
			"AL", "AC", "AP", "LC",
			"C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8",
		},
		Snapshot: req.Snapshot,
	}
	switch {
	case len(req.IDs) > 0 && len(req.GNs) == 0:
		raw.Key = "ID"
		raw.Int32 = pointIDsToInt32(req.IDs)
	case len(req.GNs) > 0 && len(req.IDs) == 0:
		raw.Key = "GN"
		raw.Strings = gnsToStrings(req.GNs)
	default:
		return openplant.TableSubscribeRequest{}, fmt.Errorf("alarm table subscription requires IDs or GNs, but not both")
	}
	return raw, raw.Validate()
}

func pointFromTableRow(row map[string]any) (openplant.Point, error) {
	id := openplant.PointID(int32Value(row["ID"]))
	gn := openplant.GN(stringValue(row["GN"]))
	if id <= 0 || gn == "" {
		return openplant.Point{}, fmt.Errorf("point row requires ID and GN: %#v", row)
	}
	return openplant.Point{
		ID:          id,
		GN:          gn,
		NodeID:      openplant.NodeID(int32Value(row["ND"])),
		Source:      openplant.PointSource(int8Value(row["PT"])),
		Type:        openplant.PointType(int8Value(row["RT"])),
		Name:        stringValue(row["PN"]),
		Alias:       stringValue(row["AN"]),
		Description: stringValue(row["ED"]),
		Resolution:  int16Value(row["FQ"]),
		AlarmCode:   openplant.AlarmCode(uint16(int64Value(row["LC"]))),
		AlarmLevel:  openplant.AlarmPriority(int8Value(row["AP"])),
		Archived:    boolValue(row["AR"]),
		Unit:        stringValue(row["EU"]),
		Format:      int16Value(row["FM"]),
		Expression:  stringValue(row["EX"]),
	}, nil
}

func alarmFromTableRow(row map[string]any) (openplant.AlarmRecord, error) {
	id := openplant.PointID(int32Value(row["ID"]))
	gn := openplant.GN(stringValue(row["GN"]))
	if id <= 0 || gn == "" {
		return openplant.AlarmRecord{}, fmt.Errorf("alarm row requires ID and GN: %#v", row)
	}
	typ := openplant.PointType(int8Value(row["RT"]))
	value := valueForType(typ, row["AV"])
	status := openplant.DSFromInt16(int16Value(row["DS"]))
	configCode := openplant.AlarmCode(int64Value(row["LC"]))
	record := openplant.AlarmRecord{
		ID:          id,
		GN:          gn,
		Type:        typ,
		Name:        stringValue(row["PN"]),
		Alias:       stringValue(row["AN"]),
		Description: stringValue(row["ED"]),
		Unit:        stringValue(row["EU"]),
		Level:       int8Value(row["AL"]),
		Color:       int32Value(row["AC"]),
		Priority:    openplant.AlarmPriority(int8Value(row["AP"])),
		ConfigCode:  configCode,
		Colors: openplant.AlarmColors{
			LL: int32Value(row["C1"]),
			ZL: int32Value(row["C2"]),
			L3: int32Value(row["C3"]),
			L4: int32Value(row["C4"]),
			HL: int32Value(row["C5"]),
			ZH: int32Value(row["C6"]),
			H3: int32Value(row["C7"]),
			H4: int32Value(row["C8"]),
		},
		ActiveCode: status.ActiveAlarm(typ, configCode),
		FirstTime:  timeValue(row["TF"]),
		AlarmTime:  timeValue(row["TA"]),
		UpdateTime: timeValue(row["TM"]),
		Status:     status,
		Value:      value,
	}
	if record.Color == 0 {
		if color, ok := record.DisplayColor(); ok {
			record.Color = color
		}
	}
	return record, nil
}

func pointIDsToInt32(ids []openplant.PointID) []int32 {
	out := make([]int32, 0, len(ids))
	for _, id := range ids {
		out = append(out, int32(id))
	}
	return out
}

func gnsToStrings(gns []openplant.GN) []string {
	out := make([]string, 0, len(gns))
	for _, gn := range gns {
		out = append(out, string(gn))
	}
	return out
}

func valueForType(typ openplant.PointType, value any) openplant.Value {
	switch typ {
	case openplant.TypeAX:
		return openplant.AX(float32Value(value))
	case openplant.TypeDX:
		return openplant.DX(boolValue(value))
	case openplant.TypeI2:
		return openplant.I2(int16Value(value))
	case openplant.TypeI4:
		return openplant.I4(int32Value(value))
	case openplant.TypeR8:
		return openplant.R8(float64Value(value))
	case openplant.TypeI8:
		return openplant.I8(int64Value(value))
	case openplant.TypeTX:
		return openplant.TX(stringValue(value))
	case openplant.TypeBN:
		if blob, ok := value.([]byte); ok {
			return openplant.BN(blob)
		}
		return openplant.BN(nil)
	default:
		switch v := value.(type) {
		case string:
			return openplant.TX(v)
		case bool:
			return openplant.DX(v)
		case []byte:
			return openplant.BN(v)
		default:
			return openplant.R8(float64Value(value))
		}
	}
}

func int8Value(v any) int8   { return int8(int64Value(v)) }
func int16Value(v any) int16 { return int16(int64Value(v)) }
func int32Value(v any) int32 { return int32(int64Value(v)) }

func int64Value(v any) int64 {
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

func float32Value(v any) float32 {
	return float32(float64Value(v))
}

func float64Value(v any) float64 {
	switch x := v.(type) {
	case float32:
		return float64(x)
	case float64:
		return x
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint:
		return float64(int64Value(x))
	default:
		return 0
	}
}

func boolValue(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint:
		return int64Value(x) != 0
	default:
		return false
	}
}

func stringValue(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(v)
	}
}

func timeValue(v any) time.Time {
	switch x := v.(type) {
	case time.Time:
		return x
	case int32:
		return time.Unix(int64(x), 0)
	case int64:
		return time.Unix(x, 0)
	case float64:
		sec := int64(x)
		nsec := int64(x*1e3) % 1000 * 1e6
		return time.Unix(sec, nsec)
	default:
		return time.Time{}
	}
}
