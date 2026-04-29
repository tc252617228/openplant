package main

import (
	"context"
	"errors"
	"testing"
	"time"

	openplant "github.com/tc252617228/openplant"
)

type fakeArchiveStrategyService struct {
	nativeErr  error
	requestErr error
	sqlErr     error
	calls      []ArchiveLayer
}

func (s *fakeArchiveStrategyService) QueryNative(context.Context, openplant.ArchiveQuery) ([]openplant.Sample, error) {
	s.calls = append(s.calls, ArchiveLayerNative)
	if s.nativeErr != nil {
		return nil, s.nativeErr
	}
	return []openplant.Sample{{ID: 1001, Value: openplant.R8(1)}}, nil
}

func (s *fakeArchiveStrategyService) QueryRequest(context.Context, openplant.ArchiveQuery) ([]openplant.Sample, error) {
	s.calls = append(s.calls, ArchiveLayerRequest)
	if s.requestErr != nil {
		return nil, s.requestErr
	}
	return []openplant.Sample{{ID: 1001, Value: openplant.R8(2)}}, nil
}

func (s *fakeArchiveStrategyService) QuerySQL(context.Context, openplant.ArchiveQuery) ([]openplant.Sample, error) {
	s.calls = append(s.calls, ArchiveLayerSQL)
	if s.sqlErr != nil {
		return nil, s.sqlErr
	}
	return []openplant.Sample{{ID: 1001, Value: openplant.R8(3)}}, nil
}

func TestQueryArchiveWithStrategyReturnsTraceForExplicitPolicy(t *testing.T) {
	nativeErr := errors.New("native unavailable")
	svc := &fakeArchiveStrategyService{nativeErr: nativeErr}
	result, err := QueryArchiveWithStrategy(context.Background(), svc, archiveStrategyQuery(), ArchiveStrategyPolicy{
		Layers: []ArchiveLayer{ArchiveLayerNative, ArchiveLayerRequest, ArchiveLayerSQL},
	})
	if err != nil {
		t.Fatalf("QueryArchiveWithStrategy failed: %v", err)
	}
	if len(result.Samples) != 1 {
		t.Fatalf("samples=%d", len(result.Samples))
	}
	value, _ := result.Samples[0].Value.Float64()
	if value != 2 {
		t.Fatalf("value=%v want request result", value)
	}
	if len(result.Trace) != 2 || result.Trace[0].Layer != ArchiveLayerNative || !errors.Is(result.Trace[0].Err, nativeErr) || result.Trace[1].Layer != ArchiveLayerRequest || result.Trace[1].Err != nil {
		t.Fatalf("unexpected trace: %#v", result.Trace)
	}
	if len(svc.calls) != 2 || svc.calls[0] != ArchiveLayerNative || svc.calls[1] != ArchiveLayerRequest {
		t.Fatalf("unexpected calls: %#v", svc.calls)
	}
}

func TestQueryArchiveWithStrategyReturnsLastError(t *testing.T) {
	sqlErr := errors.New("sql failed")
	svc := &fakeArchiveStrategyService{
		nativeErr:  errors.New("native failed"),
		requestErr: errors.New("request failed"),
		sqlErr:     sqlErr,
	}
	result, err := QueryArchiveWithStrategy(context.Background(), svc, archiveStrategyQuery(), ArchiveStrategyPolicy{
		Layers: []ArchiveLayer{ArchiveLayerNative, ArchiveLayerRequest, ArchiveLayerSQL},
	})
	if !errors.Is(err, sqlErr) {
		t.Fatalf("err=%v want %v", err, sqlErr)
	}
	if len(result.Trace) != 3 {
		t.Fatalf("trace=%#v", result.Trace)
	}
}

func archiveStrategyQuery() openplant.ArchiveQuery {
	begin := time.Unix(123456, 0)
	return openplant.ArchiveQuery{
		DB:      "W3",
		IDs:     []openplant.PointID{1001},
		Range:   openplant.TimeRange{Begin: begin, End: begin.Add(time.Hour)},
		Mode:    openplant.ModeRaw,
		Quality: openplant.QualityNone,
		Limit:   10,
	}
}
