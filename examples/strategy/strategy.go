package main

import (
	"context"
	"fmt"
	"time"

	openplant "github.com/tc252617228/openplant"
)

type ArchiveLayer string

const (
	ArchiveLayerNative  ArchiveLayer = "native"
	ArchiveLayerRequest ArchiveLayer = "request"
	ArchiveLayerSQL     ArchiveLayer = "sql"
)

type ArchiveStrategyPolicy struct {
	Layers []ArchiveLayer
}

type StrategyAttempt struct {
	Layer ArchiveLayer
	Took  time.Duration
	Err   error
}

type ArchiveStrategyResult struct {
	Samples []openplant.Sample
	Trace   []StrategyAttempt
}

type archiveQueryService interface {
	QueryNative(context.Context, openplant.ArchiveQuery) ([]openplant.Sample, error)
	QueryRequest(context.Context, openplant.ArchiveQuery) ([]openplant.Sample, error)
	QuerySQL(context.Context, openplant.ArchiveQuery) ([]openplant.Sample, error)
}

func QueryArchiveWithStrategy(ctx context.Context, svc archiveQueryService, q openplant.ArchiveQuery, policy ArchiveStrategyPolicy) (ArchiveStrategyResult, error) {
	if len(policy.Layers) == 0 {
		return ArchiveStrategyResult{}, fmt.Errorf("archive strategy requires at least one layer")
	}
	result := ArchiveStrategyResult{Trace: make([]StrategyAttempt, 0, len(policy.Layers))}
	var lastErr error
	for _, layer := range policy.Layers {
		start := time.Now()
		samples, err := queryArchiveLayer(ctx, svc, q, layer)
		attempt := StrategyAttempt{Layer: layer, Took: time.Since(start), Err: err}
		result.Trace = append(result.Trace, attempt)
		if err == nil {
			result.Samples = samples
			return result, nil
		}
		lastErr = err
	}
	return result, lastErr
}

func queryArchiveLayer(ctx context.Context, svc archiveQueryService, q openplant.ArchiveQuery, layer ArchiveLayer) ([]openplant.Sample, error) {
	switch layer {
	case ArchiveLayerNative:
		return svc.QueryNative(ctx, q)
	case ArchiveLayerRequest:
		return svc.QueryRequest(ctx, q)
	case ArchiveLayerSQL:
		return svc.QuerySQL(ctx, q)
	default:
		return nil, fmt.Errorf("unsupported archive strategy layer %q", layer)
	}
}
