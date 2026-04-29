package openplant

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/tc252617228/openplant/archive"
	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/internal/protocol"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/stat"
)

const (
	nativeModeRaw  int32 = 0
	nativeModeSpan int32 = 1
	nativeModePlot int32 = 2
	nativeModeArch int32 = 4
	nativeModeFlow int32 = 8
	nativeModeMax  int32 = 9
	nativeModeMin  int32 = 10
	nativeModeAvg  int32 = 11
	nativeModeMean int32 = 12
	nativeModeSum  int32 = 14
	nativeModeStat int32 = 15
)

func (c *Client) QueryArchiveNative(ctx context.Context, q archive.Query) ([]model.Sample, error) {
	out := make([]model.Sample, 0)
	err := c.StreamArchiveNative(ctx, q, func(sample model.Sample) bool {
		out = append(out, sample)
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) StreamArchiveNative(ctx context.Context, q archive.Query, emit func(model.Sample) bool) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if err := q.ValidateNative(); err != nil {
		return err
	}
	if emit == nil {
		return operror.Validation("openplant.Client.StreamArchiveNative", "emit callback is required")
	}
	mode, statMode, err := nativeArchiveMode(q.Mode)
	if err != nil {
		return err
	}
	if q.Quality != model.QualityNone {
		return operror.Unsupported("openplant.Client.StreamArchiveNative", "native archive query does not support quality filtering")
	}
	interval, err := nativeIntervalSeconds(q.Interval)
	if err != nil {
		return err
	}
	ids := uniquePointIDs(q.IDs)
	chunkSize := requestChunkSize(q.ChunkSize, c.options.ChunkSize)
	remaining := q.Limit
	limitReached := false
	for _, chunk := range chunkPointIDs(ids, chunkSize) {
		if limitReached {
			return nil
		}
		payload, err := encodeNativeArchiveRequest(chunk, mode, int32(q.Quality), q.Range.Begin, q.Range.End, interval)
		if err != nil {
			return err
		}
		err = c.rawRoundTripStream(ctx, payload, func(stream io.Reader) error {
			reader := codec.NewReader(stream)
			if statMode {
				return decodeNativeStatsStream(reader, chunk, mode, nil, func(stat model.StatSample) bool {
					if q.Limit > 0 {
						if remaining <= 0 {
							limitReached = true
							return true
						}
						remaining--
					}
					if !emit(sampleFromNativeStat(stat, q.Mode)) {
						return false
					}
					if q.Limit > 0 && remaining == 0 {
						limitReached = true
					}
					return true
				})
			}
			return decodeNativeArchivesStream(reader, chunk, nil, func(sample model.Sample) bool {
				if q.Limit > 0 {
					if remaining <= 0 {
						limitReached = true
						return true
					}
					remaining--
				}
				if !emit(sample) {
					return false
				}
				if q.Limit > 0 && remaining == 0 {
					limitReached = true
				}
				return true
			})
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) QueryStatNative(ctx context.Context, q stat.Query) ([]model.StatSample, error) {
	out := make([]model.StatSample, 0)
	err := c.StreamStatNative(ctx, q, func(sample model.StatSample) bool {
		out = append(out, sample)
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) StreamStatNative(ctx context.Context, q stat.Query, emit func(model.StatSample) bool) error {
	if err := c.ensureOpen(); err != nil {
		return err
	}
	if err := q.ValidateNative(); err != nil {
		return err
	}
	if emit == nil {
		return operror.Validation("openplant.Client.StreamStatNative", "emit callback is required")
	}
	mode, err := nativeStatMode(q.Mode)
	if err != nil {
		return err
	}
	if q.Quality != model.QualityNone {
		return operror.Unsupported("openplant.Client.StreamStatNative", "native stat query does not support quality filtering")
	}
	interval, err := nativeIntervalSeconds(q.Interval)
	if err != nil {
		return err
	}
	ids := uniquePointIDs(q.IDs)
	chunkSize := requestChunkSize(q.ChunkSize, c.options.ChunkSize)
	remaining := q.Limit
	limitReached := false
	for _, chunk := range chunkPointIDs(ids, chunkSize) {
		if limitReached {
			return nil
		}
		payload, err := encodeNativeArchiveRequest(chunk, mode, int32(q.Quality), q.Range.Begin, q.Range.End, interval)
		if err != nil {
			return err
		}
		err = c.rawRoundTripStream(ctx, payload, func(stream io.Reader) error {
			reader := codec.NewReader(stream)
			return decodeNativeStatsStream(reader, chunk, mode, nil, func(sample model.StatSample) bool {
				if q.Limit > 0 {
					if remaining <= 0 {
						limitReached = true
						return true
					}
					remaining--
				}
				if !emit(sample) {
					return false
				}
				if q.Limit > 0 && remaining == 0 {
					limitReached = true
				}
				return true
			})
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func encodeNativeArchiveRequest(ids []model.PointID, mode, quality int32, begin, end time.Time, interval int32) ([]byte, error) {
	beginUnix, err := timeUnix32("openplant.encodeNativeArchiveRequest.begin", begin)
	if err != nil {
		return nil, err
	}
	endUnix, err := timeUnix32("openplant.encodeNativeArchiveRequest.end", end)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 24+len(ids)*24)
	out = codec.AppendInt32(out, protocol.Magic)
	out = codec.AppendInt32(out, int32(protocol.CommandSelect))
	out = codec.AppendInt32(out, int32(protocol.URLArchive))
	out = codec.AppendInt32(out, 0)
	out = codec.AppendInt32(out, int32(len(ids)))
	for _, id := range ids {
		out = codec.AppendInt32(out, int32(id))
		out = codec.AppendInt32(out, mode)
		out = codec.AppendInt32(out, quality)
		out = codec.AppendInt32(out, beginUnix)
		out = codec.AppendInt32(out, endUnix)
		out = codec.AppendInt32(out, interval)
	}
	out = codec.AppendInt32(out, protocol.Magic)
	return out, nil
}

func decodeNativeArchives(raw []byte, ids []model.PointID, idToGN map[model.PointID]model.GN) ([]model.Sample, error) {
	r := codec.NewReader(bytes.NewReader(raw))
	out := make([]model.Sample, 0)
	err := decodeNativeArchivesStream(r, ids, idToGN, func(sample model.Sample) bool {
		out = append(out, sample)
		return true
	})
	return out, err
}

func decodeNativeArchivesStream(r *codec.Reader, ids []model.PointID, idToGN map[model.PointID]model.GN, emit func(model.Sample) bool) error {
	if err := readNativeArchiveHeader(r, len(ids)); err != nil {
		return err
	}
	for {
		next, err := r.ReadInt8()
		if err != nil {
			return operror.Wrap(operror.KindDecode, "openplant.decodeNativeArchives.next", err)
		}
		if next != 1 {
			return readNativeArchiveTail(r)
		}
		index, err := readNativeIndex(r, len(ids))
		if err != nil {
			return err
		}
		id := ids[index]
		typRaw, err := r.ReadInt8()
		if err != nil {
			return operror.Wrap(operror.KindDecode, "openplant.decodeNativeArchives.type", err)
		}
		if typRaw < 0 {
			code, err := r.ReadInt32()
			if err != nil {
				return operror.Wrap(operror.KindDecode, "openplant.decodeNativeArchives.error", err)
			}
			return operror.Server("openplant.decodeNativeArchives", code, fmt.Sprintf("native archive query failed for ID %d", id))
		}
		typ := model.PointType(typRaw & 15)
		count, err := r.ReadInt32()
		if err != nil {
			return operror.Wrap(operror.KindDecode, "openplant.decodeNativeArchives.count", err)
		}
		for i := int32(0); i < count; i++ {
			sample, err := readNativeArchiveSample(r, id, idToGN[id], typ)
			if err != nil {
				return err
			}
			if !emit(sample) {
				return errStreamStopped
			}
		}
	}
}

func decodeNativeStatsStream(r *codec.Reader, ids []model.PointID, mode int32, idToGN map[model.PointID]model.GN, emit func(model.StatSample) bool) error {
	if err := readNativeArchiveHeader(r, len(ids)); err != nil {
		return err
	}
	for {
		next, err := r.ReadInt8()
		if err != nil {
			return operror.Wrap(operror.KindDecode, "openplant.decodeNativeStats.next", err)
		}
		if next != 1 {
			return readNativeArchiveTail(r)
		}
		index, err := readNativeIndex(r, len(ids))
		if err != nil {
			return err
		}
		id := ids[index]
		typRaw, err := r.ReadInt8()
		if err != nil {
			return operror.Wrap(operror.KindDecode, "openplant.decodeNativeStats.type", err)
		}
		if typRaw < 0 {
			code, err := r.ReadInt32()
			if err != nil {
				return operror.Wrap(operror.KindDecode, "openplant.decodeNativeStats.error", err)
			}
			return operror.Server("openplant.decodeNativeStats", code, fmt.Sprintf("native stat query failed for ID %d", id))
		}
		count, err := r.ReadInt32()
		if err != nil {
			return operror.Wrap(operror.KindDecode, "openplant.decodeNativeStats.count", err)
		}
		for i := int32(0); i < count; i++ {
			sample, err := readNativeStatSample(r, id, idToGN[id], mode)
			if err != nil {
				return err
			}
			if !emit(sample) {
				return errStreamStopped
			}
		}
	}
}

func readNativeArchiveHeader(r *codec.Reader, wantRows int) error {
	magic, err := r.ReadInt32()
	if err != nil {
		return operror.Wrap(operror.KindDecode, "openplant.nativeArchive.header.magic", err)
	}
	if magic != protocol.Magic {
		return operror.New(operror.KindProtocol, "openplant.nativeArchive.header", "invalid response magic")
	}
	if _, err := r.ReadInt32(); err != nil {
		return operror.Wrap(operror.KindDecode, "openplant.nativeArchive.header.flag", err)
	}
	rows, err := r.ReadInt32()
	if err != nil {
		return operror.Wrap(operror.KindDecode, "openplant.nativeArchive.header.rows", err)
	}
	if rows != int32(wantRows) {
		return operror.New(operror.KindProtocol, "openplant.nativeArchive.header", fmt.Sprintf("row count mismatch: got %d want %d", rows, wantRows))
	}
	return nil
}

func readNativeArchiveTail(r *codec.Reader) error {
	magic, err := r.ReadInt32()
	if err != nil {
		return operror.Wrap(operror.KindDecode, "openplant.nativeArchive.tail", err)
	}
	if magic != protocol.Magic {
		return operror.New(operror.KindProtocol, "openplant.nativeArchive.tail", "invalid response tail magic")
	}
	return nil
}

func readNativeIndex(r *codec.Reader, count int) (int, error) {
	raw, err := r.ReadInt32()
	if err != nil {
		return 0, operror.Wrap(operror.KindDecode, "openplant.nativeArchive.index", err)
	}
	index := int(uint32(raw))
	if index < 0 || index >= count {
		return 0, operror.New(operror.KindProtocol, "openplant.nativeArchive.index", fmt.Sprintf("index %d out of range", index))
	}
	return index, nil
}

func readNativeArchiveSample(r *codec.Reader, id model.PointID, gn model.GN, typ model.PointType) (model.Sample, error) {
	tm, err := r.ReadInt32()
	if err != nil {
		return model.Sample{}, operror.Wrap(operror.KindDecode, "openplant.nativeArchive.sample.time", err)
	}
	ds, err := r.ReadInt16()
	if err != nil {
		return model.Sample{}, operror.Wrap(operror.KindDecode, "openplant.nativeArchive.sample.status", err)
	}
	value, err := codec.DecodeTSValue(r, typ)
	if err != nil && err != io.EOF {
		return model.Sample{}, operror.Wrap(operror.KindDecode, "openplant.nativeArchive.sample.value", err)
	}
	return model.Sample{
		ID:     id,
		GN:     gn,
		Type:   typ,
		Time:   time.Unix(int64(tm), 0),
		Status: model.DSFromInt16(ds),
		Value:  value,
	}, nil
}

func readNativeStatSample(r *codec.Reader, id model.PointID, gn model.GN, mode int32) (model.StatSample, error) {
	if mode == nativeModeStat {
		var raw [62]byte
		if _, err := io.ReadFull(r, raw[:]); err != nil {
			return model.StatSample{}, operror.Wrap(operror.KindDecode, "openplant.nativeStat.summary", err)
		}
		return model.StatSample{
			ID:      id,
			GN:      gn,
			Time:    time.Unix(int64(codec.Int32(raw[0:4])), 0),
			Status:  model.DSFromInt16(codec.Int16(raw[4:6])),
			Flow:    codec.Float64(raw[6:14]),
			Max:     codec.Float64(raw[14:22]),
			Min:     codec.Float64(raw[22:30]),
			MaxTime: time.Unix(int64(codec.Int32(raw[30:34])), 0),
			MinTime: time.Unix(int64(codec.Int32(raw[34:38])), 0),
			Avg:     codec.Float64(raw[38:46]),
			Mean:    codec.Float64(raw[46:54]),
			Sum:     codec.Float64(raw[54:62]),
		}, nil
	}
	tm, err := r.ReadInt32()
	if err != nil {
		return model.StatSample{}, operror.Wrap(operror.KindDecode, "openplant.nativeStat.time", err)
	}
	ds, err := r.ReadInt16()
	if err != nil {
		return model.StatSample{}, operror.Wrap(operror.KindDecode, "openplant.nativeStat.status", err)
	}
	value, err := r.ReadFloat64()
	if err != nil {
		return model.StatSample{}, operror.Wrap(operror.KindDecode, "openplant.nativeStat.value", err)
	}
	sample := model.StatSample{
		ID:     id,
		GN:     gn,
		Time:   time.Unix(int64(tm), 0),
		Status: model.DSFromInt16(ds),
	}
	switch mode {
	case nativeModeFlow:
		sample.Flow = value
	case nativeModeMax:
		sample.Max = value
	case nativeModeMin:
		sample.Min = value
	case nativeModeAvg:
		sample.Avg = value
	case nativeModeMean:
		sample.Mean = value
	case nativeModeSum:
		sample.Sum = value
	default:
		return model.StatSample{}, operror.Unsupported("openplant.nativeStat.read", fmt.Sprintf("native stat mode %d is not supported", mode))
	}
	return sample, nil
}

func sampleFromNativeStat(stat model.StatSample, mode model.ArchiveMode) model.Sample {
	nativeMode, _ := nativeStatMode(mode)
	value := statValueForNativeMode(stat, nativeMode)
	return model.Sample{
		ID:     stat.ID,
		GN:     stat.GN,
		Type:   model.TypeR8,
		Time:   stat.Time,
		Status: stat.Status,
		Value:  model.R8(value),
	}
}

func statValueForNativeMode(sample model.StatSample, mode int32) float64 {
	switch mode {
	case nativeModeFlow:
		return sample.Flow
	case nativeModeMax:
		return sample.Max
	case nativeModeMin:
		return sample.Min
	case nativeModeMean:
		return sample.Mean
	case nativeModeSum:
		return sample.Sum
	default:
		return sample.Avg
	}
}

func nativeArchiveMode(mode model.ArchiveMode) (int32, bool, error) {
	if mode == "" {
		mode = model.ModeRaw
	}
	switch mode {
	case model.ModeRaw:
		return nativeModeRaw, false, nil
	case model.ModeSpan:
		return nativeModeSpan, false, nil
	case model.ModePlot:
		return nativeModePlot, false, nil
	case model.ModeArch:
		return nativeModeArch, false, nil
	case model.ModeFlow:
		return nativeModeFlow, true, nil
	case model.ModeMax:
		return nativeModeMax, true, nil
	case model.ModeMin:
		return nativeModeMin, true, nil
	case model.ModeAvg:
		return nativeModeAvg, true, nil
	case model.ModeMean:
		return nativeModeMean, true, nil
	case model.ModeSum:
		return nativeModeSum, true, nil
	default:
		return 0, false, operror.Unsupported("openplant.nativeArchiveMode", fmt.Sprintf("native archive query does not support mode %q", mode))
	}
}

func nativeStatMode(mode model.ArchiveMode) (int32, error) {
	if mode == "" {
		return nativeModeAvg, nil
	}
	switch mode {
	case model.ModeFlow:
		return nativeModeFlow, nil
	case model.ModeMax:
		return nativeModeMax, nil
	case model.ModeMin:
		return nativeModeMin, nil
	case model.ModeAvg:
		return nativeModeAvg, nil
	case model.ModeMean:
		return nativeModeMean, nil
	case model.ModeSum:
		return nativeModeSum, nil
	default:
		return 0, operror.Unsupported("openplant.nativeStatMode", fmt.Sprintf("native stat query does not support mode %q", mode))
	}
}

func nativeIntervalSeconds(interval model.Interval) (int32, error) {
	value, err := requestIntervalSeconds(interval)
	if err != nil {
		return 0, err
	}
	seconds, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, operror.Validation("openplant.nativeIntervalSeconds", "interval seconds are invalid")
	}
	return int32(seconds), nil
}
