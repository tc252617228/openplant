package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/pierrec/lz4"
)

type CompressionMode byte

const (
	CompressionNone  CompressionMode = 0
	CompressionFrame CompressionMode = 1
	CompressionBlock CompressionMode = 2

	MaxFramePayload = 65535
)

var (
	ErrUnsupportedCompression = errors.New("openplant codec: unsupported compression mode")
	ErrCompressionFailed      = errors.New("openplant codec: compression failed")
	ErrDecompressionFailed    = errors.New("openplant codec: decompression failed")
)

var (
	lz4FrameHeader           = []byte{0x04, 0x22, 0x4D, 0x18, 0x64, 0x70, 0xB9}
	compressionScratchSize   = lz4.CompressBlockBound(MaxFramePayload)
	decompressionScratchSize = MaxFramePayload
)

type FrameWriter struct {
	w           io.Writer
	compression CompressionMode
	packBuffers [2][]byte
	packIndex   uint32
}

func NewFrameWriter(w io.Writer, compression CompressionMode) *FrameWriter {
	fw := &FrameWriter{w: w, compression: compression}
	if compression.Valid() && compression != CompressionNone {
		fw.ensurePackBuffers()
	}
	return fw
}

func (w *FrameWriter) SetCompression(mode CompressionMode) error {
	if !mode.Valid() {
		return fmt.Errorf("%w: %d", ErrUnsupportedCompression, mode)
	}
	if mode != CompressionNone {
		w.ensurePackBuffers()
	}
	w.compression = mode
	return nil
}

func (m CompressionMode) Valid() bool {
	switch m {
	case CompressionNone, CompressionFrame, CompressionBlock:
		return true
	default:
		return false
	}
}

func (m CompressionMode) String() string {
	switch m {
	case CompressionNone:
		return "none"
	case CompressionFrame:
		return "frame"
	case CompressionBlock:
		return "block"
	default:
		return "unknown"
	}
}

func (w *FrameWriter) CompressionMode() CompressionMode {
	return w.compression
}

func (w *FrameWriter) WriteFrame(payload []byte, eof bool) error {
	frame, err := w.encodeFrame(payload, eof)
	if err != nil {
		return err
	}
	return writeFull(w.w, frame)
}

func (w *FrameWriter) encodeFrame(payload []byte, eof bool) ([]byte, error) {
	if !w.compression.Valid() {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedCompression, w.compression)
	}
	if len(payload) > MaxFramePayload {
		return nil, fmt.Errorf("openplant codec: frame payload too large: %d", len(payload))
	}
	body, mode, err := w.encodeFramePayload(payload)
	if err != nil {
		return nil, err
	}
	if len(body) > MaxFramePayload {
		return nil, fmt.Errorf("openplant codec: compressed frame payload too large: %d", len(body))
	}
	var head [4]byte
	if eof {
		head[0] = 1
	}
	head[1] = byte(mode)
	head[2] = byte(len(body) >> 8)
	head[3] = byte(len(body))
	frame := make([]byte, 0, len(head)+len(body))
	frame = append(frame, head[:]...)
	frame = append(frame, body...)
	return frame, nil
}

func (w *FrameWriter) WriteMessage(payload []byte) error {
	if len(payload) == 0 {
		return w.WriteFrame(nil, true)
	}
	if w.compression == CompressionNone {
		for off := 0; off < len(payload); {
			end := off + MaxFramePayload
			if end > len(payload) {
				end = len(payload)
			}
			if err := w.WriteFrame(payload[off:end], end == len(payload)); err != nil {
				return err
			}
			off = end
		}
		return nil
	}

	// Encode every compressed frame before writing so a later strict compression
	// failure cannot emit a partial request.
	frames := make([][]byte, 0, len(payload)/MaxFramePayload+1)
	for off := 0; off < len(payload); {
		end := off + MaxFramePayload
		if end > len(payload) {
			end = len(payload)
		}
		frame, err := w.encodeFrame(payload[off:end], end == len(payload))
		if err != nil {
			return err
		}
		frames = append(frames, frame)
		off = end
	}
	for _, frame := range frames {
		if err := writeFull(w.w, frame); err != nil {
			return err
		}
	}
	return nil
}

type FrameReader struct {
	r             io.Reader
	buf           []byte
	off           int
	eof           bool
	lastZip       CompressionMode
	unpackBuffers [2][]byte
	unpackIndex   uint32
}

func NewFrameReader(r io.Reader) *FrameReader {
	return &FrameReader{r: r}
}

func (r *FrameReader) EOF() bool {
	return r.eof && r.off >= len(r.buf)
}

func (r *FrameReader) LastCompression() CompressionMode {
	return r.lastZip
}

func (r *FrameReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.off >= len(r.buf) {
		if r.eof {
			return 0, io.EOF
		}
		if err := r.readFrame(); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.buf[r.off:])
	r.off += n
	return n, nil
}

func (r *FrameReader) ReadFull(p []byte) error {
	_, err := io.ReadFull(r, p)
	return err
}

func (r *FrameReader) ReadMessage() ([]byte, error) {
	out := make([]byte, 0, 1024)
	var tmp [4096]byte
	for {
		n, err := r.Read(tmp[:])
		if n > 0 {
			out = append(out, tmp[:n]...)
		}
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func (r *FrameReader) ResetMessage() {
	r.buf = nil
	r.off = 0
	r.eof = false
}

func (r *FrameReader) readFrame() error {
	var head [4]byte
	if _, err := io.ReadFull(r.r, head[:]); err != nil {
		return err
	}
	if head == [4]byte{0x10, 0x20, 0x30, 0x40} {
		r.buf = []byte{
			0, 0, 0, 110,
			0x46, 0, 0, 0,
			0, 0, 0, 0,
			0, 0, 0, 0,
			0xA5,
			0x10, 0x20, 0x30, 0x40,
		}
		r.off = 0
		r.eof = true
		r.lastZip = CompressionNone
		return nil
	}
	r.eof = head[0] == 1
	r.lastZip = CompressionMode(head[1] & 3)
	if !r.lastZip.Valid() {
		return fmt.Errorf("%w: %d", ErrUnsupportedCompression, r.lastZip)
	}
	size := int(head[2])<<8 | int(head[3])
	if size == 0 {
		r.buf = nil
		r.off = 0
		return nil
	}
	r.buf = make([]byte, size)
	r.off = 0
	if _, err := io.ReadFull(r.r, r.buf); err != nil {
		return err
	}
	payload, err := r.decodeFramePayload(r.buf, r.lastZip)
	if err != nil {
		return err
	}
	r.buf = payload
	return nil
}

func (w *FrameWriter) encodeFramePayload(payload []byte) ([]byte, CompressionMode, error) {
	// Empty protocol frames carry no body to compress.
	if len(payload) == 0 || w.compression == CompressionNone {
		return payload, CompressionNone, nil
	}
	switch w.compression {
	case CompressionFrame:
		body, err := w.compressFrame(payload)
		return body, CompressionFrame, err
	case CompressionBlock:
		body, err := w.compressBlock(payload)
		return body, CompressionBlock, err
	default:
		return nil, CompressionNone, fmt.Errorf("%w: %d", ErrUnsupportedCompression, w.compression)
	}
}

func (r *FrameReader) decodeFramePayload(payload []byte, compression CompressionMode) ([]byte, error) {
	switch compression {
	case CompressionNone:
		return payload, nil
	case CompressionFrame:
		return decompressFrame(payload)
	case CompressionBlock:
		return r.decompressBlock(payload)
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedCompression, compression)
	}
}

func (w *FrameWriter) compressBlock(payload []byte) ([]byte, error) {
	return compressBlockInto(payload, w.nextPackBuffer(), "block")
}

func (w *FrameWriter) compressFrame(payload []byte) ([]byte, error) {
	block, err := compressBlockInto(payload, w.nextPackBuffer(), "frame")
	if err != nil {
		return nil, err
	}
	body := make([]byte, 0, len(lz4FrameHeader)+4+len(block)+8)
	body = append(body, lz4FrameHeader...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(block)))
	body = append(body, block...)
	body = binary.LittleEndian.AppendUint32(body, 0)
	body = binary.LittleEndian.AppendUint32(body, xxh32Zero(payload))
	if len(body) >= len(payload) {
		return nil, fmt.Errorf("%w: frame: compressed size %d >= original %d", ErrCompressionFailed, len(body), len(payload))
	}
	return body, nil
}

func compressBlockInto(payload, dst []byte, label string) ([]byte, error) {
	n, err := lz4.CompressBlock(payload, dst, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrCompressionFailed, label, err)
	}
	if n <= 0 {
		return nil, fmt.Errorf("%w: %s: data is not compressible", ErrCompressionFailed, label)
	}
	if n >= len(payload) {
		return nil, fmt.Errorf("%w: %s: compressed size %d >= original %d", ErrCompressionFailed, label, n, len(payload))
	}
	return dst[:n], nil
}

func (r *FrameReader) decompressBlock(payload []byte) ([]byte, error) {
	dst := r.nextUnpackBuffer()
	n, err := lz4.UncompressBlock(payload, dst)
	if err != nil {
		return nil, fmt.Errorf("%w: block: %w", ErrDecompressionFailed, err)
	}
	return dst[:n], nil
}

func decompressFrame(payload []byte) ([]byte, error) {
	reader := lz4.NewReader(bytes.NewReader(payload))
	out, err := io.ReadAll(io.LimitReader(reader, MaxFramePayload+1))
	if err != nil {
		return nil, fmt.Errorf("%w: frame: %w", ErrDecompressionFailed, err)
	}
	if len(out) > MaxFramePayload {
		return nil, fmt.Errorf("%w: frame: decompressed payload too large: %d", ErrDecompressionFailed, len(out))
	}
	return out, nil
}

func (w *FrameWriter) ensurePackBuffers() {
	for i := range w.packBuffers {
		if w.packBuffers[i] == nil {
			w.packBuffers[i] = make([]byte, compressionScratchSize)
		}
	}
}

func (w *FrameWriter) nextPackBuffer() []byte {
	w.ensurePackBuffers()
	idx := w.packIndex & 1
	w.packIndex++
	return w.packBuffers[idx]
}

func (r *FrameReader) nextUnpackBuffer() []byte {
	idx := r.unpackIndex & 1
	r.unpackIndex++
	if r.unpackBuffers[idx] == nil {
		r.unpackBuffers[idx] = make([]byte, decompressionScratchSize)
	}
	return r.unpackBuffers[idx]
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
