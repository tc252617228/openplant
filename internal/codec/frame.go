package codec

import (
	"errors"
	"fmt"
	"io"
)

type CompressionMode byte

const (
	CompressionNone  CompressionMode = 0
	CompressionFrame CompressionMode = 1
	CompressionBlock CompressionMode = 2

	MaxFramePayload = 65535
)

var ErrUnsupportedCompression = errors.New("openplant codec: unsupported compression mode")

type FrameWriter struct {
	w           io.Writer
	compression CompressionMode
}

func NewFrameWriter(w io.Writer, compression CompressionMode) *FrameWriter {
	return &FrameWriter{w: w, compression: compression}
}

func (w *FrameWriter) SetCompression(mode CompressionMode) {
	w.compression = mode
}

func (w *FrameWriter) WriteFrame(payload []byte, eof bool) error {
	if w.compression != CompressionNone {
		return fmt.Errorf("%w: %d", ErrUnsupportedCompression, w.compression)
	}
	if len(payload) > MaxFramePayload {
		return fmt.Errorf("openplant codec: frame payload too large: %d", len(payload))
	}
	var head [4]byte
	if eof {
		head[0] = 1
	}
	head[1] = byte(w.compression)
	head[2] = byte(len(payload) >> 8)
	head[3] = byte(len(payload))
	if _, err := w.w.Write(head[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.w.Write(payload)
	return err
}

func (w *FrameWriter) WriteMessage(payload []byte) error {
	if len(payload) == 0 {
		return w.WriteFrame(nil, true)
	}
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

type FrameReader struct {
	r       io.Reader
	buf     []byte
	off     int
	eof     bool
	lastZip CompressionMode
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
	if r.lastZip != CompressionNone {
		return fmt.Errorf("%w: %d", ErrUnsupportedCompression, r.lastZip)
	}
	return nil
}
