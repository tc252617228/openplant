package codec

import (
	"encoding/binary"
	"io"
	"math"
	"time"
)

func PutInt16(dst []byte, v int16)     { binary.BigEndian.PutUint16(dst, uint16(v)) }
func PutUint16(dst []byte, v uint16)   { binary.BigEndian.PutUint16(dst, v) }
func PutInt32(dst []byte, v int32)     { binary.BigEndian.PutUint32(dst, uint32(v)) }
func PutUint32(dst []byte, v uint32)   { binary.BigEndian.PutUint32(dst, v) }
func PutInt64(dst []byte, v int64)     { binary.BigEndian.PutUint64(dst, uint64(v)) }
func PutUint64(dst []byte, v uint64)   { binary.BigEndian.PutUint64(dst, v) }
func PutFloat32(dst []byte, v float32) { binary.BigEndian.PutUint32(dst, math.Float32bits(v)) }
func PutFloat64(dst []byte, v float64) { binary.BigEndian.PutUint64(dst, math.Float64bits(v)) }

func Int16(src []byte) int16     { return int16(binary.BigEndian.Uint16(src)) }
func Uint16(src []byte) uint16   { return binary.BigEndian.Uint16(src) }
func Int32(src []byte) int32     { return int32(binary.BigEndian.Uint32(src)) }
func Uint32(src []byte) uint32   { return binary.BigEndian.Uint32(src) }
func Int64(src []byte) int64     { return int64(binary.BigEndian.Uint64(src)) }
func Uint64(src []byte) uint64   { return binary.BigEndian.Uint64(src) }
func Float32(src []byte) float32 { return math.Float32frombits(binary.BigEndian.Uint32(src)) }
func Float64(src []byte) float64 { return math.Float64frombits(binary.BigEndian.Uint64(src)) }

func PutDateTime(dst []byte, v time.Time) {
	PutFloat64(dst, float64(v.UnixNano()/1e6)/1e3)
}

func AppendInt16(dst []byte, v int16) []byte {
	var buf [2]byte
	PutInt16(buf[:], v)
	return append(dst, buf[:]...)
}

func AppendInt32(dst []byte, v int32) []byte {
	var buf [4]byte
	PutInt32(buf[:], v)
	return append(dst, buf[:]...)
}

func AppendFloat64(dst []byte, v float64) []byte {
	var buf [8]byte
	PutFloat64(buf[:], v)
	return append(dst, buf[:]...)
}

func DateTime(src []byte) time.Time {
	seconds := Float64(src)
	sec := int64(seconds)
	nsec := int64(seconds*1e3) % 1000 * 1e6
	return time.Unix(sec, nsec)
}

type Writer struct {
	w       io.Writer
	scratch [8]byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

func (w *Writer) WriteByte(v byte) error {
	w.scratch[0] = v
	_, err := w.w.Write(w.scratch[:1])
	return err
}

func (w *Writer) WriteInt8(v int8) error {
	return w.WriteByte(byte(v))
}

func (w *Writer) WriteInt16(v int16) error {
	PutInt16(w.scratch[:2], v)
	_, err := w.w.Write(w.scratch[:2])
	return err
}

func (w *Writer) WriteInt32(v int32) error {
	PutInt32(w.scratch[:4], v)
	_, err := w.w.Write(w.scratch[:4])
	return err
}

func (w *Writer) WriteInt64(v int64) error {
	PutInt64(w.scratch[:8], v)
	_, err := w.w.Write(w.scratch[:8])
	return err
}

func (w *Writer) WriteFloat32(v float32) error {
	PutFloat32(w.scratch[:4], v)
	_, err := w.w.Write(w.scratch[:4])
	return err
}

func (w *Writer) WriteFloat64(v float64) error {
	PutFloat64(w.scratch[:8], v)
	_, err := w.w.Write(w.scratch[:8])
	return err
}

type Reader struct {
	r       io.Reader
	scratch [8]byte
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

func (r *Reader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func (r *Reader) ReadByte() (byte, error) {
	_, err := io.ReadFull(r.r, r.scratch[:1])
	return r.scratch[0], err
}

func (r *Reader) ReadInt8() (int8, error) {
	v, err := r.ReadByte()
	return int8(v), err
}

func (r *Reader) ReadInt16() (int16, error) {
	_, err := io.ReadFull(r.r, r.scratch[:2])
	return Int16(r.scratch[:2]), err
}

func (r *Reader) ReadInt32() (int32, error) {
	_, err := io.ReadFull(r.r, r.scratch[:4])
	return Int32(r.scratch[:4]), err
}

func (r *Reader) ReadInt64() (int64, error) {
	_, err := io.ReadFull(r.r, r.scratch[:8])
	return Int64(r.scratch[:8]), err
}

func (r *Reader) ReadFloat32() (float32, error) {
	_, err := io.ReadFull(r.r, r.scratch[:4])
	return Float32(r.scratch[:4]), err
}

func (r *Reader) ReadFloat64() (float64, error) {
	_, err := io.ReadFull(r.r, r.scratch[:8])
	return Float64(r.scratch[:8]), err
}
