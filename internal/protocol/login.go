package protocol

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/operror"
)

const (
	ChallengeSize     = 100
	LoginReplySize    = 100
	LoginResponseSize = 16
)

type Challenge struct {
	Info    string
	Random  [20]byte
	Version int32
	Raw     [ChallengeSize]byte
}

type LoginResult struct {
	ClientAddress string
	Version       int32
	Info          string
}

func ReadChallenge(ctx context.Context, r *codec.FrameReader) (Challenge, error) {
	var raw [ChallengeSize]byte
	if err := readFullContext(ctx, r, raw[:]); err != nil {
		return Challenge{}, wrapReadError("protocol.ReadChallenge", err)
	}
	return ParseChallenge(raw[:])
}

func ParseChallenge(raw []byte) (Challenge, error) {
	if len(raw) != ChallengeSize {
		return Challenge{}, operror.New(operror.KindProtocol, "protocol.ParseChallenge", "challenge must be 100 bytes")
	}
	var c Challenge
	copy(c.Raw[:], raw)
	n := 60
	for n > 0 && raw[n-1] == 0 {
		n--
	}
	c.Info = string(raw[:n])
	copy(c.Random[:], raw[64:84])
	c.Version = codec.Int32(raw[96:100])
	return c, nil
}

func BuildLoginReply(user, password string, random []byte) ([LoginReplySize]byte, error) {
	var out [LoginReplySize]byte
	if len(user) > 16 {
		return out, operror.New(operror.KindValidation, "protocol.BuildLoginReply", "user must be at most 16 bytes")
	}
	copy(out[44:60], []byte(user))
	if password != "" {
		if len(random) != 20 {
			return out, operror.New(operror.KindProtocol, "protocol.BuildLoginReply", "server random must be 20 bytes")
		}
		reply := ScramblePassword(random, []byte(password))
		codec.PutInt16(out[60:62], int16(len(reply)))
		copy(out[62:82], reply)
	}
	return out, nil
}

func WriteLoginReply(ctx context.Context, w *codec.FrameWriter, user, password string, random []byte) error {
	reply, err := BuildLoginReply(user, password, random)
	if err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := w.WriteMessage(reply[:]); err != nil {
		return operror.Wrap(operror.KindNetwork, "protocol.WriteLoginReply", err)
	}
	return nil
}

func ReadLoginResult(ctx context.Context, r *codec.FrameReader, challenge Challenge) (LoginResult, error) {
	var raw [LoginResponseSize]byte
	if err := readFullContext(ctx, r, raw[:]); err != nil {
		return LoginResult{}, wrapReadError("protocol.ReadLoginResult", err)
	}
	return ParseLoginResult(raw[:], challenge)
}

func ParseLoginResult(raw []byte, challenge Challenge) (LoginResult, error) {
	if len(raw) != LoginResponseSize {
		return LoginResult{}, operror.New(operror.KindProtocol, "protocol.ParseLoginResult", "login response must be 16 bytes")
	}
	ret := codec.Int32(raw[8:12])
	if ret != 0 {
		return LoginResult{}, operror.Server("protocol.ParseLoginResult", ret, "OpenPlant login failed")
	}
	return LoginResult{
		ClientAddress: fmt.Sprintf("%d.%d.%d.%d", raw[4], raw[5], raw[6], raw[7]),
		Version:       challenge.Version,
		Info:          challenge.Info,
	}, nil
}

func ScramblePassword(random []byte, password []byte) []byte {
	stage1 := sha1.Sum(password)
	stage2 := sha1.Sum(stage1[:])
	h := sha1.New()
	_, _ = h.Write(random)
	_, _ = h.Write(stage2[:])
	stage3 := h.Sum(nil)
	for i := 0; i < sha1.Size; i++ {
		stage3[i] ^= stage1[i]
	}
	return stage3
}

func VersionString(version int32) string {
	return fmt.Sprintf("%d.%d.%d", uint8(version>>16), uint8(version>>8), uint8(version))
}

func readFullContext(ctx context.Context, r io.Reader, dst []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	_, err := io.ReadFull(r, dst)
	if err != nil {
		if ctxErr := contextError(ctx); ctxErr != nil {
			return ctxErr
		}
	}
	return err
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func wrapReadError(op string, err error) error {
	if errors.Is(err, context.Canceled) {
		return operror.Wrap(operror.KindCanceled, op, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return operror.Wrap(operror.KindTimeout, op, err)
	}
	return operror.Wrap(operror.KindNetwork, op, err)
}

func WithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		return ctx, func() {}
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
