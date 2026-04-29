package protocol

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/operror"
)

func TestChallengeAndLoginReply(t *testing.T) {
	raw := make([]byte, ChallengeSize)
	copy(raw[:60], []byte("openPlant test"))
	for i := 0; i < 20; i++ {
		raw[64+i] = byte(i + 1)
	}
	codec.PutInt32(raw[96:100], 0x00050102)

	challenge, err := ParseChallenge(raw)
	if err != nil {
		t.Fatalf("ParseChallenge failed: %v", err)
	}
	if challenge.Info != "openPlant test" {
		t.Fatalf("info=%q", challenge.Info)
	}
	if VersionString(challenge.Version) != "5.1.2" {
		t.Fatalf("version=%s", VersionString(challenge.Version))
	}

	reply, err := BuildLoginReply("test-user", "test-secret", challenge.Random[:])
	if err != nil {
		t.Fatalf("BuildLoginReply failed: %v", err)
	}
	if got := string(bytes.TrimRight(reply[44:60], "\x00")); got != "test-user" {
		t.Fatalf("user offset mismatch: %q", got)
	}
	if got := codec.Int16(reply[60:62]); got != 20 {
		t.Fatalf("scramble length=%d", got)
	}
}

func TestParseLoginResult(t *testing.T) {
	challenge := Challenge{Info: "server", Version: 0x00050004}
	raw := make([]byte, LoginResponseSize)
	copy(raw[4:8], []byte{127, 0, 0, 1})
	got, err := ParseLoginResult(raw, challenge)
	if err != nil {
		t.Fatalf("ParseLoginResult failed: %v", err)
	}
	if got.ClientAddress != "127.0.0.1" || got.Info != "server" || got.Version != challenge.Version {
		t.Fatalf("unexpected login result: %#v", got)
	}

	codec.PutInt32(raw[8:12], -7)
	if _, err := ParseLoginResult(raw, challenge); err == nil {
		t.Fatalf("expected server error")
	}
}

func TestReadChallengeClassifiesCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ReadChallenge(ctx, codec.NewFrameReader(bytes.NewReader(nil)))
	if err == nil || !operror.IsKind(err, operror.KindCanceled) {
		t.Fatalf("expected canceled error, got %v", err)
	}
}

func TestReadLoginResultClassifiesExpiredContext(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := ReadLoginResult(ctx, codec.NewFrameReader(bytes.NewReader(nil)), Challenge{})
	if err == nil || !operror.IsKind(err, operror.KindTimeout) {
		t.Fatalf("expected timeout error, got %v", err)
	}
}
