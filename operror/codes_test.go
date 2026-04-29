package operror

import (
	"errors"
	"strings"
	"testing"
)

func TestLookupServerCodeFromOPConsoleLocale(t *testing.T) {
	tests := []struct {
		code      ServerCode
		name      string
		class     ServerCodeClass
		reconnect bool
	}{
		{ServerCodeNetworkIOReconnect, "NetworkIOReconnect", ServerCodeClassTransport, true},
		{ServerCodeParameterError, "ParameterError", ServerCodeClassProtocol, false},
		{ServerCodeNoArchive, "NoArchive", ServerCodeClassHistory, false},
		{ServerCodePasswordMismatch, "PasswordMismatch", ServerCodeClassAuth, false},
	}
	for _, tt := range tests {
		info, ok := LookupServerCode(int32(tt.code))
		if !ok {
			t.Fatalf("LookupServerCode(%d) not found", tt.code)
		}
		if info.Name != tt.name || info.Class != tt.class || info.Reconnect != tt.reconnect {
			t.Fatalf("LookupServerCode(%d)=%#v", tt.code, info)
		}
	}
}

func TestResourceLockedUsesContiguousMinus115Code(t *testing.T) {
	info, ok := LookupServerCode(-115)
	if !ok {
		t.Fatalf("expected -115 resource locked code")
	}
	if info.Name != "ResourceLocked" || info.Message != "Resource locked" {
		t.Fatalf("unexpected -115 info: %#v", info)
	}
}

func TestServerErrorUsesStandardMessageWhenEmpty(t *testing.T) {
	err := Server("test", int32(ServerCodeParameterError), "")
	if !strings.Contains(err.Error(), "Parameter error") {
		t.Fatalf("server error did not include standard message: %v", err)
	}
	if !IsKind(err, KindServer) {
		t.Fatalf("expected server kind")
	}
	if !IsServerCode(err, int32(ServerCodeParameterError)) {
		t.Fatalf("expected parameter error code")
	}
	code, ok := ServerErrorCode(err)
	if !ok || code != int32(ServerCodeParameterError) {
		t.Fatalf("ServerErrorCode=%d,%v", code, ok)
	}
}

func TestServerCodeHelpersRejectNonServerErrors(t *testing.T) {
	if code, ok := ServerErrorCode(errors.New("plain")); ok || code != 0 {
		t.Fatalf("plain error returned server code %d,%v", code, ok)
	}
	if ServerCodeRequiresReconnect(int32(ServerCodeParameterError)) {
		t.Fatalf("parameter errors should not request reconnect")
	}
	if !ServerCodeRequiresReconnect(int32(ServerCodeNetworkIOReconnect)) {
		t.Fatalf("network IO errors should request reconnect")
	}
}
