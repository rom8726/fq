package dbcli

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/fq-db/fq/internal/protocol"
)

func TestTimeoutFor(t *testing.T) {
	cases := map[string]time.Duration{
		"WATCH foo": watchTimeout,
		"STREAM":    streamTimeout,
		"GET foo":   defaultTimeout,
		"INSPECT":   defaultTimeout,
	}
	for input, want := range cases {
		if got := TimeoutFor(input); got != want {
			t.Errorf("TimeoutFor(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestRenderProtocolError(t *testing.T) {
	err := protocol.NewError(protocol.CodeQuotaNotFound, "quota not found")
	if got := fmt.Sprint(renderProtocolError("[fq]> ", err)); !strings.Contains(got, "[fq]> [4000] quota not found") {
		t.Fatalf("renderProtocolError(err) = %q", got)
	}
}

func TestRenderErrorIncludesProtocolCode(t *testing.T) {
	err := protocol.NewError(protocol.CodePermissionDenied, "permission denied")
	if got := fmt.Sprint(renderError("error: ", err)); !strings.Contains(got, "error: [3001] permission denied") {
		t.Fatalf("renderError(err) = %q", got)
	}
}
