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
	if got := fmt.Sprint(renderProtocolError(err)); !strings.Contains(got, "[4000] quota not found") {
		t.Fatalf("renderProtocolError(err) = %q", got)
	}
}
