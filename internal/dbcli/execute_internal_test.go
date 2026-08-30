package dbcli

import (
	"fmt"
	"strings"
	"testing"
	"time"
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

func TestParseRespMalformedEmptyResponse(t *testing.T) {
	if got := fmt.Sprint(parseResp(nil)); !strings.Contains(got, "[fq]> malformed empty response") {
		t.Fatalf("parseResp(nil) = %q", got)
	}
}
