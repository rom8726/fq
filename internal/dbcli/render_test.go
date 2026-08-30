package dbcli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/fq-db/fq/internal/inspect"
)

func TestRenderReportInstanceSection(t *testing.T) {
	report := &inspect.Report{
		Section: "instance",
		TS:      1700000000,
		Instance: &inspect.InstanceInfo{
			Role:        "master",
			Version:     "1.2.3",
			Commit:      "abcdef1234567890",
			BuildDate:   "2026-01-01",
			GoVersion:   "go1.25",
			Platform:    "linux/amd64",
			UptimeSec:   3661,
			PID:         42,
			ListenAddr:  ":1945",
			Connections: 3,
		},
	}

	var buf bytes.Buffer
	renderReport(&buf, report)

	out := buf.String()
	if !strings.Contains(out, "INSTANCE") {
		t.Errorf("expected output to contain section header INSTANCE, got:\n%s", out)
	}
	if !strings.Contains(out, "1.2.3") {
		t.Errorf("expected output to contain version 1.2.3, got:\n%s", out)
	}
	if !strings.Contains(out, ":1945") {
		t.Errorf("expected output to contain listen address, got:\n%s", out)
	}
}
