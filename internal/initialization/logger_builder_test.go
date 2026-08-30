package initialization

import (
	"bytes"
	"strings"
	"testing"

	"github.com/fq-db/fq/internal/config"
)

func TestCreateLoggerWritesToProvidedWriter(t *testing.T) {
	var buf bytes.Buffer

	logger, err := CreateLogger(config.LoggingConfig{Level: InfoLevel}, &buf)
	if err != nil {
		t.Fatalf("CreateLogger returned error: %v", err)
	}

	logger.Info().Msg("hello from test")

	if !strings.Contains(buf.String(), "hello from test") {
		t.Errorf("expected buffer to contain log message, got: %q", buf.String())
	}
}
