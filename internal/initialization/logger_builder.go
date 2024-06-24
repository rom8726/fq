//nolint:gocritic
package initialization

import (
	"errors"
	"os"

	"github.com/rs/zerolog"

	"fq/internal/config"
)

const (
	loggerTimestampFormat = "2006-01-02 15:04:05"
)

const (
	DebugLevel = "debug"
	InfoLevel  = "info"
	WarnLevel  = "warn"
	ErrorLevel = "error"
)

var supportedLoggingLevels = map[string]zerolog.Level{
	DebugLevel: zerolog.DebugLevel,
	InfoLevel:  zerolog.InfoLevel,
	WarnLevel:  zerolog.WarnLevel,
	ErrorLevel: zerolog.ErrorLevel,
}

// const defaultEncoding = "json"
const defaultLevel = zerolog.InfoLevel

func CreateLogger(cfg config.LoggingConfig) (*zerolog.Logger, error) {
	level := defaultLevel

	if cfg.Level != "" {
		var found bool
		if level, found = supportedLoggingLevels[cfg.Level]; !found {
			return nil, errors.New("logging level is incorrect")
		}
	}

	writer := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: loggerTimestampFormat}
	logger := zerolog.New(writer).With().Timestamp().Logger().Level(level)

	return &logger, nil
}
