package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"

	"fq/internal/config"
	"fq/internal/initialization"
)

const (
	loggerTimestampFormat = "2006-01-02 15:04:05"
)

var Commit = ""

func main() {
	console := consoleLogger()
	if err := run(console); err != nil {
		console.Error().Msg(err.Error())
		os.Exit(1)
	}
}

func run(console *zerolog.Logger) error {
	console.Info().Msg("init config...")
	cfg, err := config.Init()
	if err != nil {
		return fmt.Errorf("init config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	console.Info().Msg("initialize database...")
	initializer, err := initialization.NewInitializer(cfg)
	if err != nil {
		console.Fatal().Err(err).Msg("init initializer")
	}

	console.Info().Msg("start database...")
	if err = initializer.StartDatabase(ctx); err != nil {
		console.Fatal().Err(err).Msg("start database")
	}

	return nil
}

func consoleLogger() *zerolog.Logger {
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: loggerTimestampFormat}
	logger := zerolog.New(consoleWriter).
		With().
		Timestamp().
		Logger()

	return &logger
}
