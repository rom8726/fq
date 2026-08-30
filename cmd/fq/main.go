package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/initialization"
	"github.com/fq-db/fq/internal/tui"
	"github.com/fq-db/fq/internal/version"
)

const (
	loggerTimestampFormat = "2006-01-02 15:04:05"
	defaultConfigPath     = "config.yml"
)

func main() {
	if version.Requested(os.Args[1:]) {
		fmt.Println(version.String())

		return
	}

	interactive := flag.Bool("i", false, "start in interactive mode (split-pane log + embedded CLI)")
	flag.Parse()

	configPath := defaultConfigPath
	if flag.NArg() > 0 {
		configPath = flag.Arg(0)
	}

	if *interactive {
		if err := runInteractive(configPath); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		return
	}

	console := consoleLogger()
	if err := run(console, configPath); err != nil {
		console.Error().Msg(err.Error())
		os.Exit(1)
	}
}

func run(console *zerolog.Logger, configPath string) error {
	console.Info().Msg(version.String())
	console.Info().Msg("init config...")
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("init config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	console.Info().Msg("initialize database...")
	initializer, err := initialization.NewInitializer(cfg)
	if err != nil {
		return fmt.Errorf("init initializer: %w", err)
	}

	console.Info().Msg("start database...")
	if err = initializer.StartDatabase(ctx); err != nil {
		return fmt.Errorf("start database: %w", err)
	}

	return nil
}

func runInteractive(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("init config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	uiApp := tui.New()

	logger, err := initialization.CreateLogger(cfg.Logging, uiApp.LogWriter())
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	logger.Info().Msg(version.String())

	initializer, err := initialization.NewInitializerWithLogger(cfg, logger)
	if err != nil {
		return fmt.Errorf("init initializer: %w", err)
	}

	maxMessageSize, err := cfg.Network.ParseMaxMessageSize()
	if err != nil {
		return fmt.Errorf("parse max message size: %w", err)
	}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- initializer.StartDatabase(ctx)
	}()

	runErr := uiApp.Run(ctx, cancel, tui.Config{
		Address:        cfg.Network.Address,
		MaxMessageSize: maxMessageSize,
		IdleTimeout:    cfg.Network.IdleTimeout,
		Logger:         logger,
	})

	dbErr := <-serverErr
	if runErr != nil {
		return fmt.Errorf("interactive UI: %w", runErr)
	}
	if dbErr != nil {
		return fmt.Errorf("start database: %w", dbErr)
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
