package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/peterh/liner"
	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/dbcli"
	"github.com/fq-db/fq/internal/security"
	"github.com/fq-db/fq/internal/tools"
	"github.com/fq-db/fq/internal/version"
)

const loggerTimestampFormat = "2006-01-02 15:04:05"

func main() {
	if version.Requested(os.Args[1:]) {
		fmt.Println(version.String())

		return
	}

	address := flag.String("address", ":1945", "Address of the database")
	idleTimeout := flag.Duration("idle_timeout", time.Minute, "Idle timeout for connection")
	maxMessageSizeStr := flag.String("max_message_size", "4KB", "Max message size for connection")
	token := flag.String("token", os.Getenv("FQ_TOKEN"), "Authentication token")
	tlsCA := flag.String("tls_ca", "", "CA certificate file used to verify the server")
	tlsCert := flag.String("tls_cert", "", "Client certificate file for mutual TLS")
	tlsKey := flag.String("tls_key", "", "Client key file for mutual TLS")
	tlsServerName := flag.String("tls_server_name", "", "Expected server name in the certificate")
	tlsSkipVerify := flag.Bool("tls_skip_verify", false, "Skip server certificate verification")
	flag.Parse()

	logger := consoleLogger()
	maxMessageSize, err := tools.ParseSize(*maxMessageSizeStr)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to parse max message size")
	}

	client, err := dbcli.Connect(context.Background(), dbcli.ConnectOptions{
		Address:        *address,
		MaxMessageSize: maxMessageSize,
		IdleTimeout:    *idleTimeout,
		Token:          *token,
		TLS: security.TLSOptions{
			CAFile:     *tlsCA,
			CertFile:   *tlsCert,
			KeyFile:    *tlsKey,
			ServerName: *tlsServerName,
			SkipVerify: *tlsSkipVerify,
		},
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect with server")
	}

	line := liner.NewLiner()
	defer func() { _ = line.Close() }()

	line.SetCtrlCAborts(true)

	for {
		request, err := line.Prompt("[fq]> ")
		if err != nil {
			if errors.Is(err, liner.ErrPromptAborted) {
				break
			}

			if errors.Is(err, syscall.EPIPE) {
				logger.Fatal().Err(err).Msg("connection was closed")
			}

			logger.Fatal().Err(err).Msg("failed to read user query")
		}

		if request == "" {
			continue
		}

		if dbcli.IsQuitCommand(request) {
			return
		}

		line.AppendHistory(request)

		start := time.Now()
		if err := dbcli.Execute(context.Background(), logger, client, request, os.Stdout, start); err != nil {
			if errors.Is(err, syscall.EPIPE) {
				logger.Fatal().Err(err).Msg("connection was closed")
			}

			logger.Fatal().Err(err).Msg("command failed")
		}
	}
}

func consoleLogger() *zerolog.Logger {
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: loggerTimestampFormat}
	logger := zerolog.New(consoleWriter).
		With().
		Timestamp().
		Logger()

	return &logger
}
