package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/logrusorgru/aurora/v4"
	"github.com/peterh/liner"
	"github.com/rs/zerolog"

	"fq/internal/network"
	"fq/internal/tools"
)

const (
	loggerTimestampFormat = "2006-01-02 15:04:05"
)

func main() {
	address := flag.String("address", ":1945", "Address of the database")
	idleTimeout := flag.Duration("idle_timeout", time.Minute, "Idle timeout for connection")
	maxMessageSizeStr := flag.String("max_message_size", "4KB", "Max message size for connection")
	flag.Parse()

	logger := consoleLogger()
	maxMessageSize, err := tools.ParseSize(*maxMessageSizeStr)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to parse max message size")
	}

	client, err := network.NewTCPClient(*address, maxMessageSize, *idleTimeout)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect with server")
	}

	line := liner.NewLiner()
	defer line.Close()

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

		if request == "q" || request == "quit" || request == "exit" {
			return
		}

		line.AppendHistory(request)

		func() {
			start := time.Now()
			ctx, cancel := context.WithDeadline(context.Background(), start.Add(time.Minute))
			defer cancel()

			response, err := client.Send(ctx, []byte(request))
			elapsed := time.Since(start)
			if err != nil {
				if errors.Is(err, syscall.EPIPE) {
					logger.Fatal().Err(err).Msg("connection was closed")
				}

				logger.Fatal().Err(err).Msg("failed to send query")
			}

			fmt.Printf("%s\t\t\t\tElapsed: %s\n", parseResp(response), elapsed.String())
		}()
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

func parseResp(response []byte) aurora.Value {
	idx := bytes.IndexByte(response, '|')
	status := string(response[:idx])
	data := string(response[idx+1:])
	if status == "ok" {
		return aurora.Green("[fq]> " + data)
	}

	return aurora.Red("[fq]> " + data)
}
