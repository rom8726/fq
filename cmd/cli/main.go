package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/logrusorgru/aurora/v4"
	"github.com/peterh/liner"
	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/inspect"
	"github.com/fq-db/fq/internal/network"
	"github.com/fq-db/fq/internal/tools"
	"github.com/fq-db/fq/internal/version"
)

const (
	loggerTimestampFormat = "2006-01-02 15:04:05"
	wireInspectCommand    = "INSPECT"
)

func main() {
	if version.Requested(os.Args[1:]) {
		fmt.Println(version.String())

		return
	}

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

		if request == "q" || request == "quit" || request == "exit" {
			return
		}

		line.AppendHistory(request)

		func() {
			start := time.Now()

			// For WATCH command, use longer timeout (30 seconds)
			timeout := time.Minute
			if isWatchCommand(request) {
				timeout = 30 * time.Second
				fmt.Println("Watching for changes... (press Ctrl+C to cancel)")
			}
			if isStreamCommand(request) {
				timeout = time.Hour
				fmt.Println("Streaming events... (press Ctrl+C to cancel)")
			}

			ctx, cancel := context.WithDeadline(context.Background(), start.Add(timeout))
			defer cancel()

			if isStreamCommand(request) {
				err := client.Stream(ctx, []byte(request), func(response []byte) error {
					fmt.Printf("%s\t\t\t\tElapsed: %s\n", parseResp(response), time.Since(start).String())

					return nil
				})
				if err != nil {
					if errors.Is(err, network.ErrIdleTimeout) {
						fmt.Println("Stream idle timeout")

						return
					}

					if errors.Is(err, context.DeadlineExceeded) {
						fmt.Println("Stream timeout")
						return
					}

					logger.Fatal().Err(err).Msg("failed to stream query")
				}

				return
			}

			if isHumanInspectCommand(request) {
				runHumanInspectCommand(ctx, logger, client, request, start)

				return
			}

			if isInspectCommand(request) {
				runInspectCommand(ctx, logger, client, request, start)

				return
			}

			response, err := client.Send(ctx, []byte(request))
			elapsed := time.Since(start)
			if err != nil {
				if errors.Is(err, syscall.EPIPE) {
					logger.Fatal().Err(err).Msg("connection was closed")
				}

				if errors.Is(err, context.DeadlineExceeded) && isWatchCommand(request) {
					fmt.Println("Watch timeout: no changes detected")
					return
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

func fetchChunkedBody(ctx context.Context, client *network.TCPClient, query string) ([]byte, error) {
	var body bytes.Buffer
	err := client.Stream(ctx, []byte(query), func(frame []byte) error {
		idx := bytes.IndexByte(frame, '|')
		if idx < 0 {
			return fmt.Errorf("malformed response frame")
		}

		status := string(frame[:idx])
		data := frame[idx+1:]

		switch status {
		case "nxt":
			body.Write(data)
			return nil
		case "ok":
			body.Write(data)
			return io.EOF
		case "err":
			return fmt.Errorf("%s", data)
		default:
			return fmt.Errorf("unexpected frame status %q", status)
		}
	})
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return body.Bytes(), nil
}

func runInspectCommand(
	ctx context.Context,
	logger *zerolog.Logger,
	client *network.TCPClient,
	request string,
	start time.Time,
) {
	body, err := fetchChunkedBody(ctx, client, request)
	elapsed := time.Since(start)
	if err != nil {
		fmt.Printf("%s\t\t\t\tElapsed: %s\n", aurora.Red("[fq]> "+err.Error()), elapsed.String())

		return
	}

	var pretty bytes.Buffer
	if jsonErr := json.Indent(&pretty, body, "", "  "); jsonErr != nil {
		logger.Warn().Err(jsonErr).Msg("failed to pretty-print inspect report")
		fmt.Printf("%s\t\t\t\tElapsed: %s\n", aurora.Green("[fq]> "+string(body)), elapsed.String())

		return
	}

	fmt.Printf("%s\nElapsed: %s\n", aurora.Green(pretty.String()), elapsed.String())
}

func runHumanInspectCommand(
	ctx context.Context,
	logger *zerolog.Logger,
	client *network.TCPClient,
	request string,
	start time.Time,
) {
	wireQuery := toWireInspectQuery(request)

	body, err := fetchChunkedBody(ctx, client, wireQuery)
	elapsed := time.Since(start)
	if err != nil {
		fmt.Printf("%s\nElapsed: %s\n", aurora.Red("error: "+err.Error()), elapsed.String())

		return
	}

	var report inspect.Report
	if jsonErr := json.Unmarshal(body, &report); jsonErr != nil {
		logger.Warn().Err(jsonErr).Msg("failed to parse inspect report")
		msg := aurora.Red("error: failed to parse inspect report: " + jsonErr.Error())
		fmt.Printf("%s\nElapsed: %s\n", msg, elapsed.String())

		return
	}

	renderReport(os.Stdout, &report)
	fmt.Printf("\nElapsed: %s\n", elapsed.String())
}

func toWireInspectQuery(request string) string {
	fields := strings.Fields(request)
	if len(fields) == 0 {
		return wireInspectCommand
	}

	fields[0] = wireInspectCommand

	return strings.Join(fields, " ")
}

func isInspectCommand(request string) bool {
	upperRequest := strings.ToUpper(strings.TrimSpace(request))
	return upperRequest == wireInspectCommand || strings.HasPrefix(upperRequest, wireInspectCommand+" ")
}

func isHumanInspectCommand(request string) bool {
	upperRequest := strings.ToUpper(strings.TrimSpace(request))
	return upperRequest == "HINSPECT" || strings.HasPrefix(upperRequest, "HINSPECT ")
}

func isWatchCommand(request string) bool {
	upperRequest := strings.ToUpper(strings.TrimSpace(request))
	return strings.HasPrefix(upperRequest, "WATCH ")
}

func isStreamCommand(request string) bool {
	upperRequest := strings.ToUpper(strings.TrimSpace(request))
	return upperRequest == "STREAM" ||
		strings.HasPrefix(upperRequest, "PSTREAM ") ||
		upperRequest == "QSTREAM" ||
		strings.HasPrefix(upperRequest, "QPSTREAM ")
}
