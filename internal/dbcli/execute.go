package dbcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/logrusorgru/aurora/v4"
	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/inspect"
	"github.com/fq-db/fq/internal/network"
	"github.com/fq-db/fq/internal/protocol"
)

const (
	defaultTimeout = time.Minute
	watchTimeout   = 30 * time.Second
	streamTimeout  = time.Hour
)

func TimeoutFor(request string) time.Duration {
	switch {
	case IsWatchCommand(request):
		return watchTimeout
	case IsStreamCommand(request):
		return streamTimeout
	default:
		return defaultTimeout
	}
}

func Execute(
	ctx context.Context,
	logger *zerolog.Logger,
	client *network.TCPClient,
	request string,
	out io.Writer,
	start time.Time,
) error {
	if IsStreamCommand(request) {
		_, _ = fmt.Fprintln(out, "Streaming events... (press Ctrl+C to cancel)")
	} else if IsWatchCommand(request) {
		_, _ = fmt.Fprintln(out, "Watching for changes... (press Ctrl+C to cancel)")
	}

	reqCtx, cancel := context.WithDeadline(ctx, start.Add(TimeoutFor(request)))
	defer cancel()

	switch {
	case IsStreamCommand(request):
		return executeStream(reqCtx, client, request, out, start)
	case IsHumanInspectCommand(request):
		return executeHumanInspect(reqCtx, logger, client, request, out, start)
	case IsInspectCommand(request):
		return executeInspect(reqCtx, logger, client, request, out, start)
	default:
		return executePlain(reqCtx, client, request, out, start)
	}
}

func executeStream(
	ctx context.Context,
	client *network.TCPClient,
	request string,
	out io.Writer,
	start time.Time,
) error {
	err := client.Stream(ctx, []byte(request), func(_ protocol.Kind, response []byte) error {
		_, _ = fmt.Fprintf(
			out, "%s\t\t\t\tElapsed: %s\n", aurora.Green("[fq]> "+string(response)), time.Since(start).String(),
		)

		return nil
	})
	if err != nil {
		if errors.Is(err, network.ErrIdleTimeout) {
			_, _ = fmt.Fprintln(out, "Stream idle timeout")

			return nil
		}

		if errors.Is(err, context.DeadlineExceeded) {
			_, _ = fmt.Fprintln(out, "Stream timeout")

			return nil
		}

		return fmt.Errorf("stream query: %w", err)
	}

	return nil
}

func executePlain(
	ctx context.Context,
	client *network.TCPClient,
	request string,
	out io.Writer,
	start time.Time,
) error {
	response, err := client.Send(ctx, []byte(request))
	elapsed := time.Since(start)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && IsWatchCommand(request) {
			_, _ = fmt.Fprintln(out, "Watch timeout: no changes detected")

			return nil
		}

		var protoErr *protocol.Error
		if errors.As(err, &protoErr) {
			_, _ = fmt.Fprintf(out, "%s\t\t\t\tElapsed: %s\n", renderProtocolError(protoErr), elapsed.String())

			return nil
		}

		return fmt.Errorf("send query: %w", err)
	}

	_, _ = fmt.Fprintf(out, "%s\t\t\t\tElapsed: %s\n", aurora.Green("[fq]> "+string(response)), elapsed.String())

	return nil
}

func executeInspect(
	ctx context.Context,
	logger *zerolog.Logger,
	client *network.TCPClient,
	request string,
	out io.Writer,
	start time.Time,
) error {
	body, err := fetchChunkedBody(ctx, client, request)
	elapsed := time.Since(start)
	if err != nil {
		_, _ = fmt.Fprintf(out, "%s\t\t\t\tElapsed: %s\n", aurora.Red("[fq]> "+err.Error()), elapsed.String())

		return nil
	}

	var pretty bytes.Buffer
	if jsonErr := json.Indent(&pretty, body, "", "  "); jsonErr != nil {
		logger.Warn().Err(jsonErr).Msg("failed to pretty-print inspect report")
		_, _ = fmt.Fprintf(out, "%s\t\t\t\tElapsed: %s\n", aurora.Green("[fq]> "+string(body)), elapsed.String())

		return nil
	}

	_, _ = fmt.Fprintf(out, "%s\nElapsed: %s\n", aurora.Green(pretty.String()), elapsed.String())

	return nil
}

func executeHumanInspect(
	ctx context.Context,
	logger *zerolog.Logger,
	client *network.TCPClient,
	request string,
	out io.Writer,
	start time.Time,
) error {
	wireQuery := toWireInspectQuery(request)

	body, err := fetchChunkedBody(ctx, client, wireQuery)
	elapsed := time.Since(start)
	if err != nil {
		_, _ = fmt.Fprintf(out, "%s\nElapsed: %s\n", aurora.Red("error: "+err.Error()), elapsed.String())

		return nil
	}

	var report inspect.Report
	if jsonErr := json.Unmarshal(body, &report); jsonErr != nil {
		logger.Warn().Err(jsonErr).Msg("failed to parse inspect report")
		msg := aurora.Red("error: failed to parse inspect report: " + jsonErr.Error())
		_, _ = fmt.Fprintf(out, "%s\nElapsed: %s\n", msg, elapsed.String())

		return nil
	}

	renderReport(out, &report)
	_, _ = fmt.Fprintf(out, "\nElapsed: %s\n", elapsed.String())

	return nil
}

func fetchChunkedBody(ctx context.Context, client *network.TCPClient, query string) ([]byte, error) {
	var body bytes.Buffer
	err := client.Stream(ctx, []byte(query), func(kind protocol.Kind, frame []byte) error {
		body.Write(frame)
		if kind == protocol.KindOK {
			return io.EOF
		}

		return nil
	})
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return body.Bytes(), nil
}

func renderProtocolError(err *protocol.Error) aurora.Value {
	return aurora.Red(fmt.Sprintf("[fq]> [%d] %s", err.Code, err.Msg))
}
