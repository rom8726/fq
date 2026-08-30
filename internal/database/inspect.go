package database

import (
	"context"
	"errors"

	"github.com/fq-db/fq/internal/database/compute"
)

const (
	maxInspectReportSize    = 1 << 20
	chunkContinuationPrefix = "nxt|"
)

var (
	errInspectUnavailable    = errors.New("inspect is not available")
	errInspectReportTooLarge = errors.New("inspect report too large")
	errMessageSizeTooSmall   = errors.New("max message size too small for a chunked response")
)

func (d *Database) handleInspectQuery(ctx context.Context, query compute.Query, write func([]byte) error) error {
	if d.inspector == nil {
		return write(makeErrorMsg(errInspectUnavailable))
	}

	payload, err := d.inspector.Report(ctx, query.Arg(0))
	if err != nil {
		return write(makeErrorMsg(err))
	}

	return d.writeChunked(payload, write)
}

func (d *Database) writeChunked(payload []byte, write func([]byte) error) error {
	if len(payload) > maxInspectReportSize {
		return write(makeErrorMsg(errInspectReportTooLarge))
	}

	budget := d.maxMessageSize - len(chunkContinuationPrefix)
	if budget <= 0 {
		return write(makeErrorMsg(errMessageSizeTooSmall))
	}

	for len(payload) > budget {
		chunk := payload[:budget]
		payload = payload[budget:]

		buf := make([]byte, 0, len(chunkContinuationPrefix)+len(chunk))
		buf = append(buf, chunkContinuationPrefix...)
		buf = append(buf, chunk...)

		if err := write(buf); err != nil {
			return err
		}
	}

	buf := make([]byte, 0, len("ok|")+len(payload))
	buf = append(buf, "ok|"...)
	buf = append(buf, payload...)

	return write(buf)
}
