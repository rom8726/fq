package database

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func adminContext(t *testing.T, db *Database) context.Context {
	t.Helper()

	ctx, _ := authContext(t)
	require.Equal(t, "ok|1", db.HandleQuery(ctx, "AUTH admin-token-value"))

	return ctx
}

func TestHandleGetDelMDelWatchQueries(t *testing.T) {
	db := newTestDatabase(t)
	ctx := adminContext(t, db)

	require.Equal(t, "ok|1", db.HandleQuery(ctx, "GET key 60"))
	require.Equal(t, "ok|1", db.HandleQuery(ctx, "DEL key 60"))
	require.Equal(t, "ok|1", db.HandleQuery(ctx, "MDEL key 60 key2 60"))
	require.Contains(t, db.HandleQuery(ctx, "MDEL key 60 key2"), "err|")
	require.Equal(t, "ok|1", db.HandleQuery(ctx, "WATCH key 60"))
}

func TestHandleQuotaQueries(t *testing.T) {
	db := newTestDatabase(t)
	ctx := adminContext(t, db)

	require.Equal(t, "ok|1", db.HandleQuery(ctx, "QUOTA SET name 10"))
	require.Equal(t, "ok|1", db.HandleQuery(ctx, "QUOTA SETN name 10 5"))
	require.Equal(t, "ok|1;0;0;0;0", db.HandleQuery(ctx, "QUOTA ACQ name 1 client"))
	require.Equal(t, "ok|1;0;0;0;0", db.HandleQuery(ctx, "QUOTA ACQN name client 1"))
	require.Equal(t, "ok|1;0;0;0;0", db.HandleQuery(ctx, "QUOTA ACQL name 10 1 client 1"))
	require.Equal(t, "ok|1", db.HandleQuery(ctx, "QUOTA REL name client"))
	require.Equal(t, "ok|1", db.HandleQuery(ctx, "QUOTA DEL name"))
	require.Contains(t, db.HandleQuery(ctx, "QUOTA WHATEVER name"), "err|")
}

func TestHandlePScanQuery(t *testing.T) {
	db := newTestDatabase(t)
	ctx := adminContext(t, db)

	require.Contains(t, db.HandleQuery(ctx, "PSCAN prefix 0 10"), "ok|")
	require.Contains(t, db.HandleQuery(ctx, "PSCAN  0 10"), "err|")
}

func TestHandleStreamQueriesStopOnContextCancel(t *testing.T) {
	db := newTestDatabase(t)
	ctx := adminContext(t, db)

	for _, query := range []string{"STREAM", "PSTREAM prefix", "QSTREAM", "QPSTREAM prefix"} {
		streamCtx, cancel := context.WithCancel(ctx)
		cancel()

		err := db.HandleQueryStream(streamCtx, query, func([]byte) error { return nil })
		require.ErrorIs(t, err, context.Canceled, query)
	}
}

func TestHandlePStreamQueryRejectsEmptyPrefix(t *testing.T) {
	db := newTestDatabase(t)
	ctx := adminContext(t, db)

	var response []byte
	err := db.HandleQueryStream(ctx, "PSTREAM", func(msg []byte) error {
		response = append(response[:0], msg...)

		return nil
	})
	require.NoError(t, err)
	require.Contains(t, string(response), "err|")
}

type stubInspector struct {
	payload []byte
	err     error
}

func (s stubInspector) Report(context.Context, string) ([]byte, error) {
	return s.payload, s.err
}

func TestHandleInspectQueryUnavailableByDefault(t *testing.T) {
	db := newTestDatabase(t)
	ctx := adminContext(t, db)

	require.Contains(t, db.HandleQuery(ctx, "INSPECT section"), "not available")
}

func TestHandleInspectQueryReportsError(t *testing.T) {
	db := newTestDatabase(t)
	ctx := adminContext(t, db)
	db.SetInspector(stubInspector{err: errors.New("boom")})

	require.Contains(t, db.HandleQuery(ctx, "INSPECT section"), "err|")
}

func TestWriteChunkedSplitsPayloadAcrossMessageSizeBudget(t *testing.T) {
	db := newTestDatabase(t)
	ctx := adminContext(t, db)
	db.SetInspector(stubInspector{payload: []byte(strings.Repeat("a", 20))})
	db.maxMessageSize = 9

	var chunks [][]byte
	err := db.HandleQueryStream(ctx, "INSPECT x", func(msg []byte) error {
		chunks = append(chunks, append([]byte{}, msg...))

		return nil
	})
	require.NoError(t, err)
	require.Greater(t, len(chunks), 1)
	require.True(t, strings.HasPrefix(string(chunks[len(chunks)-1]), "ok|"))
}

func TestWriteChunkedRejectsOversizedReport(t *testing.T) {
	db := newTestDatabase(t)
	ctx := adminContext(t, db)
	db.SetInspector(stubInspector{payload: make([]byte, maxInspectReportSize+1)})

	require.Contains(t, db.HandleQuery(ctx, "INSPECT section"), "err|")
}

func TestWriteChunkedRejectsTooSmallMessageSize(t *testing.T) {
	db := newTestDatabase(t)
	db.maxMessageSize = 1

	var response []byte
	err := db.writeChunked([]byte("payload"), func(msg []byte) error {
		response = append(response[:0], msg...)

		return nil
	})
	require.NoError(t, err)
	require.Contains(t, string(response), "err|")
}

func TestMakeTTLBoundaries(t *testing.T) {
	_, err := makeTTL("not-a-number")
	require.Error(t, err)

	_, err = makeTTL("0")
	require.Error(t, err)

	value, err := makeTTL("5")
	require.NoError(t, err)
	require.Equal(t, uint32(5), value)
}
