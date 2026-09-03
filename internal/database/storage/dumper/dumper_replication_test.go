package dumper

import (
	"bytes"
	"context"
	"encoding/gob"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database"
	inMemory "github.com/fq-db/fq/internal/database/storage/engine/in-memory"
	"github.com/fq-db/fq/internal/database/storage/format"
)

func TestDumper_GetNextData(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := inMemory.NewEngine(inMemory.HashTableBuilder, 1, &logger, nil, nil)
	require.NoError(t, err)

	now := time.Now().Unix()
	engine.Incr(
		database.TxContext{
			Tx:       0,
			DumpTx:   0,
			CurrTime: database.TxTime(now),
			FromWAL:  false,
		},
		database.BatchKey{
			BatchSize:    60,
			BatchSizeStr: "60",
			Key:          "key1",
		},
		nil,
	)

	d := New(engine, nil, "/tmp", format.Compression{})
	err = d.Dump(context.Background(), 1)
	require.NoError(t, err)

	sessionUUID := "session1"
	batch, ok, err := d.GetNextData(sessionUUID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, len(batch))

	batch, ok, err = d.GetNextData(sessionUUID)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 0, len(batch))
}

func TestGetNextRawBatchPassesThroughMatchingCodec(t *testing.T) {
	dir := t.TempDir()
	compression := format.Compression{Codec: format.CodecZstd, MinFrameSize: 0}
	d := New(compressibleDumpEngine{}, nil, dir, compression)
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(5)))

	codec, data, ok, err := d.GetNextRawBatch("session-1", format.CodecZstd)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, format.CodecZstd, codec)
	require.Equal(t, format.CodecZstd, format.PayloadCodec(data))
}

func TestGetNextRawBatchRecodesToRequestedCodec(t *testing.T) {
	dir := t.TempDir()
	compression := format.Compression{Codec: format.CodecZstd, MinFrameSize: 0}
	d := New(compressibleDumpEngine{}, nil, dir, compression)
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(5)))

	codec, data, ok, err := d.GetNextRawBatch("session-2", format.CodecS2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, format.CodecS2, codec)

	decoded, err := format.DecodePayload(nil, data, format.PayloadVersionCompressed, dumpMaxFrameSize)
	require.NoError(t, err)

	var batch []database.DumpElem
	require.NoError(t, gob.NewDecoder(bytes.NewReader(decoded)).Decode(&batch))
	require.NotEmpty(t, batch)
}

func TestGetNextRawBatchOnUncompressedDump(t *testing.T) {
	dir := t.TempDir()
	d := New(&recordingRestoreEngine{}, nil, dir, format.Compression{})
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(5)))

	_, data, ok, err := d.GetNextRawBatch("session-3", format.CodecS2)
	require.NoError(t, err)
	require.True(t, ok)

	decoded, err := format.DecodePayload(nil, data, format.PayloadVersionCompressed, dumpMaxFrameSize)
	require.NoError(t, err)

	var batch []database.DumpElem
	require.NoError(t, gob.NewDecoder(bytes.NewReader(decoded)).Decode(&batch))
	require.NotEmpty(t, batch)
}

func TestGetNextRawBatchReturnsFalseAtEndOfDump(t *testing.T) {
	dir := t.TempDir()
	d := New(&recordingRestoreEngine{}, nil, dir, format.Compression{})
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(5)))

	_, _, ok, err := d.GetNextRawBatch("session-4", format.CodecS2)
	require.NoError(t, err)
	require.True(t, ok)

	_, _, ok, err = d.GetNextRawBatch("session-4", format.CodecS2)
	require.NoError(t, err)
	require.False(t, ok)
}

type compressibleDumpEngine struct{}

func (compressibleDumpEngine) Dump(context.Context, database.Tx) (<-chan database.DumpElem, <-chan error) {
	elems := make(chan database.DumpElem, 512)
	errs := make(chan error, 1)

	for i := range 512 {
		elems <- database.DumpElem{Tx: database.Tx(i + 1), Key: "repeating-key-prefix"}
	}
	close(elems)
	errs <- nil
	close(errs)

	return elems, errs
}

func (compressibleDumpEngine) RestoreDumpElem(context.Context, database.DumpElem) error {
	return nil
}
