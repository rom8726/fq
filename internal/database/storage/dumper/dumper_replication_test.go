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
	snapshot, err := engine.Snapshot(context.Background(), 1)
	require.NoError(t, err)
	require.NoError(t, d.Dump(context.Background(), 1, snapshot))

	sessionUUID := "session1"
	batch, ok, err := d.GetNextData(sessionUUID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, batch, 2)
	require.Equal(t, database.DumpElemKindCheckpoint, batch[0].Kind)
	require.Equal(t, database.Tx(1), batch[0].Tx)
	require.Equal(t, "key1", batch[1].Key)

	batch, ok, err = d.GetNextData(sessionUUID)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 0, len(batch))
}

func TestGetNextRawBatchPassesThroughMatchingCodec(t *testing.T) {
	dir := t.TempDir()
	compression := format.Compression{Codec: format.CodecZstd, MinFrameSize: 0}
	d := New(emptyDumpEngine{}, nil, dir, compression)
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(5), compressibleSnapshot()))

	codec, data, ok, err := d.GetNextRawBatch("session-1", format.CodecZstd)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, format.CodecZstd, codec)
	require.Equal(t, format.CodecZstd, format.PayloadCodec(data))
}

func TestGetNextRawBatchRecodesToRequestedCodec(t *testing.T) {
	dir := t.TempDir()
	compression := format.Compression{Codec: format.CodecZstd, MinFrameSize: 0}
	d := New(emptyDumpEngine{}, nil, dir, compression)
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(5), compressibleSnapshot()))

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

	require.NoError(t, d.Dump(context.Background(), database.Tx(5), singleElemSnapshot()))

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

	require.NoError(t, d.Dump(context.Background(), database.Tx(5), singleElemSnapshot()))

	_, _, ok, err := d.GetNextRawBatch("session-4", format.CodecS2)
	require.NoError(t, err)
	require.True(t, ok)

	_, _, ok, err = d.GetNextRawBatch("session-4", format.CodecS2)
	require.NoError(t, err)
	require.False(t, ok)
}

func compressibleSnapshot() database.DumpSnapshot {
	elems := make([]database.DumpElem, 0, 512)
	for i := range 512 {
		elems = append(elems, database.DumpElem{Tx: database.Tx(i + 1), Key: "repeating-key-prefix"})
	}

	return database.DumpSnapshot{elems}
}
