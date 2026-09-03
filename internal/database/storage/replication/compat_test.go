package replication

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/storage/format"
)

type legacyRequest struct {
	AuthToken       string
	ProtocolVersion uint32
	DumpRequest
	WALRequest
}

type legacyWALResponse struct {
	Succeed           bool
	ErrorCode         uint16
	SegmentName       string
	SegmentOffset     int64
	NextSegmentOffset int64
	SegmentData       []byte
}

func TestLegacyMasterIgnoresUnknownRequestFields(t *testing.T) {
	t.Parallel()

	request := NewWALRequest("token", "replica-1", "wal_1.log", 64, 7, format.SupportedCodecs())

	data, err := Encode(&request)
	require.NoError(t, err)

	var legacy legacyRequest
	require.NoError(t, gob.NewDecoder(bytes.NewReader(data)).Decode(&legacy))
	require.Equal(t, "token", legacy.AuthToken)
	require.Equal(t, "replica-1", legacy.ReplicaID)
	require.Equal(t, int64(64), legacy.SegmentOffset)
}

func TestLegacySlaveIgnoresUnknownResponseFields(t *testing.T) {
	t.Parallel()

	response := WALResponse{
		Succeed:              true,
		SegmentName:          "wal_1.log",
		SegmentData:          []byte{1, 2, 3},
		SegmentFormatVersion: 2,
		SegmentCodec:         uint8(format.CodecS2),
	}

	data, err := Encode(&response)
	require.NoError(t, err)

	var legacy legacyWALResponse
	require.NoError(t, gob.NewDecoder(bytes.NewReader(data)).Decode(&legacy))
	require.True(t, legacy.Succeed)
	require.Equal(t, []byte{1, 2, 3}, legacy.SegmentData)
}

func TestNewFieldsAreZeroWhenLegacyMasterResponds(t *testing.T) {
	t.Parallel()

	legacy := legacyWALResponse{Succeed: true, SegmentName: "wal_1.log", SegmentData: []byte{9}}

	var buffer bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buffer).Encode(&legacy))

	var response WALResponse
	require.NoError(t, Decode(&response, buffer.Bytes()))
	require.Equal(t, uint16(0), response.SegmentFormatVersion)
	require.Equal(t, uint8(0), response.SegmentCodec)
	require.Equal(t, []byte{9}, response.SegmentData)
}

func TestSupportsCodec(t *testing.T) {
	t.Parallel()

	require.True(t, SupportsCodec(format.SupportedCodecs(), format.CodecS2))
	require.True(t, SupportsCodec(format.SupportedCodecs(), format.CodecZstd))
	require.False(t, SupportsCodec(nil, format.CodecS2))
	require.False(t, SupportsCodec([]uint8{uint8(format.CodecS2)}, format.CodecZstd))
	require.True(t, SupportsCodec(nil, format.CodecNone))
}
