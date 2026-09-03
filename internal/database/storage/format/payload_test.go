package format

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodePayloadRoundTrip(t *testing.T) {
	t.Parallel()

	source := bytes.Repeat([]byte("compressible-"), 500)

	for _, id := range []CodecID{CodecS2, CodecZstd} {
		t.Run(id.String(), func(t *testing.T) {
			t.Parallel()

			payload := EncodePayload(nil, source, Compression{Codec: id, MinFrameSize: 0})
			require.Equal(t, id, PayloadCodec(payload))
			require.Less(t, len(payload), len(source))

			restored, err := DecodePayload(nil, payload, PayloadVersionCompressed, 1<<20)
			require.NoError(t, err)
			require.Equal(t, source, restored)
		})
	}
}

func TestEncodePayloadKeepsRawBelowMinFrameSize(t *testing.T) {
	t.Parallel()

	source := bytes.Repeat([]byte("a"), 100)

	payload := EncodePayload(nil, source, Compression{Codec: CodecZstd, MinFrameSize: 512})
	require.Equal(t, CodecNone, PayloadCodec(payload))

	restored, err := DecodePayload(nil, payload, PayloadVersionCompressed, 1<<20)
	require.NoError(t, err)
	require.Equal(t, source, restored)
}

func TestEncodePayloadKeepsRawWhenCompressionDoesNotHelp(t *testing.T) {
	t.Parallel()

	source := make([]byte, 4096)
	for i := range source {
		source[i] = byte(i * 7919 % 251)
	}

	payload := EncodePayload(nil, source, Compression{Codec: CodecZstd, MinFrameSize: 0})

	restored, err := DecodePayload(nil, payload, PayloadVersionCompressed, 1<<20)
	require.NoError(t, err)
	require.Equal(t, source, restored)
	require.LessOrEqual(t, len(payload), len(source)+1)
}

func TestEncodePayloadWithoutCodecKeepsRaw(t *testing.T) {
	t.Parallel()

	source := bytes.Repeat([]byte("compressible-"), 500)

	payload := EncodePayload(nil, source, Compression{Codec: CodecNone})
	require.Equal(t, CodecNone, PayloadCodec(payload))
	require.Len(t, payload, len(source)+1)
}

func TestEncodePayloadHandlesEmptyAndTinyInput(t *testing.T) {
	t.Parallel()

	for _, source := range [][]byte{{}, {0x01}} {
		payload := EncodePayload(nil, source, Compression{Codec: CodecS2, MinFrameSize: 0})

		restored, err := DecodePayload(nil, payload, PayloadVersionCompressed, 1<<20)
		require.NoError(t, err)
		require.Equal(t, source, restored)
	}
}

func TestDecodePayloadVersionOneReturnsRaw(t *testing.T) {
	t.Parallel()

	source := []byte("legacy payload without codec prefix")

	restored, err := DecodePayload(nil, source, PayloadVersionRaw, 1<<20)
	require.NoError(t, err)
	require.Equal(t, source, restored)
}

func TestDecodePayloadRejectsOversizedDeclaredLength(t *testing.T) {
	t.Parallel()

	var head [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(head[:], 1<<40)

	payload := append([]byte{byte(CodecZstd)}, head[:n]...)
	payload = append(payload, 0xff, 0xff)

	_, err := DecodePayload(nil, payload, PayloadVersionCompressed, 1<<20)
	require.ErrorIs(t, err, ErrPayloadTooLarge)
}

func TestDecodePayloadRejectsUnknownCodec(t *testing.T) {
	t.Parallel()

	payload := []byte{200, 0x01, 0x02}

	_, err := DecodePayload(nil, payload, PayloadVersionCompressed, 1<<20)
	require.ErrorIs(t, err, ErrUnknownCodec)
}

func TestDecodePayloadRejectsMissingPrefix(t *testing.T) {
	t.Parallel()

	_, err := DecodePayload(nil, nil, PayloadVersionCompressed, 1<<20)
	require.ErrorIs(t, err, ErrIncompleteFrame)
}

func TestParseHeaderVersionsAcceptsRange(t *testing.T) {
	t.Parallel()

	data := AppendHeader(nil, MagicWAL, 2)
	data = append(data, []byte("frames")...)

	rest, version, err := ParseHeaderVersions(data, MagicWAL, 1, 2)
	require.NoError(t, err)
	require.Equal(t, uint16(2), version)
	require.Equal(t, []byte("frames"), rest)
}

func TestParseHeaderVersionsRejectsOutOfRange(t *testing.T) {
	t.Parallel()

	data := AppendHeader(nil, MagicWAL, 3)

	_, _, err := ParseHeaderVersions(data, MagicWAL, 1, 2)
	require.ErrorIs(t, err, ErrUnsupportedVersion)
}

func TestParseHeaderVersionsRejectsBadMagic(t *testing.T) {
	t.Parallel()

	data := AppendHeader(nil, MagicDump, 1)

	_, _, err := ParseHeaderVersions(data, MagicWAL, 1, 2)
	require.ErrorIs(t, err, ErrBadMagic)
}
