package format

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

const testVersion = 1

func TestFrameRoundTrip(t *testing.T) {
	t.Parallel()

	data := AppendFrame(nil, []byte("payload"))

	payload, rest, err := NextFrame(data, 1024)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), payload)
	require.Empty(t, rest)
}

func TestNextFrameReturnsRestOfStream(t *testing.T) {
	t.Parallel()

	data := AppendFrame(AppendFrame(nil, []byte("first")), []byte("second"))

	payload, rest, err := NextFrame(data, 1024)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), payload)

	payload, rest, err = NextFrame(rest, 1024)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), payload)
	require.Empty(t, rest)
}

func TestNextFrameDetectsPayloadCorruption(t *testing.T) {
	t.Parallel()

	data := AppendFrame(nil, []byte("payload"))
	data[FrameHeaderSize] ^= 0xff

	_, _, err := NextFrame(data, 1024)
	require.ErrorIs(t, err, ErrChecksumMismatch)
}

func TestNextFrameDetectsLengthCorruption(t *testing.T) {
	t.Parallel()

	data := AppendFrame(nil, []byte("payload"))
	data = append(data, make([]byte, 32)...)
	binary.BigEndian.PutUint32(data[:4], 8)

	_, _, err := NextFrame(data, 1024)
	require.ErrorIs(t, err, ErrChecksumMismatch)
}

func TestNextFrameRejectsIncompleteFrame(t *testing.T) {
	t.Parallel()

	data := AppendFrame(nil, []byte("payload"))

	_, _, err := NextFrame(data[:len(data)-1], 1024)
	require.ErrorIs(t, err, ErrIncompleteFrame)

	_, _, err = NextFrame(data[:FrameHeaderSize-1], 1024)
	require.ErrorIs(t, err, ErrIncompleteFrame)
}

func TestNextFrameRejectsTooLargeFrame(t *testing.T) {
	t.Parallel()

	data := AppendFrame(nil, []byte("payload"))

	_, _, err := NextFrame(data, 2)
	require.ErrorIs(t, err, ErrFrameTooLarge)
}

func TestCheckPayloadSizeRejectsOversizedPayload(t *testing.T) {
	t.Parallel()

	require.NoError(t, CheckPayloadSize([]byte("payload"), 7))
	require.NoError(t, CheckPayloadSize([]byte("payload"), 0))
	require.ErrorIs(t, CheckPayloadSize([]byte("payload"), 6), ErrFrameTooLarge)
}

func TestParseHeaderRoundTrip(t *testing.T) {
	t.Parallel()

	data := AppendFrame(AppendHeader(nil, MagicWAL, testVersion), []byte("payload"))

	rest, err := ParseHeader(data, MagicWAL, testVersion)
	require.NoError(t, err)

	payload, _, err := NextFrame(rest, 1024)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), payload)
}

func TestParseHeaderRejectsForeignMagic(t *testing.T) {
	t.Parallel()

	data := AppendHeader(nil, MagicDump, testVersion)

	_, err := ParseHeader(data, MagicWAL, testVersion)
	require.ErrorIs(t, err, ErrBadMagic)
	require.Contains(t, err.Error(), "FQDP")
}

func TestParseHeaderRejectsUnknownVersion(t *testing.T) {
	t.Parallel()

	data := AppendHeader(nil, MagicWAL, 7)

	_, err := ParseHeader(data, MagicWAL, testVersion)
	require.ErrorIs(t, err, ErrUnsupportedVersion)
}

func TestParseHeaderRejectsShortData(t *testing.T) {
	t.Parallel()

	_, err := ParseHeader([]byte{'F', 'Q'}, MagicWAL, testVersion)
	require.ErrorIs(t, err, ErrIncompleteFrame)
}

func TestParseHeaderIgnoresReservedBytes(t *testing.T) {
	t.Parallel()

	data := AppendHeader(nil, MagicWAL, testVersion)
	data[6] = 0xab
	data[7] = 0xcd

	_, err := ParseHeader(data, MagicWAL, testVersion)
	require.NoError(t, err)
}

func TestCompleteFramesSizeStopsAtFrameBoundary(t *testing.T) {
	t.Parallel()

	first := AppendFrame(nil, []byte("first"))
	second := AppendFrame(nil, []byte("second"))
	data := append(append([]byte(nil), first...), second[:len(second)-1]...)

	require.Equal(t, len(first), CompleteFramesSize(data, false, 1024))
}

func TestCompleteFramesSizeAccountsForFileHeader(t *testing.T) {
	t.Parallel()

	frame := AppendFrame(nil, []byte("first"))
	data := append(AppendHeader(nil, MagicWAL, testVersion), frame...)

	require.Equal(t, HeaderSize+len(frame), CompleteFramesSize(data, true, 1024))
	require.Zero(t, CompleteFramesSize(data[:HeaderSize], true, 1024))
}

func TestFirstFrameSizeIncludesFileHeader(t *testing.T) {
	t.Parallel()

	frame := AppendFrame(nil, []byte("first"))
	data := append(AppendHeader(nil, MagicWAL, testVersion), frame...)

	require.Equal(t, HeaderSize+len(frame), FirstFrameSize(data[:HeaderSize+FrameHeaderSize], true, 1024))
	require.Zero(t, FirstFrameSize(data[:HeaderSize+FrameHeaderSize-1], true, 1024))
}

func TestMagicValuesExceedMaxFrameSize(t *testing.T) {
	t.Parallel()

	const maxFrameSize = 100 * 1024 * 1024

	for _, magic := range []Magic{MagicWAL, MagicDump, MagicMeta} {
		require.Greater(t, binary.BigEndian.Uint32(magic[:]), uint32(maxFrameSize), magic.String())
	}
}

func TestPutFrameHeaderMatchesFrameHeader(t *testing.T) {
	t.Parallel()

	payload := []byte("dump batch payload")

	buffer := make([]byte, FrameHeaderSize+len(payload))
	copy(buffer[FrameHeaderSize:], payload)
	PutFrameHeader(buffer, buffer[FrameHeaderSize:])

	require.Equal(t, AppendFrame(nil, payload), buffer)
}

func TestPutFrameHeaderProducesReadableFrame(t *testing.T) {
	t.Parallel()

	payload := []byte("another payload")

	buffer := make([]byte, FrameHeaderSize+len(payload))
	copy(buffer[FrameHeaderSize:], payload)
	PutFrameHeader(buffer, buffer[FrameHeaderSize:])

	got, rest, err := NextFrame(buffer, 1024)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.Empty(t, rest)
}
