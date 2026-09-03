package format

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCodec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  CodecID
	}{
		{name: "empty", input: "", want: CodecNone},
		{name: "none", input: "none", want: CodecNone},
		{name: "s2", input: "s2", want: CodecS2},
		{name: "zstd", input: "zstd", want: CodecZstd},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseCodec(test.input)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestParseCodecRejectsUnknownName(t *testing.T) {
	t.Parallel()

	_, err := ParseCodec("lz4")
	require.ErrorIs(t, err, ErrUnknownCodec)
}

func TestCodecIDString(t *testing.T) {
	t.Parallel()

	require.Equal(t, "none", CodecNone.String())
	require.Equal(t, "s2", CodecS2.String())
	require.Equal(t, "zstd", CodecZstd.String())
	require.Equal(t, "unknown", CodecID(200).String())
}

func TestCodecRoundTrip(t *testing.T) {
	t.Parallel()

	source := bytes.Repeat([]byte("fq-compression-payload-"), 200)

	for _, id := range []CodecID{CodecS2, CodecZstd} {
		t.Run(id.String(), func(t *testing.T) {
			t.Parallel()

			impl, ok := codecs[id]
			require.True(t, ok)

			compressed := impl.compress(nil, source)
			require.Less(t, len(compressed), len(source))

			restored, err := impl.decompress(nil, compressed)
			require.NoError(t, err)
			require.Equal(t, source, restored)
		})
	}
}

func TestSupportedCodecs(t *testing.T) {
	t.Parallel()

	require.Equal(t, []uint8{uint8(CodecS2), uint8(CodecZstd)}, SupportedCodecs())
}

func TestCompressionEnabled(t *testing.T) {
	t.Parallel()

	require.False(t, Compression{}.Enabled())
	require.False(t, Compression{Codec: CodecNone}.Enabled())
	require.True(t, Compression{Codec: CodecS2}.Enabled())
}
