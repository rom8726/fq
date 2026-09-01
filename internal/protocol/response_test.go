package protocol_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/protocol"
)

func TestParseResponseOK(t *testing.T) {
	kind, body, err := protocol.ParseResponse([]byte("ok|1;2;3"))
	require.NoError(t, err)
	require.Equal(t, protocol.KindOK, kind)
	require.Equal(t, "1;2;3", string(body))
}

func TestParseResponseNext(t *testing.T) {
	kind, body, err := protocol.ParseResponse([]byte(`nxt|{"a":`))
	require.NoError(t, err)
	require.Equal(t, protocol.KindNext, kind)
	require.Equal(t, `{"a":`, string(body))
}

func TestParseResponseErrorCarriesCode(t *testing.T) {
	kind, _, err := protocol.ParseResponse([]byte("err|4000|quota not found"))
	require.Equal(t, protocol.KindError, kind)

	var protoErr *protocol.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, protocol.CodeQuotaNotFound, protoErr.Code)
	require.Equal(t, "quota not found", protoErr.Msg)
}

func TestParseResponseErrorWithPipeInMessage(t *testing.T) {
	_, _, err := protocol.ParseResponse([]byte("err|1000|invalid symbol: |"))

	var protoErr *protocol.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, "invalid symbol: |", protoErr.Msg)
}

func TestParseResponseMalformed(t *testing.T) {
	cases := [][]byte{
		nil,
		[]byte(""),
		[]byte("ok"),
		[]byte("wat|1"),
		[]byte("err|abc|bad code"),
		[]byte("err|4000"),
	}

	for _, frame := range cases {
		_, _, err := protocol.ParseResponse(frame)
		require.ErrorIs(t, err, protocol.ErrMalformedResponse, string(frame))
	}
}

func TestParseServerInfo(t *testing.T) {
	info, err := protocol.ParseServerInfo([]byte("1;65536;1;admin"))
	require.NoError(t, err)
	require.Equal(t, uint16(1), info.Version)
	require.Equal(t, 65536, info.MaxMessageSize)
	require.True(t, info.AuthRequired)
	require.Equal(t, "admin", info.Role)
}

func TestParseServerInfoRejectsShortBody(t *testing.T) {
	_, err := protocol.ParseServerInfo([]byte("1;65536"))
	require.ErrorIs(t, err, protocol.ErrMalformedResponse)
}
