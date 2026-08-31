package compute_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/compute"
)

func TestAuthCommandNameMapping(t *testing.T) {
	require.Equal(t, compute.AuthCommandID, compute.CommandNameToCommandID("AUTH"))
}

func TestParseAndAnalyzeAuthAcceptsOpaqueToken(t *testing.T) {
	logger := zerolog.Nop()
	parser := compute.NewParser(&logger)

	tokens := []string{
		"plain-token-value",
		"YWRtaW4tdG9rZW4tdmFsdWU=",
		"a+b/c==",
		"tok.en:with!punctuation",
	}

	for _, token := range tokens {
		query, err := parser.ParseAndAnalyzeQuery(context.Background(), "AUTH "+token)
		require.NoError(t, err, token)
		require.Equal(t, compute.AuthCommandID, query.CommandID())
		require.Equal(t, []string{token}, query.Arguments())
	}
}

func TestParseAndAnalyzeAuthIsCaseInsensitive(t *testing.T) {
	logger := zerolog.Nop()
	parser := compute.NewParser(&logger)

	query, err := parser.ParseAndAnalyzeQuery(context.Background(), "auth some-token-value")
	require.NoError(t, err)
	require.Equal(t, compute.AuthCommandID, query.CommandID())
}

func TestParseAndAnalyzeAuthRejectsWrongArgumentCount(t *testing.T) {
	logger := zerolog.Nop()
	parser := compute.NewParser(&logger)

	_, err := parser.ParseAndAnalyzeQuery(context.Background(), "AUTH")
	require.Error(t, err)

	_, err = parser.ParseAndAnalyzeQuery(context.Background(), "AUTH one two")
	require.Error(t, err)
}

func TestAnalyzerAcceptsAuthTokens(t *testing.T) {
	logger := zerolog.Nop()
	analyzer := compute.NewAnalyzer(&logger)

	query, err := analyzer.AnalyzeQuery(context.Background(), []string{"AUTH", "some-token-value"})
	require.NoError(t, err)
	require.Equal(t, compute.AuthCommandID, query.CommandID())
	require.Equal(t, []string{"some-token-value"}, query.Arguments())

	_, err = analyzer.AnalyzeQuery(context.Background(), []string{"AUTH"})
	require.ErrorIs(t, err, compute.ErrInvalidArguments)
}

func TestAuthTokenIsNotLoggedAtDebugLevel(t *testing.T) {
	var sink bytes.Buffer
	logger := zerolog.New(&sink).Level(zerolog.DebugLevel)
	parser := compute.NewParser(&logger)

	_, err := parser.ParseAndAnalyzeQuery(context.Background(), "AUTH super-secret-token")
	require.NoError(t, err)

	tokens, err := parser.ParseQuery(context.Background(), "AUTH supersecrettoken")
	require.NoError(t, err)
	require.Len(t, tokens, 2)

	require.NotContains(t, sink.String(), "super-secret-token")
	require.NotContains(t, sink.String(), "supersecrettoken")
	require.Contains(t, sink.String(), "AUTH")
}
