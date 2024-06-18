package compute_test

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"fq/internal/database/compute"
)

func TestAnalyzeQuery(t *testing.T) {
	tests := map[string]struct {
		tokens []string
		query  compute.Query
		err    error
	}{
		"empty tokens": {
			tokens: []string{},
			err:    compute.ErrInvalidCommand,
		},
		"invalid command": {
			tokens: []string{"TRUNCATE"},
			err:    compute.ErrInvalidCommand,
		},
		"invalid number arguments for set query": {
			tokens: []string{"INCR", "key"},
			err:    compute.ErrInvalidArguments,
		},
		"invalid number arguments for get query": {
			tokens: []string{"GET", "key", "value"},
			err:    compute.ErrInvalidArguments,
		},
		"invalid number arguments for del query": {
			tokens: []string{"GET", "key", "value"},
			err:    compute.ErrInvalidArguments,
		},
		"valid set query": {
			tokens: []string{"INCR", "key", "batch"},
			query:  compute.NewQuery(compute.IncrCommandID, []string{"key", "batch"}),
		},
		"valid get query": {
			tokens: []string{"GET", "key"},
			query:  compute.NewQuery(compute.GetCommandID, []string{"key"}),
		},
	}

	ctx := context.Background()
	logger := zerolog.Nop()
	analyzer := compute.NewAnalyzer(&logger)

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			query, err := analyzer.AnalyzeQuery(ctx, test.tokens)
			require.Equal(t, test.query, query)
			require.Equal(t, test.err, err)
		})
	}
}
