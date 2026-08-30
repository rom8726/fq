package compute_test

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/compute"
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
			tokens: []string{"UNKNOWN"},
			err:    compute.ErrInvalidCommand,
		},
		"invalid number arguments for incr query": {
			tokens: []string{"INCR", "key"},
			err:    compute.ErrInvalidArguments,
		},
		"invalid number arguments for get query": {
			tokens: []string{"GET", "key"},
			err:    compute.ErrInvalidArguments,
		},
		"invalid number arguments for del query": {
			tokens: []string{"DEL", "key"},
			err:    compute.ErrInvalidArguments,
		},
		"invalid number arguments for mdel query": {
			tokens: []string{"MDEL", "key1", "600", "key2"},
			err:    compute.ErrInvalidArguments,
		},
		"invalid number arguments for message size query": {
			tokens: []string{"MSGSIZE", "key"},
			err:    compute.ErrInvalidArguments,
		},
		"invalid number arguments for rlimit query": {
			tokens: []string{"RLIMIT", "FW", "key", "100"},
			err:    compute.ErrInvalidArguments,
		},
		"invalid number arguments for token bucket rlimit query": {
			tokens: []string{"RLIMIT", "TB", "key", "100", "10"},
			err:    compute.ErrInvalidArguments,
		},
		"valid incr query": {
			tokens: []string{"INCR", "key", "60"},
			query:  compute.NewQuery(compute.IncrCommandID, []string{"key", "60"}),
		},
		"valid get query": {
			tokens: []string{"GET", "key", "60"},
			query:  compute.NewQuery(compute.GetCommandID, []string{"key", "60"}),
		},
		"valid del query": {
			tokens: []string{"DEL", "key", "60"},
			query:  compute.NewQuery(compute.DelCommandID, []string{"key", "60"}),
		},
		"valid mdel query": {
			tokens: []string{"MDEL", "key1", "60", "key2", "60"},
			query:  compute.NewQuery(compute.MDelCommandID, []string{"key1", "60", "key2", "60"}),
		},
		"valid message size query": {
			tokens: []string{"MSGSIZE"},
			query:  compute.NewQuery(compute.MsgSizeCommandID, []string{}),
		},
		"valid flushdb query": {
			tokens: []string{"FLUSHDB"},
			query:  compute.NewQuery(compute.FlushDBCommandID, []string{}),
		},
		"valid truncate query": {
			tokens: []string{"TRUNCATE"},
			query:  compute.NewQuery(compute.TruncateCommandID, []string{}),
		},
		"valid stream query": {
			tokens: []string{"STREAM"},
			query:  compute.NewQuery(compute.StreamCommandID, []string{}),
		},
		"valid pstream query": {
			tokens: []string{"PSTREAM", "tenant_a-"},
			query:  compute.NewQuery(compute.PStreamCommandID, []string{"tenant_a-"}),
		},
		"valid qstream query": {
			tokens: []string{"QSTREAM"},
			query:  compute.NewQuery(compute.QStreamCommandID, []string{}),
		},
		"valid qpstream query": {
			tokens: []string{"QPSTREAM", "tenant_a-"},
			query:  compute.NewQuery(compute.QPStreamCommandID, []string{"tenant_a-"}),
		},
		"valid rlimit query": {
			tokens: []string{"RLIMIT", "FW", "key", "100", "60"},
			query:  compute.NewQuery(compute.RLimitCommandID, []string{"FW", "key", "100", "60"}),
		},
		"valid token bucket rlimit query": {
			tokens: []string{"RLIMIT", "TB", "key", "100", "10", "60"},
			query:  compute.NewQuery(compute.RLimitCommandID, []string{"TB", "key", "100", "10", "60"}),
		},
		"invalid number arguments for quota acquire": {
			tokens: []string{"QUOTA", "ACQL", "pool", "10", "3"},
			err:    compute.ErrInvalidArguments,
		},
		"invalid quota action": {
			tokens: []string{"QUOTA", "TAKE", "pool"},
			err:    compute.ErrInvalidArguments,
		},
		"valid quota lease acquire query": {
			tokens: []string{"QUOTA", "ACQL", "pool", "10", "3", "client-1"},
			query:  compute.NewQuery(compute.QuotaCommandID, []string{"ACQL", "pool", "10", "3", "client-1"}),
		},
		"valid quota lease acquire query with ttl": {
			tokens: []string{"QUOTA", "ACQL", "pool", "10", "3", "client-1", "60"},
			query:  compute.NewQuery(compute.QuotaCommandID, []string{"ACQL", "pool", "10", "3", "client-1", "60"}),
		},
		"valid quota set query": {
			tokens: []string{"QUOTA", "SET", "pool", "10"},
			query:  compute.NewQuery(compute.QuotaCommandID, []string{"SET", "pool", "10"}),
		},
		"valid quota setn query": {
			tokens: []string{"QUOTA", "SETN", "pool", "10", "3"},
			query:  compute.NewQuery(compute.QuotaCommandID, []string{"SETN", "pool", "10", "3"}),
		},
		"valid server-owned quota acquire query": {
			tokens: []string{"QUOTA", "ACQ", "pool", "3", "client-1"},
			query:  compute.NewQuery(compute.QuotaCommandID, []string{"ACQ", "pool", "3", "client-1"}),
		},
		"valid server-owned quota acquire query with ttl": {
			tokens: []string{"QUOTA", "ACQ", "pool", "3", "client-1", "60"},
			query:  compute.NewQuery(compute.QuotaCommandID, []string{"ACQ", "pool", "3", "client-1", "60"}),
		},
		"valid negotiated quota acquire query": {
			tokens: []string{"QUOTA", "ACQN", "pool", "client-1"},
			query:  compute.NewQuery(compute.QuotaCommandID, []string{"ACQN", "pool", "client-1"}),
		},
		"valid negotiated quota acquire query with ttl": {
			tokens: []string{"QUOTA", "ACQN", "pool", "client-1", "60"},
			query:  compute.NewQuery(compute.QuotaCommandID, []string{"ACQN", "pool", "client-1", "60"}),
		},
		"valid quota release query": {
			tokens: []string{"QUOTA", "REL", "pool", "client-1"},
			query:  compute.NewQuery(compute.QuotaCommandID, []string{"REL", "pool", "client-1"}),
		},
		"valid quota delete query": {
			tokens: []string{"QUOTA", "DEL", "pool"},
			query:  compute.NewQuery(compute.QuotaCommandID, []string{"DEL", "pool"}),
		},
		"valid quota info query": {
			tokens: []string{"QUOTA", "INF", "pool"},
			query:  compute.NewQuery(compute.QuotaCommandID, []string{"INF", "pool"}),
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
