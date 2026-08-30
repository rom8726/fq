package compute_test

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/compute"
)

func TestParse(t *testing.T) {
	tests := map[string]struct {
		query  string
		tokens []string
		err    error
	}{
		"empty query": {
			query: "",
		},
		"query without tokens": {
			query: "   ",
		},
		"query with UTF symbols": {
			query: "字文下",
			err:   compute.ErrInvalidSymbol,
		},
		"query with one token": {
			query:  "set",
			tokens: []string{"set"},
		},
		"query with two tokens": {
			query:  "set key",
			tokens: []string{"set", "key"},
		},
		"query with one token with digits": {
			query:  "2set1",
			tokens: []string{"2set1"},
		},
		"query with one token with underscores": {
			query:  "_set__",
			tokens: []string{"_set__"},
		},
		"query with one token with invalid symbols": {
			query: ".set#",
			err:   compute.ErrInvalidSymbol,
		},
		"query with two tokens with additional spaces": {
			query:  " set   key  ",
			tokens: []string{"set", "key"},
		},
	}

	ctx := context.Background()

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			logger := zerolog.Nop()
			parser := compute.NewParser(&logger)

			tokens, err := parser.ParseQuery(ctx, test.query)
			require.Equal(t, test.err, err)
			require.True(t, reflect.DeepEqual(test.tokens, tokens))
		})
	}
}

func TestParserCanParseConcurrently(t *testing.T) {
	logger := zerolog.Nop()
	parser := compute.NewParser(&logger)
	ctx := context.Background()

	queries := []string{
		"INCR key_1 600",
		"GET key_2 60",
		"RLIMIT FW user-1 100 60",
		"RLIMIT TB token_1 100 10 60",
	}

	var wg sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				query := queries[(worker+i)%len(queries)]
				tokens, err := parser.ParseQuery(ctx, query)
				require.NoError(t, err)
				require.NotEmpty(t, tokens)
			}
		}(worker)
	}
	wg.Wait()
}

func TestParserParseAndAnalyzeQuery(t *testing.T) {
	tests := map[string]struct {
		query   string
		command compute.CommandID
		args    []string
		err     error
	}{
		"empty query": {
			query: "",
			err:   compute.ErrInvalidCommand,
		},
		"invalid symbol": {
			query: ".set#",
			err:   compute.ErrInvalidSymbol,
		},
		"invalid command": {
			query: "TRUNCATE key",
			err:   compute.ErrInvalidCommand,
		},
		"invalid arguments": {
			query: "INCR key",
			err:   compute.ErrInvalidArguments,
		},
		"valid lowercase incr": {
			query:   "incr key 60",
			command: compute.IncrCommandID,
			args:    []string{"key", "60"},
		},
		"valid mdel": {
			query:   "MDEL key1 60 key2 60",
			command: compute.MDelCommandID,
			args:    []string{"key1", "60", "key2", "60"},
		},
		"valid qstream": {
			query:   "QSTREAM",
			command: compute.QStreamCommandID,
			args:    []string{},
		},
		"valid qpstream": {
			query:   "QPSTREAM tenant-",
			command: compute.QPStreamCommandID,
			args:    []string{"tenant-"},
		},
		"valid token bucket rlimit": {
			query:   "RLIMIT TB key 100 10 60",
			command: compute.RLimitCommandID,
			args:    []string{"TB", "key", "100", "10", "60"},
		},
		"valid quota lease acquire with ttl": {
			query:   "QUOTA ACQL pool 10 3 client-1 60",
			command: compute.QuotaCommandID,
			args:    []string{"ACQL", "pool", "10", "3", "client-1", "60"},
		},
		"valid quota set": {
			query:   "QUOTA SET pool 10",
			command: compute.QuotaCommandID,
			args:    []string{"SET", "pool", "10"},
		},
		"valid server-owned quota acquire with ttl": {
			query:   "QUOTA ACQ pool 3 client-1 60",
			command: compute.QuotaCommandID,
			args:    []string{"ACQ", "pool", "3", "client-1", "60"},
		},
		"valid quota release": {
			query:   "QUOTA REL pool client-1",
			command: compute.QuotaCommandID,
			args:    []string{"REL", "pool", "client-1"},
		},
		"valid quota delete": {
			query:   "QUOTA DEL pool",
			command: compute.QuotaCommandID,
			args:    []string{"DEL", "pool"},
		},
		"valid quota info": {
			query:   "QUOTA INF pool",
			command: compute.QuotaCommandID,
			args:    []string{"INF", "pool"},
		},
	}

	ctx := context.Background()
	logger := zerolog.Nop()
	parser := compute.NewParser(&logger)

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			query, err := parser.ParseAndAnalyzeQuery(ctx, test.query)
			require.Equal(t, test.err, err)
			if test.err != nil {
				return
			}

			require.Equal(t, test.command, query.CommandID())
			require.Equal(t, test.args, query.Arguments())
		})
	}
}

func BenchmarkParserParseQuery(b *testing.B) {
	logger := zerolog.Nop()
	parser := compute.NewParser(&logger)
	ctx := context.Background()
	query := "INCR bench_key_123 600"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tokens, err := parser.ParseQuery(ctx, query)
		if err != nil {
			b.Fatal(err)
		}
		if len(tokens) != 3 {
			b.Fatalf("tokens = %d", len(tokens))
		}
	}
}

func BenchmarkParserParseAndAnalyzeQuery(b *testing.B) {
	logger := zerolog.Nop()
	parser := compute.NewParser(&logger)
	ctx := context.Background()
	queryStr := "INCR bench_key_123 600"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		query, err := parser.ParseAndAnalyzeQuery(ctx, queryStr)
		if err != nil {
			b.Fatal(err)
		}
		if query.CommandID() != compute.IncrCommandID || query.Arg(0) != "bench_key_123" || query.Arg(1) != "600" {
			b.Fatalf("query = %+v", query)
		}
	}
}
