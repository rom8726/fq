package compute_test

import (
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/database/compute"
)

func FuzzParseQuery(f *testing.F) {
	seeds := []string{
		"",
		"   ",
		"set",
		"set key",
		" set   key  ",
		"字文下",
		".set#",
		"GET key 60",
		"AUTH token",
		"HELLO 1 AUTH token",
		"MDEL a 1 b 2",
		"QUOTA ACQ name 10 client",
		"RLIMIT TB key 10 5 60",
		"INSPECT section",
		"\t\n set \t key\n",
		"SET" + string(rune(0)),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	logger := zerolog.Nop()
	parser := compute.NewParser(&logger)
	ctx := context.Background()

	f.Fuzz(func(t *testing.T, query string) {
		_, _ = parser.ParseQuery(ctx, query)
		_, _ = parser.ParseAndAnalyzeQuery(ctx, query)
	})
}
