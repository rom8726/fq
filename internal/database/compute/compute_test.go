package compute_test

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"fq/internal/database/compute"
	"fq/internal/database/compute/mocks"
)

func TestHandleQueryWithParsingError(t *testing.T) {
	ctx := context.Background()

	parser := mocks.NewQueryParser(t)
	parser.On("ParseQuery", mock.Anything, "## key").
		Return(nil, compute.ErrInvalidCommand)
	analyzer := mocks.NewQueryAnalyzer(t)

	log := zerolog.Nop()
	comp := compute.NewCompute(parser, analyzer, &log)

	query, err := comp.HandleQuery(ctx, "## key")
	require.Error(t, err, compute.ErrInvalidCommand)
	require.Equal(t, compute.Query{}, query)
}

func TestHandleQueryWithAnalyzingError(t *testing.T) {
	ctx := context.Background()

	parser := mocks.NewQueryParser(t)
	parser.On("ParseQuery", mock.Anything, "TRUNCATE key").
		Return([]string{"TRUNCATE", "key"}, nil)
	analyzer := mocks.NewQueryAnalyzer(t)
	analyzer.On("AnalyzeQuery", mock.Anything, []string{"TRUNCATE", "key"}).
		Return(compute.Query{}, compute.ErrInvalidCommand)

	log := zerolog.Nop()
	comp := compute.NewCompute(parser, analyzer, &log)

	query, err := comp.HandleQuery(ctx, "TRUNCATE key")
	require.Error(t, err, compute.ErrInvalidCommand)
	require.Equal(t, compute.Query{}, query)
}

func TestHandleQuery(t *testing.T) {
	ctx := context.Background()

	parser := mocks.NewQueryParser(t)
	parser.On("ParseQuery", mock.Anything, "GET key 60").
		Return([]string{"GET", "key", "60"}, nil)
	analyzer := mocks.NewQueryAnalyzer(t)
	analyzer.On("AnalyzeQuery", mock.Anything, []string{"GET", "key", "60"}).
		Return(compute.NewQuery(compute.GetCommandID, []string{"key", "60"}), nil)

	log := zerolog.Nop()
	comp := compute.NewCompute(parser, analyzer, &log)

	query, err := comp.HandleQuery(ctx, "GET key 60")
	require.NoError(t, err)
	require.Equal(t, compute.NewQuery(compute.GetCommandID, []string{"key", "60"}), query)
}
