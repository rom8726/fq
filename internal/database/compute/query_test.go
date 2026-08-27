package compute_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/compute"
)

func TestQuery(t *testing.T) {
	query := compute.NewQuery(compute.GetCommandID, []string{"GET", "key", "60"})
	require.Equal(t, compute.GetCommandID, query.CommandID())
	require.True(t, reflect.DeepEqual([]string{"GET", "key", "60"}, query.Arguments()))
	require.Equal(t, "GET", query.Arg(0))
	require.Equal(t, "key", query.Arg(1))
	require.Equal(t, "60", query.Arg(2))
	require.Equal(t, 3, query.ArgumentCount())
}

func TestQueryFromSlots(t *testing.T) {
	query := compute.NewQueryFromSlots(compute.GetCommandID, 2, "key", "60", "", "", "")
	require.Equal(t, compute.GetCommandID, query.CommandID())
	require.Equal(t, "key", query.Arg(0))
	require.Equal(t, "60", query.Arg(1))
	require.Equal(t, []string{"key", "60"}, query.Arguments())
	require.Equal(t, 2, query.ArgumentCount())
}
