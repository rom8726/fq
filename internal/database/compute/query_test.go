package compute_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"fq/internal/database/compute"
)

func TestQuery(t *testing.T) {
	query := compute.NewQuery(compute.GetCommandID, []string{"GET", "key", "60"})
	require.Equal(t, compute.GetCommandID, query.CommandID())
	require.True(t, reflect.DeepEqual([]string{"GET", "key", "60"}, query.Arguments()))
}
