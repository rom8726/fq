package initialization

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/config"
)

func TestReplicationMaxMessageSizeDefaultsToMinimum(t *testing.T) {
	size, err := replicationMaxMessageSize(nil)

	require.NoError(t, err)
	require.Equal(t, defaultReplicationMaxMessageSize, size)
}

func TestReplicationMaxMessageSizeKeepsMinimumForSmallSegments(t *testing.T) {
	size, err := replicationMaxMessageSize(&config.WALConfig{MaxSegmentSize: "4MB"})

	require.NoError(t, err)
	require.Equal(t, defaultReplicationMaxMessageSize, size)
}

func TestReplicationMaxMessageSizeExceedsWALSegmentSize(t *testing.T) {
	size, err := replicationMaxMessageSize(&config.WALConfig{MaxSegmentSize: "64MB"})

	require.NoError(t, err)
	require.Greater(t, size, 64<<20)
}
