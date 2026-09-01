package inspect

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/storage"
	"github.com/fq-db/fq/internal/database/storage/replication"
	"github.com/fq-db/fq/internal/protocol"
)

func TestBuildSlaveInfo(t *testing.T) {
	status := replication.SlaveStatus{
		MasterAddress:     "127.0.0.1:9000",
		Connected:         true,
		LastSegmentName:   "0001.log",
		LastAppliedLSN:    42,
		ConsecutiveErrors: 2,
		LastErrorCode:     protocol.CodeInternal,
		ReconnectTotal:    3,
		UpdatedAt:         time.Unix(100, 0),
	}

	info := buildSlaveInfo(status)
	require.Equal(t, "127.0.0.1:9000", info.MasterAddress)
	require.True(t, info.Connected)
	require.Equal(t, uint64(42), info.LastAppliedLSN)
	require.Equal(t, int(protocol.CodeInternal), info.LastErrorCode)
	require.Nil(t, info.LastReconnectAt)
	require.Equal(t, int64(100), info.UpdatedAt)

	status.LastReconnectAt = time.Unix(200, 0)
	info = buildSlaveInfo(status)
	require.NotNil(t, info.LastReconnectAt)
	require.Equal(t, int64(200), *info.LastReconnectAt)
}

func TestToSubscriberInfo(t *testing.T) {
	info := toSubscriberInfo(storage.SubscriberStat{HasPrefix: false, QueueLen: 1, QueueCap: 10, Dropped: 2})
	require.Nil(t, info.Prefix)
	require.Equal(t, 1, info.QueueLen)
	require.Equal(t, uint64(2), info.Dropped)

	info = toSubscriberInfo(storage.SubscriberStat{HasPrefix: true, Prefix: "tenant-", QueueCap: 5})
	require.NotNil(t, info.Prefix)
	require.Equal(t, "tenant-", *info.Prefix)
}
