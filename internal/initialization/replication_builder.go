package initialization

import (
	"errors"
	"time"

	"github.com/rs/zerolog"

	"fq/internal/config"
	"fq/internal/database"
	"fq/internal/database/storage/dumper"
	"fq/internal/database/storage/replication"
	"fq/internal/database/storage/wal"
	"fq/internal/network"
	"fq/internal/tools"
)

const defaultReplicationMasterAddress = ":1946"
const defaultReplicationSyncInterval = time.Second
const defaultReplicationMaxMessageSize = 16 << 20
const replicationMessageOverheadMin = 1 << 20

func CreateReplica(
	replicationCfg config.ReplicationConfig,
	walCfg *config.WALConfig,
	logger *zerolog.Logger,
	dumperSrv *dumper.Dumper,
	walStream chan<- []*wal.LogData,
	dumpStream chan<- database.DumpChunk,
) (interface{}, error) {
	if replicationCfg.ReplicaType == "" {
		return nil, nil
	}

	replicaType := replicationCfg.ReplicaType
	masterAddress := defaultReplicationMasterAddress
	syncInterval := defaultReplicationSyncInterval
	walDirectory := defaultWALDataDirectory

	if replicationCfg.ReplicaType != config.ReplicaTypeMaster && replicationCfg.ReplicaType != config.ReplicaTypeSlave {
		return nil, errors.New("replica type is incorrect")
	}

	if replicationCfg.MasterAddress != "" {
		masterAddress = replicationCfg.MasterAddress
	}

	if replicationCfg.SyncInterval != 0 {
		syncInterval = replicationCfg.SyncInterval
	}

	if walCfg != nil && walCfg.DataDirectory != "" {
		walDirectory = walCfg.DataDirectory
	}

	const maxReplicasNumber = 5
	maxMessageSize, err := replicationMaxMessageSize(walCfg)
	if err != nil {
		return nil, err
	}
	idleTimeout := syncInterval * 3

	if replicaType == config.ReplicaTypeMaster {
		server, err := network.NewTCPServer(masterAddress, maxReplicasNumber, maxMessageSize, idleTimeout, logger)
		if err != nil {
			return nil, err
		}

		return replication.NewMaster(server, walDirectory, dumperSrv, logger)
	}

	// Create client factory for reconnection support
	clientFactory := replication.NewTCPClientFactory(masterAddress, maxMessageSize, idleTimeout)

	fsReader := wal.NewFSReader(walDirectory, logger)

	return replication.NewSlaveWithFactory(
		clientFactory,
		fsReader,
		walStream,
		dumpStream,
		walDirectory,
		syncInterval,
		logger,
	)
}

func replicationMaxMessageSize(walCfg *config.WALConfig) (int, error) {
	if walCfg == nil || walCfg.MaxSegmentSize == "" {
		return defaultReplicationMaxMessageSize, nil
	}

	maxSegmentSize, err := tools.ParseSize(walCfg.MaxSegmentSize)
	if err != nil {
		return 0, errors.New("max segment size is incorrect")
	}

	overhead := maxSegmentSize / 10
	if overhead < replicationMessageOverheadMin {
		overhead = replicationMessageOverheadMin
	}

	return max(defaultReplicationMaxMessageSize, maxSegmentSize+overhead), nil
}
