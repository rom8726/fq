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
)

const defaultReplicationType = "master"
const defaultReplicationMasterAddress = ":1946"
const defaultReplicationSyncInterval = time.Second

func CreateReplica(
	replicationCfg config.ReplicationConfig,
	walCfg *config.WALConfig,
	logger *zerolog.Logger,
	dumperSrv *dumper.Dumper,
	walStream chan<- []*wal.LogData,
	dumpStream chan<- []database.DumpElem,
) (interface{}, error) {
	replicaType := defaultReplicationType
	masterAddress := defaultReplicationMasterAddress
	syncInterval := defaultReplicationSyncInterval
	walDirectory := defaultWALDataDirectory

	if replicationCfg.ReplicaType != "" {
		if replicationCfg.ReplicaType != "master" && replicationCfg.ReplicaType != "slave" {
			return nil, errors.New("replica type is incorrect")
		}

		replicaType = replicationCfg.ReplicaType
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
	const maxMessageSize = 16 << 20
	idleTimeout := syncInterval * 3

	if replicaType == "master" {
		server, err := network.NewTCPServer(masterAddress, maxReplicasNumber, maxMessageSize, idleTimeout, logger)
		if err != nil {
			return nil, err
		}

		return replication.NewMaster(server, walDirectory, dumperSrv, logger)
	}

	client, err := network.NewTCPClient(masterAddress, maxMessageSize, idleTimeout)
	if err != nil {
		return nil, err
	}

	fsReader := wal.NewFSReader(walDirectory, logger)

	return replication.NewSlave(client, fsReader, walStream, dumpStream, walDirectory, syncInterval, logger)
}
