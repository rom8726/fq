package initialization

import (
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage/dumper"
	"github.com/fq-db/fq/internal/database/storage/replication"
	"github.com/fq-db/fq/internal/database/storage/wal"
	"github.com/fq-db/fq/internal/network"
	"github.com/fq-db/fq/internal/tools"
)

const defaultReplicationMasterAddress = ":1946"
const defaultReplicationSyncInterval = time.Second
const defaultReplicationMaxMessageSize = 16 << 20
const replicationMessageOverheadMin = 1 << 20

func CreateReplica(
	replicationCfg config.ReplicationConfig,
	walCfg *config.WALConfig,
	compression replication.Compression,
	logger *zerolog.Logger,
	dumperSrv *dumper.Dumper,
	walStream chan<- wal.Chunk,
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

	secret := replicationCfg.Auth.ResolvedSecret()
	if secret.Empty() {
		return nil, errors.New("replication auth secret is required")
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
		serverTLS, err := replicationCfg.TLS.Options().ServerConfig()
		if err != nil {
			return nil, fmt.Errorf("replication tls: %w", err)
		}

		var options []network.ServerOption
		if serverTLS != nil {
			options = append(options, network.WithTLS(serverTLS))
		}

		warnCleartextAuth(logger, replicationPortName, masterAddress, true, serverTLS != nil)

		server, err := network.NewTCPServer(
			masterAddress, maxReplicasNumber, maxMessageSize, idleTimeout, logger, options...,
		)
		if err != nil {
			return nil, err
		}

		return replication.NewMaster(server, walDirectory, dumperSrv, secret, compression, logger)
	}

	clientTLS, err := replicationCfg.TLS.Options().ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("replication tls: %w", err)
	}

	warnCleartextAuth(logger, replicationPortName, masterAddress, true, clientTLS != nil)

	// Create client factory for reconnection support
	clientFactory := replication.NewTCPClientFactory(masterAddress, maxMessageSize, idleTimeout, clientTLS)

	fsReader := wal.NewFSReader(walDirectory, logger)

	return replication.NewSlaveWithFactory(
		clientFactory,
		replicationCfg.ReplicaID,
		secret,
		masterAddress,
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
