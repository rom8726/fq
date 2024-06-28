package initialization

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"fq/internal/config"
	"fq/internal/database"
	"fq/internal/database/compute"
	"fq/internal/database/storage"
	"fq/internal/database/storage/dumper"
	"fq/internal/database/storage/replication"
	walPkg "fq/internal/database/storage/wal"
	"fq/internal/network"
)

type Initializer struct {
	wal            *walPkg.WAL
	engine         storage.Engine
	dumper         *dumper.Dumper
	server         *network.TCPServer
	logger         *zerolog.Logger
	slave          *replication.Slave
	master         *replication.Master
	walStream      chan []*walPkg.LogData
	dumpStream     chan []database.DumpElem
	cfg            config.Config
	maxMessageSize int
}

func NewInitializer(cfg config.Config) (*Initializer, error) {
	walStream := make(chan []*walPkg.LogData, 1)
	dumpStream := make(chan []database.DumpElem, 1)

	logger, err := CreateLogger(cfg.Logging)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	wal, err := CreateWAL(cfg.WAL, logger, walStream)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize wal: %w", err)
	}

	dbEngine, err := CreateEngine(cfg.Engine, logger, walStream, dumpStream)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize engine: %w", err)
	}

	tcpServer, err := CreateNetwork(cfg.Network, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize network: %w", err)
	}

	maxMessageSize, err := cfg.Network.ParseMaxMessageSize()
	if err != nil {
		return nil, fmt.Errorf("failed to parse max message size: %w", err)
	}

	dumpSrv := dumper.New(dbEngine, wal, cfg.Dump.Directory)

	replica, err := CreateReplica(cfg.Replication, cfg.WAL, logger, dumpSrv, walStream, dumpStream)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize replication: %w", err)
	}

	initializer := &Initializer{
		wal:            wal,
		engine:         dbEngine,
		dumper:         dumpSrv,
		server:         tcpServer,
		logger:         logger,
		walStream:      walStream,
		dumpStream:     dumpStream,
		cfg:            cfg,
		maxMessageSize: maxMessageSize,
	}

	initializer.initializeReplication(replica)

	return initializer, nil
}

func (i *Initializer) StartDatabase(ctx context.Context) error {
	defer close(i.walStream)
	defer close(i.dumpStream)

	computeLayer := i.createComputeLayer()

	strg, err := i.createStorageLayer(ctx)
	if err != nil {
		return err
	}

	defer strg.Shutdown()

	db := database.NewDatabase(computeLayer, strg, i.logger, i.maxMessageSize)

	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		strg.Start(groupCtx)

		return nil
	})

	lastTx, err := i.dumper.Restore(ctx)
	if err != nil {
		return fmt.Errorf("restore dump failed: %w", err)
	}

	if i.wal != nil {
		if err := strg.LoadWAL(ctx, lastTx); err != nil {
			return err
		}
	}

	if i.master != nil {
		group.Go(func() error {
			return i.master.Start(groupCtx)
		})
	}

	group.Go(func() error {
		return i.server.HandleQueries(groupCtx, func(ctx context.Context, query []byte) ([]byte, error) {
			response := db.HandleQuery(ctx, string(query))

			return []byte(response), nil
		})
	})

	return group.Wait()
}

func (i *Initializer) createComputeLayer() *compute.Compute {
	queryParser := compute.NewParser(i.logger)
	queryAnalyzer := compute.NewAnalyzer(i.logger)

	return compute.NewCompute(queryParser, queryAnalyzer, i.logger)
}

func (i *Initializer) createStorageLayer(context.Context) (*storage.Storage, error) {
	// if i.slave != nil {
	// i.slave.StartSynchronization(ctx) // TODO:
	// }

	walSyncCommit := i.cfg.WAL != nil && i.cfg.WAL.SyncCommit == config.WALSyncCommitOn

	strg, err := storage.NewStorage(
		i.engine,
		i.wal,
		i.dumper,
		i.storageReplicaSlave(),
		i.logger,
		i.cfg.Engine.CleanInterval,
		i.cfg.Dump.Interval,
		walSyncCommit,
	)
	if err != nil {
		i.logger.Error().Err(err).Msg("failed to initialize storage layer")

		return nil, err
	}

	return strg, nil
}

func (i *Initializer) initializeReplication(replica interface{}) {
	if replica == nil {
		return
	}

	if i.wal == nil {
		i.logger.Error().Msg("wal is required for replication")

		return
	}

	switch v := replica.(type) {
	case *replication.Slave:
		i.slave = v
	case *replication.Master:
		i.master = v
	default:
		i.logger.Error().Msg("incorrect replication type")
	}
}

func (i *Initializer) storageReplicaSlave() storage.Replica {
	if i.slave == nil {
		return nil
	}

	return i.slave
}
