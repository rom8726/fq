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
	walPkg "fq/internal/database/storage/wal"
	"fq/internal/network"
)

type Initializer struct {
	wal            storage.WAL
	engine         storage.Engine
	server         *network.TCPServer
	logger         *zerolog.Logger
	stream         chan []*walPkg.LogData
	cfg            config.Config
	maxMessageSize int
}

func NewInitializer(cfg config.Config) (*Initializer, error) {
	stream := make(chan []*walPkg.LogData, 1)

	logger, err := CreateLogger(cfg.Logging)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	wal, err := CreateWAL(cfg.WAL, logger, stream)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize wal: %w", err)
	}

	dbEngine, err := CreateEngine(cfg.Engine, logger, stream)
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

	initializer := &Initializer{
		wal:            wal,
		engine:         dbEngine,
		server:         tcpServer,
		logger:         logger,
		stream:         stream,
		cfg:            cfg,
		maxMessageSize: maxMessageSize,
	}

	return initializer, nil
}

func (i *Initializer) StartDatabase(ctx context.Context) error {
	defer close(i.stream)

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

	if i.wal != nil {
		if err := strg.LoadWAL(ctx); err != nil {
			return err
		}
	}

	group.Go(func() error {
		return i.server.HandleQueries(groupCtx, func(ctx context.Context, query []byte) []byte {
			response := db.HandleQuery(ctx, string(query))

			return []byte(response)
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

	strg, err := storage.NewStorage(
		i.engine,
		i.wal,
		i.logger,
		i.cfg.Engine.CleanInterval,
		i.cfg.Engine.DumpInterval,
	)
	if err != nil {
		i.logger.Error().Err(err).Msg("failed to initialize storage layer")

		return nil, err
	}

	return strg, nil
}
