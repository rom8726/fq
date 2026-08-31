package initialization

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/database/storage"
	"github.com/fq-db/fq/internal/database/storage/dumper"
	"github.com/fq-db/fq/internal/database/storage/replication"
	walPkg "github.com/fq-db/fq/internal/database/storage/wal"
	"github.com/fq-db/fq/internal/inspect"
	"github.com/fq-db/fq/internal/network"
	"github.com/fq-db/fq/internal/observability"
	"github.com/fq-db/fq/internal/security"
)

type Initializer struct {
	wal            *walPkg.WAL
	engine         storage.Engine
	dumper         *dumper.Dumper
	server         *network.TCPServer
	logger         *zerolog.Logger
	slave          *replication.Slave
	master         *replication.Master
	walStream      chan walPkg.Chunk
	dumpStream     chan database.DumpChunk
	cfg            config.Config
	maxMessageSize int
	observability  *observability.Server
	registry       *security.Registry
	startedAt      time.Time
	tuiTLS         security.TLSOptions
}

func NewInitializer(cfg config.Config) (*Initializer, error) {
	logger, err := CreateLogger(cfg.Logging, os.Stdout)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	return newInitializer(cfg, logger)
}

func NewInitializerWithLogger(cfg config.Config, logger *zerolog.Logger) (*Initializer, error) {
	return newInitializer(cfg, logger)
}

func newInitializer(cfg config.Config, logger *zerolog.Logger) (*Initializer, error) {
	startedAt := time.Now()
	walStream := make(chan walPkg.Chunk, 1)
	dumpStream := make(chan database.DumpChunk, 1)

	var wal *walPkg.WAL
	if cfg.UsesWAL() {
		var err error
		wal, err = CreateWAL(cfg.WAL, logger, walStream)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize wal: %w", err)
		}
	}

	dbEngine, err := CreateEngine(cfg.Engine, logger, walStream, dumpStream)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize engine: %w", err)
	}

	registry, err := BuildRegistry(cfg.Network.Auth)
	if err != nil {
		return nil, fmt.Errorf("failed to build auth registry: %w", err)
	}

	networkTLS, tuiTLS, err := interactiveTLSOptions(cfg.Network.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize interactive tls: %w", err)
	}

	tcpServer, err := CreateNetwork(cfg.Network, registry, logger, networkTLS)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize network: %w", err)
	}

	maxMessageSize, err := cfg.Network.ParseMaxMessageSize()
	if err != nil {
		return nil, fmt.Errorf("failed to parse max message size: %w", err)
	}

	var dumpSrv *dumper.Dumper
	if cfg.UsesDump() {
		var dumpWAL dumper.WAL
		if wal != nil {
			dumpWAL = wal
		}
		dumpSrv = dumper.New(dbEngine, dumpWAL, cfg.Dump.Directory)
	}

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
		observability:  observability.NewServer(cfg.Observability.Address, cfg.Observability.Pprof, logger),
		registry:       registry,
		startedAt:      startedAt,
		tuiTLS:         tuiTLS,
	}

	initializer.initializeReplication(replica)

	return initializer, nil
}

func interactiveTLSOptions(cfg config.TLSConfig) (
	networkTLS security.TLSOptions,
	tuiTLS security.TLSOptions,
	err error,
) {
	networkTLS = cfg.Options()
	tuiTLS = cfg.ClientOptions()

	if cfg.ClientCAFile == "" {
		return networkTLS, tuiTLS, nil
	}

	caCert, clientCert, err := security.NewEphemeralClientCertificate("fq interactive tui")
	if err != nil {
		return security.TLSOptions{}, security.TLSOptions{}, err
	}

	networkTLS.ClientCACerts = append(networkTLS.ClientCACerts, caCert)
	tuiTLS.Certificates = append(tuiTLS.Certificates, clientCert)

	return networkTLS, tuiTLS, nil
}

func (i *Initializer) TUITLSOptions() security.TLSOptions {
	return i.tuiTLS
}

func (i *Initializer) IssueEphemeralAdminToken() (string, error) {
	if !i.registry.Enabled() {
		return "", nil
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate ephemeral token: %w", err)
	}

	token := base64.RawURLEncoding.EncodeToString(raw)
	if err := i.registry.Add(token, security.RoleAdmin); err != nil {
		return "", fmt.Errorf("register ephemeral token: %w", err)
	}

	return token, nil
}

func (i *Initializer) StartDatabase(ctx context.Context) error {
	computeLayer := i.createComputeLayer()

	strg, err := i.createStorageLayer()
	if err != nil {
		return err
	}

	// Shutdown storage (which includes slave) before closing channels
	defer func() {
		strg.Shutdown()
		// Close channels after slave is stopped
		close(i.walStream)
		close(i.dumpStream)
	}()

	db := database.NewDatabase(computeLayer, strg, i.logger, i.maxMessageSize)
	db.SetInspector(inspect.New(inspect.Deps{
		Cfg:       i.cfg,
		Storage:   strg,
		WAL:       i.wal,
		Dumper:    i.dumper,
		Master:    i.master,
		Slave:     i.slave,
		StartedAt: i.startedAt,
	}))

	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		strg.Start(groupCtx)

		return nil
	})

	var lastTx database.Tx
	if i.dumper != nil {
		var err error
		lastTx, err = i.dumper.Restore(ctx)
		if err != nil {
			return fmt.Errorf("restore dump failed: %w", err)
		}
	}

	if err := strg.LoadWAL(ctx, lastTx); err != nil {
		return err
	}

	if i.master != nil {
		group.Go(func() error {
			return i.master.Start(groupCtx)
		})
	}

	group.Go(func() error {
		return i.observability.Start(groupCtx)
	})

	group.Go(func() error {
		return i.server.HandleQueryStreams(groupCtx, func(
			ctx context.Context,
			query []byte,
			write func([]byte) error,
		) error {
			return db.HandleQueryStream(ctx, string(query), write)
		})

	})

	return group.Wait()
}

func (i *Initializer) createComputeLayer() *compute.Compute {
	queryParser := compute.NewParser(i.logger)
	queryAnalyzer := compute.NewAnalyzer(i.logger)

	return compute.NewCompute(queryParser, queryAnalyzer, i.logger)
}

func (i *Initializer) createStorageLayer() (*storage.Storage, error) {
	walSyncCommit := i.cfg.UsesWAL() && i.cfg.WAL.SyncCommit == config.WALSyncCommitOn
	var walStore storage.WAL
	if i.wal != nil {
		walStore = i.wal
	}
	var dumperSrv storage.Dumper
	if i.dumper != nil {
		dumperSrv = i.dumper
	}

	strg, err := storage.NewStorage(
		i.engine,
		walStore,
		dumperSrv,
		i.storageReplicaSlave(),
		i.logger,
		i.cfg.Engine.CleanInterval,
		i.cfg.Dump.Interval,
		walSyncCommit,
		i.cfg.Engine.LimitEventQueueCapacityValue(),
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
		if i.dumper != nil {
			i.dumper.SetWALCleanupLSNProvider(replicaWALCleanupLSNProvider{master: v})
		}
	default:
		i.logger.Error().Msg("incorrect replication type")
	}
}

type replicaWALCleanupLSNProvider struct {
	master *replication.Master
}

func (p replicaWALCleanupLSNProvider) WALCleanupLSN() (uint64, bool) {
	return p.master.MinReplicaAckLSN()
}

func (i *Initializer) storageReplicaSlave() storage.Replica {
	if i.slave == nil {
		return nil
	}

	return i.slave
}
