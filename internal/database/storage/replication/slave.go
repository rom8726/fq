package replication

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"fq/internal/database"
	"fq/internal/database/storage/wal"
)

type TCPClient interface {
	Send(context.Context, []byte) ([]byte, error)
}

type Slave struct {
	client                TCPClient
	walStream             chan<- []*wal.LogData
	dumpStream            chan<- []database.DumpElem
	syncInterval          time.Duration
	walDirectory          string
	lastSegmentName       string
	dumpLastSegmentNumber uint64

	closeCh     chan struct{}
	closeDoneCh chan struct{}

	readDump    bool
	sessionUUID string

	logger *zerolog.Logger
}

func NewSlave(
	client TCPClient,
	walStream chan<- []*wal.LogData,
	dumpStream chan<- []database.DumpElem,
	walDirectory string,
	syncInterval time.Duration,
	logger *zerolog.Logger,
) (*Slave, error) {
	if client == nil {
		return nil, errors.New("client is invalid")
	}

	if logger == nil {
		return nil, errors.New("logger is invalid")
	}

	segmentName, err := wal.SegmentLast(walDirectory)
	if err != nil {
		logger.Error().Err(err).Msg("failed to find last WAL segment")
	}

	return &Slave{
		client:          client,
		walStream:       walStream,
		dumpStream:      dumpStream,
		syncInterval:    syncInterval,
		walDirectory:    walDirectory,
		lastSegmentName: segmentName,
		closeCh:         make(chan struct{}),
		closeDoneCh:     make(chan struct{}),
		readDump:        true,
		sessionUUID:     uuid.NewString(),
		logger:          logger,
	}, nil
}

func (s *Slave) IsMaster() bool {
	return false
}

func (s *Slave) Start(ctx context.Context) {
	go func() {
		defer close(s.closeDoneCh)

		for {
			select {
			case <-s.closeCh:
				return
			default:
			}

			if s.readDump {
				select {
				case <-s.closeCh:
					return
				case <-ctx.Done():
					return
				default:
					if err := s.synchronizeDump(ctx); err != nil {
						s.logger.Error().Err(err).Msg("failed to synchronize dump")
					}
				}
			} else {
				select {
				case <-s.closeCh:
					return
				case <-ctx.Done():
					return
				case <-time.After(s.syncInterval):
					if err := s.synchronizeWAL(ctx); err != nil {
						s.logger.Error().Err(err).Msg("failed to synchronize wal")
					}
				}
			}
		}
	}()
}

func (s *Slave) Shutdown() {
	close(s.closeCh)
	<-s.closeDoneCh
}
