package replication

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"fq/internal/database"
	"fq/internal/database/storage/wal"
)

type TCPClient interface {
	Send(context.Context, []byte) ([]byte, error)
	Close() error
}

type WALReader interface {
	ReadSegmentData(ctx context.Context, data []byte) ([]*wal.LogData, error)
}

type TCPClientFactory interface {
	Create() (TCPClient, error)
}

type Slave struct {
	clientFactory         TCPClientFactory
	client                TCPClient
	walReader             WALReader
	walStream             chan<- []*wal.LogData
	dumpStream            chan<- []database.DumpElem
	syncInterval          time.Duration
	walDirectory          string
	lastSegmentName       string
	lastSegmentSize       int64 // Track size of last segment to detect updates
	dumpLastSegmentNumber uint64
	lastAppliedLSN        uint64 // Track last applied LSN to avoid duplicate application

	closeCh     chan struct{}
	closeDoneCh chan struct{}

	readDump        bool
	sessionUUID     string
	dumpApplied     bool
	dumpAppliedMu   sync.Mutex
	dumpAppliedCond *sync.Cond

	// Retry mechanism
	retryCount        int
	maxRetries        int
	retryDelay        time.Duration
	maxRetryDelay     time.Duration
	consecutiveErrors int

	// Reconnection state
	reconnectMu sync.Mutex

	logger *zerolog.Logger
}

func NewSlave(
	client TCPClient,
	walReader WALReader,
	walStream chan<- []*wal.LogData,
	dumpStream chan<- []database.DumpElem,
	walDirectory string,
	syncInterval time.Duration,
	logger *zerolog.Logger,
) (*Slave, error) {
	if walReader == nil {
		return nil, errors.New("walReader is invalid")
	}

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

	slave := &Slave{
		client:            client,
		walReader:         walReader,
		walStream:         walStream,
		dumpStream:        dumpStream,
		syncInterval:      syncInterval,
		walDirectory:      walDirectory,
		lastSegmentName:   segmentName,
		closeCh:           make(chan struct{}),
		closeDoneCh:       make(chan struct{}),
		readDump:          true,
		sessionUUID:       uuid.NewString(),
		maxRetries:        10,
		retryDelay:        time.Second,
		maxRetryDelay:     5 * time.Minute,
		consecutiveErrors: 0,
		dumpApplied:       false,
		logger:            logger,
	}
	slave.dumpAppliedCond = sync.NewCond(&slave.dumpAppliedMu)
	return slave, nil
}

// NewSlaveWithFactory creates a slave with a client factory for reconnection support
func NewSlaveWithFactory(
	clientFactory TCPClientFactory,
	walReader WALReader,
	walStream chan<- []*wal.LogData,
	dumpStream chan<- []database.DumpElem,
	walDirectory string,
	syncInterval time.Duration,
	logger *zerolog.Logger,
) (*Slave, error) {
	if walReader == nil {
		return nil, errors.New("walReader is invalid")
	}

	if clientFactory == nil {
		return nil, errors.New("clientFactory is invalid")
	}

	if logger == nil {
		return nil, errors.New("logger is invalid")
	}

	client, err := clientFactory.Create()
	if err != nil {
		return nil, fmt.Errorf("failed to create initial client: %w", err)
	}

	segmentName, err := wal.SegmentLast(walDirectory)
	if err != nil {
		logger.Error().Err(err).Msg("failed to find last WAL segment")
	}

	slave := &Slave{
		clientFactory:     clientFactory,
		client:            client,
		walReader:         walReader,
		walStream:         walStream,
		dumpStream:        dumpStream,
		syncInterval:      syncInterval,
		walDirectory:      walDirectory,
		lastSegmentName:   segmentName,
		closeCh:           make(chan struct{}),
		closeDoneCh:       make(chan struct{}),
		readDump:          true,
		sessionUUID:       uuid.NewString(),
		maxRetries:        10,
		retryDelay:        time.Second,
		maxRetryDelay:     5 * time.Minute,
		consecutiveErrors: 0,
		dumpApplied:       false,
		logger:            logger,
	}
	slave.dumpAppliedCond = sync.NewCond(&slave.dumpAppliedMu)
	return slave, nil
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
						s.handleSyncError(err, "dump")
					} else {
						s.resetRetryState()
					}
				}
			} else {
				// Wait for dump to be fully applied before starting WAL sync
				s.waitForDumpApplied()

				select {
				case <-s.closeCh:
					return
				case <-ctx.Done():
					return
				case <-time.After(s.getRetryDelay()):
					if err := s.synchronizeWAL(ctx); err != nil {
						s.handleSyncError(err, "wal")
					} else {
						s.resetRetryState()
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

// handleSyncError handles synchronization errors with exponential backoff
func (s *Slave) handleSyncError(err error, syncType string) {
	s.consecutiveErrors++
	s.logger.Error().
		Err(err).
		Str("sync_type", syncType).
		Int("consecutive_errors", s.consecutiveErrors).
		Int("max_retries", s.maxRetries).
		Dur("next_retry_delay", s.getRetryDelay()).
		Msg("synchronization error")

	if s.consecutiveErrors >= s.maxRetries {
		s.logger.Error().
			Int("max_retries", s.maxRetries).
			Msg("max retries reached, entering wait mode")
		// Reset counter after long wait
		time.Sleep(s.maxRetryDelay)
		s.consecutiveErrors = 0
	}
}

// getRetryDelay returns delay before next attempt with exponential backoff
func (s *Slave) getRetryDelay() time.Duration {
	if s.consecutiveErrors == 0 {
		return s.syncInterval
	}

	// Exponential delay: delay = baseDelay * 2^(errors-1)
	delay := s.retryDelay
	for i := 0; i < s.consecutiveErrors-1 && i < 10; i++ {
		delay *= 2
		if delay > s.maxRetryDelay {
			delay = s.maxRetryDelay
			break
		}
	}

	return delay
}

// resetRetryState resets retry state after successful synchronization
func (s *Slave) resetRetryState() {
	if s.consecutiveErrors > 0 {
		s.logger.Info().
			Int("previous_errors", s.consecutiveErrors).
			Msg("synchronization restored, resetting error counter")
		s.consecutiveErrors = 0
	}
}

// reconnect attempts to reconnect to master with exponential backoff
func (s *Slave) reconnect(ctx context.Context) error {
	if s.clientFactory == nil {
		return errors.New("client factory not available for reconnection")
	}

	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()

	// Close old connection if exists
	if s.client != nil {
		_ = s.client.Close()
	}

	reconnectDelay := s.retryDelay
	maxAttempts := 5

	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closeCh:
			return errors.New("slave is shutting down")
		default:
		}

		newClient, err := s.clientFactory.Create()
		if err == nil {
			s.client = newClient
			s.logger.Info().
				Int("attempt", attempt+1).
				Int("max_attempts", maxAttempts).
				Msg("successfully reconnected to master")
			return nil
		}

		s.logger.Warn().
			Err(err).
			Int("attempt", attempt+1).
			Int("max_attempts", maxAttempts).
			Dur("delay", reconnectDelay).
			Msg("reconnection attempt failed, retrying")

		if attempt < maxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.closeCh:
				return errors.New("slave is shutting down")
			case <-time.After(reconnectDelay):
				reconnectDelay *= 2
				if reconnectDelay > s.maxRetryDelay {
					reconnectDelay = s.maxRetryDelay
				}
			}
		}
	}

	return fmt.Errorf("failed to reconnect after %d attempts", maxAttempts)
}

// isNetworkError checks if error is a network error that requires reconnection
func (s *Slave) isNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// Check for network errors
	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout() || netErr.Temporary()
	}

	// Check for connection closed errors
	if errors.Is(err, net.ErrClosed) {
		return true
	}

	// Check for broken pipe errors
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "write" || opErr.Op == "read"
	}

	return false
}

// waitForDumpApplied waits until dump is fully applied to the engine
func (s *Slave) waitForDumpApplied() {
	s.dumpAppliedMu.Lock()
	defer s.dumpAppliedMu.Unlock()

	for !s.dumpApplied {
		s.dumpAppliedCond.Wait()
	}
}

// markDumpApplied marks that dump has been fully applied
func (s *Slave) markDumpApplied() {
	s.dumpAppliedMu.Lock()
	defer s.dumpAppliedMu.Unlock()

	if !s.dumpApplied {
		s.dumpApplied = true
		s.dumpAppliedCond.Broadcast()
		s.logger.Info().Msg("dump fully applied, WAL synchronization can start")
	}
}
