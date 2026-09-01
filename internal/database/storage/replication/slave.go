package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage/wal"
	"github.com/fq-db/fq/internal/observability"
	"github.com/fq-db/fq/internal/security"
)

type TCPClient interface {
	SendRaw(context.Context, []byte) ([]byte, error)
	Close() error
}

type WALReader interface {
	ReadSegmentData(ctx context.Context, data []byte, expectHeader bool) ([]*wal.LogData, error)
}

type TCPClientFactory interface {
	Create() (TCPClient, error)
}

var errSlaveClosed = errors.New("slave is shutting down")

type SlaveStatus struct {
	ReplicaID         string
	MasterAddress     string
	Connected         bool
	LastSegmentName   string
	LastSegmentOffset int64
	LastAppliedLSN    uint64
	ConsecutiveErrors int
	ReconnectTotal    uint64
	LastReconnectAt   time.Time
	UpdatedAt         time.Time
}

type Slave struct {
	clientFactory         TCPClientFactory
	client                TCPClient
	replicaID             string
	secret                security.Secret
	masterAddress         string
	walReader             WALReader
	walStream             chan<- wal.Chunk
	dumpStream            chan<- database.DumpChunk
	syncInterval          time.Duration
	walDirectory          string
	lastSegmentName       string
	lastSegmentOffset     int64
	dumpLastSegmentNumber uint64
	lastAppliedLSN        uint64 // Track last applied LSN to avoid duplicate application

	closeCh     chan struct{}
	closeDoneCh chan struct{}

	readDump        bool
	sessionUUID     string
	dumpAppliedCh   chan struct{}
	dumpAppliedOnce sync.Once

	// Retry mechanism
	maxRetries        int
	retryDelay        time.Duration
	maxRetryDelay     time.Duration
	consecutiveErrors int

	// Reconnection state
	reconnectMu     sync.Mutex
	reconnectTotal  atomic.Uint64
	lastReconnectAt atomic.Pointer[time.Time]

	status atomic.Pointer[SlaveStatus]

	logger *zerolog.Logger
}

func (s *Slave) refreshStatus(connected bool) {
	lastReconnectAt := time.Time{}
	if p := s.lastReconnectAt.Load(); p != nil {
		lastReconnectAt = *p
	}

	s.status.Store(&SlaveStatus{
		ReplicaID:         s.replicaID,
		MasterAddress:     s.masterAddress,
		Connected:         connected,
		LastSegmentName:   s.lastSegmentName,
		LastSegmentOffset: s.lastSegmentOffset,
		LastAppliedLSN:    s.lastAppliedLSN,
		ConsecutiveErrors: s.consecutiveErrors,
		ReconnectTotal:    s.reconnectTotal.Load(),
		LastReconnectAt:   lastReconnectAt,
		UpdatedAt:         time.Now(),
	})
}

func (s *Slave) Status() SlaveStatus {
	p := s.status.Load()
	if p == nil {
		return SlaveStatus{ReplicaID: s.replicaID, MasterAddress: s.masterAddress}
	}

	return *p
}

func NewSlave(
	client TCPClient,
	replicaID string,
	secret security.Secret,
	masterAddress string,
	walReader WALReader,
	walStream chan<- wal.Chunk,
	dumpStream chan<- database.DumpChunk,
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

	if replicaID == "" {
		return nil, errors.New("replicaID is invalid")
	}

	if logger == nil {
		return nil, errors.New("logger is invalid")
	}

	segmentName, segmentOffset, err := lastWALSegmentCursor(walDirectory)
	if err != nil {
		logger.Error().Err(err).Msg("failed to find last WAL segment cursor")
	}

	slave := &Slave{
		client:            client,
		replicaID:         replicaID,
		secret:            secret,
		masterAddress:     masterAddress,
		walReader:         walReader,
		walStream:         walStream,
		dumpStream:        dumpStream,
		syncInterval:      syncInterval,
		walDirectory:      walDirectory,
		lastSegmentName:   segmentName,
		lastSegmentOffset: segmentOffset,
		closeCh:           make(chan struct{}),
		closeDoneCh:       make(chan struct{}),
		dumpAppliedCh:     make(chan struct{}),
		readDump:          true,
		sessionUUID:       uuid.NewString(),
		maxRetries:        10,
		retryDelay:        time.Second,
		maxRetryDelay:     5 * time.Minute,
		consecutiveErrors: 0,
		logger:            logger,
	}
	slave.refreshStatus(true)

	return slave, nil
}

// NewSlaveWithFactory creates a slave with a client factory for reconnection support
func NewSlaveWithFactory(
	clientFactory TCPClientFactory,
	replicaID string,
	secret security.Secret,
	masterAddress string,
	walReader WALReader,
	walStream chan<- wal.Chunk,
	dumpStream chan<- database.DumpChunk,
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

	if replicaID == "" {
		return nil, errors.New("replicaID is invalid")
	}

	if logger == nil {
		return nil, errors.New("logger is invalid")
	}

	client, err := clientFactory.Create()
	if err != nil {
		return nil, fmt.Errorf("failed to create initial client: %w", err)
	}

	segmentName, segmentOffset, err := lastWALSegmentCursor(walDirectory)
	if err != nil {
		logger.Error().Err(err).Msg("failed to find last WAL segment cursor")
	}

	slave := &Slave{
		clientFactory:     clientFactory,
		client:            client,
		replicaID:         replicaID,
		secret:            secret,
		masterAddress:     masterAddress,
		walReader:         walReader,
		walStream:         walStream,
		dumpStream:        dumpStream,
		syncInterval:      syncInterval,
		walDirectory:      walDirectory,
		lastSegmentName:   segmentName,
		lastSegmentOffset: segmentOffset,
		closeCh:           make(chan struct{}),
		closeDoneCh:       make(chan struct{}),
		dumpAppliedCh:     make(chan struct{}),
		readDump:          true,
		sessionUUID:       uuid.NewString(),
		maxRetries:        10,
		retryDelay:        time.Second,
		maxRetryDelay:     5 * time.Minute,
		consecutiveErrors: 0,
		logger:            logger,
	}
	slave.refreshStatus(true)

	return slave, nil
}

func lastWALSegmentCursor(walDirectory string) (segmentName string, sz int64, err error) {
	segmentName, err = wal.SegmentLast(walDirectory)
	if err != nil || segmentName == "" {
		return segmentName, 0, err
	}

	stat, err := os.Stat(filepath.Join(walDirectory, segmentName))
	if err != nil {
		return segmentName, 0, err
	}

	return segmentName, stat.Size(), nil
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
				if err := s.waitForDumpApplied(ctx); err != nil {
					return
				}

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
	s.refreshStatus(false)
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
	s.refreshStatus(true)
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
		observability.IncReplicationReconnectAttemptsTotal()
		if err == nil {
			s.client = newClient
			observability.IncReplicationReconnectTotal()
			now := time.Now()
			s.reconnectTotal.Add(1)
			s.lastReconnectAt.Store(&now)
			s.refreshStatus(true)
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
				return errSlaveClosed
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
		return netErr.Timeout()
	}

	// Check for connection closed errors
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, io.EOF) {
		return true
	}

	// Check for broken pipe errors
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "write" || opErr.Op == "read"
	}

	return false
}

// waitForDumpApplied waits until dump is fully applied to the engine.
func (s *Slave) waitForDumpApplied(ctx context.Context) error {
	select {
	case <-s.dumpAppliedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return errSlaveClosed
	}
}

// markDumpApplied marks that dump has been fully applied
func (s *Slave) markDumpApplied() {
	s.dumpAppliedOnce.Do(func() {
		close(s.dumpAppliedCh)
		s.logger.Info().Msg("dump fully applied, WAL synchronization can start")
	})
}
