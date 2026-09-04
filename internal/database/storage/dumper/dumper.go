package dumper

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage/format"
)

const (
	dumpBatchSize        = 1000
	currentDumpFileName  = "current.dump"
	walCleanupRetryDelay = time.Second
)

type WAL interface {
	RemovePastSegments(ctx context.Context, lsn uint64) error
}

type WALCleanupLSNProvider interface {
	WALCleanupLSN() (uint64, bool)
}

type Engine interface {
	RestoreDumpElem(ctx context.Context, elem database.DumpElem) error
}

type Dumper struct {
	engine      Engine
	wal         WAL
	dir         string
	compression format.Compression

	walCleanupLSNProvider WALCleanupLSNProvider
	walCleanupMu          sync.Mutex
	pendingWALCleanupLSN  uint64
	walCleanupRequested   bool
	walCleanupNotify      chan struct{}
	walCleanupStop        chan struct{}
	walCleanupDone        chan struct{}
	walCleanupCtx         context.Context
	walCleanupCancel      context.CancelFunc

	sessions       map[string]*readSession
	sessMu         sync.Mutex
	readDumpMu     sync.RWMutex
	dumpVersion    uint64 // dump version for tracking changes
	sessionTTL     time.Duration
	cleanupTicker  *time.Ticker
	cleanupStop    chan struct{}
	maxSessions    int // maximum number of concurrent dump sessions
	activeSessions int // current number of active sessions
}

func New(engine Engine, wal WAL, dir string, compression format.Compression) *Dumper {
	walCleanupCtx, walCleanupCancel := context.WithCancel(context.Background())
	d := &Dumper{
		engine:           engine,
		wal:              wal,
		dir:              dir,
		compression:      compression,
		sessions:         make(map[string]*readSession),
		dumpVersion:      0,
		sessionTTL:       30 * time.Minute, // default session TTL
		cleanupStop:      make(chan struct{}),
		walCleanupNotify: make(chan struct{}, 1),
		walCleanupStop:   make(chan struct{}),
		walCleanupDone:   make(chan struct{}),
		walCleanupCtx:    walCleanupCtx,
		walCleanupCancel: walCleanupCancel,
		maxSessions:      10, // default max concurrent sessions
		activeSessions:   0,
	}

	// Start periodic session cleanup
	d.startSessionCleanup()
	d.startWALCleanup()

	return d
}

func (d *Dumper) currentDumpFilePath() string {
	return filepath.Join(d.dir, currentDumpFileName)
}

func (d *Dumper) CurrentDumpPath() string {
	return d.currentDumpFilePath()
}

func (d *Dumper) DumpCodec() format.CodecID {
	return d.compression.Codec
}

func (d *Dumper) formatVersion() uint16 {
	if d.compression.Enabled() {
		return dumpFormatVersionCompressed
	}

	return dumpFormatVersionRaw
}

func (d *Dumper) SetWALCleanupLSNProvider(provider WALCleanupLSNProvider) {
	d.walCleanupLSNProvider = provider
}

func (d *Dumper) scheduleWALCleanup(lsn uint64) {
	if d.wal == nil {
		return
	}

	d.walCleanupMu.Lock()
	if !d.walCleanupRequested || lsn > d.pendingWALCleanupLSN {
		d.pendingWALCleanupLSN = lsn
	}
	d.walCleanupRequested = true
	d.walCleanupMu.Unlock()

	select {
	case d.walCleanupNotify <- struct{}{}:
	default:
	}
}

func (d *Dumper) startWALCleanup() {
	if d.wal == nil {
		close(d.walCleanupDone)

		return
	}

	go func() {
		defer close(d.walCleanupDone)

		for {
			select {
			case <-d.walCleanupStop:
				return
			case <-d.walCleanupNotify:
				d.runPendingWALCleanups()
			}
		}
	}()
}

func (d *Dumper) runPendingWALCleanups() {
	for {
		d.walCleanupMu.Lock()
		if !d.walCleanupRequested {
			d.walCleanupMu.Unlock()

			return
		}
		lsn := d.pendingWALCleanupLSN
		d.walCleanupRequested = false
		d.walCleanupMu.Unlock()

		if err := d.wal.RemovePastSegments(d.walCleanupCtx, lsn); err != nil {
			d.walCleanupMu.Lock()
			if !d.walCleanupRequested || lsn > d.pendingWALCleanupLSN {
				d.pendingWALCleanupLSN = lsn
			}
			d.walCleanupRequested = true
			d.walCleanupMu.Unlock()

			select {
			case <-d.walCleanupStop:
				return
			case <-time.After(walCleanupRetryDelay):
			}

			select {
			case d.walCleanupNotify <- struct{}{}:
			default:
			}

			return
		}
	}
}

// invalidateAllSessions invalidates all active dump read sessions
func (d *Dumper) invalidateAllSessions() {
	d.sessMu.Lock()
	defer d.sessMu.Unlock()

	for _, sess := range d.sessions {
		if !sess.closed {
			sess.closed = true
			sess.data = nil
		}
	}
}

// startSessionCleanup starts periodic cleanup of expired sessions
func (d *Dumper) startSessionCleanup() {
	d.cleanupTicker = time.NewTicker(5 * time.Minute)
	go func() {
		for {
			select {
			case <-d.cleanupTicker.C:
				d.cleanupExpiredSessions()
			case <-d.cleanupStop:
				return
			}
		}
	}()
}

// cleanupExpiredSessions removes sessions that haven't been used longer than TTL
func (d *Dumper) cleanupExpiredSessions() {
	d.sessMu.Lock()
	defer d.sessMu.Unlock()

	now := time.Now()
	for uuid, sess := range d.sessions {
		if sess.closed || now.After(sess.lastAccess.Add(d.sessionTTL)) {
			delete(d.sessions, uuid)
		}
	}
}

// Shutdown stops periodic session cleanup
func (d *Dumper) Shutdown() {
	if d.cleanupTicker != nil {
		d.cleanupTicker.Stop()
	}
	close(d.cleanupStop)
	d.walCleanupCancel()
	close(d.walCleanupStop)
	<-d.walCleanupDone
}
