package dumper

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"fq/internal/database"
)

const (
	dumpBatchSize       = 1000
	currentDumpFileName = "current.dump"
)

type WAL interface {
	RemovePastSegments(ctx context.Context, lsn uint64) error
}

type Engine interface {
	Dump(context.Context, database.Tx) (<-chan database.DumpElem, <-chan error)
	RestoreDumpElem(ctx context.Context, elem database.DumpElem) error
}

type Dumper struct {
	engine Engine
	wal    WAL
	dir    string

	sessions       map[string]readSession
	sessMu         sync.Mutex
	readDumpMu     sync.RWMutex
	dumpVersion    uint64 // dump version for tracking changes
	sessionTTL     time.Duration
	cleanupTicker  *time.Ticker
	cleanupStop    chan struct{}
	maxSessions    int // maximum number of concurrent dump sessions
	activeSessions int // current number of active sessions
}

func New(engine Engine, wal WAL, dir string) *Dumper {
	d := &Dumper{
		engine:         engine,
		wal:            wal,
		dir:            dir,
		sessions:       make(map[string]readSession),
		dumpVersion:    0,
		sessionTTL:     30 * time.Minute, // default session TTL
		cleanupStop:    make(chan struct{}),
		maxSessions:    10, // default max concurrent sessions
		activeSessions: 0,
	}

	// Start periodic session cleanup
	d.startSessionCleanup()

	return d
}

func (d *Dumper) currentDumpFilePath() string {
	return filepath.Join(d.dir, currentDumpFileName)
}

// invalidateAllSessions invalidates all active dump read sessions
func (d *Dumper) invalidateAllSessions() {
	d.sessMu.Lock()
	defer d.sessMu.Unlock()

	for uuid, sess := range d.sessions {
		if !sess.closed {
			sess.closed = true
			sess.buff = nil
			d.sessions[uuid] = sess
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
}
