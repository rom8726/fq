package dumper

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"time"

	"fq/internal/database"
)

type readSession struct {
	buff        *bytes.Buffer
	closed      bool
	dumpVersion uint64    // dump version when session was created
	lastAccess  time.Time // last access time to the session
}

func (d *Dumper) GetNextData(sessionUUID string) ([]database.DumpElem, bool, error) {
	// Check session limit
	d.sessMu.Lock()
	if d.activeSessions >= d.maxSessions {
		// Check if session already exists
		if _, exists := d.sessions[sessionUUID]; !exists {
			d.sessMu.Unlock()
			return nil, false, fmt.Errorf("maximum number of dump sessions (%d) reached", d.maxSessions)
		}
	}
	d.sessMu.Unlock()

	buff, err := d.getSessionBuff(sessionUUID)
	if err != nil {
		return nil, false, err
	}

	// Check that session wasn't invalidated due to new dump
	d.sessMu.Lock()
	sess, ok := d.sessions[sessionUUID]
	if ok && sess.dumpVersion != d.dumpVersion {
		d.sessMu.Unlock()
		d.CloseReadSession(sessionUUID)
		return nil, false, database.ErrDumpReadSessionClosed
	}

	// Update last access time
	if ok {
		sess.lastAccess = time.Now()
		d.sessions[sessionUUID] = sess
	}
	d.sessMu.Unlock()

	if buff.Len() == 0 {
		d.CloseReadSession(sessionUUID)

		return nil, false, nil
	}

	var batch []database.DumpElem
	decoder := gob.NewDecoder(buff)
	if err := decoder.Decode(&batch); err != nil {
		d.CloseReadSession(sessionUUID)

		return nil, false, fmt.Errorf("decode batch: %w", err)
	}

	return batch, true, nil
}

func (d *Dumper) CloseReadSession(sessionUUID string) {
	d.sessMu.Lock()
	defer d.sessMu.Unlock()

	sess, ok := d.sessions[sessionUUID]
	if !ok {
		return
	}

	if sess.closed {
		return
	}

	sess.buff = nil
	sess.closed = true
	d.sessions[sessionUUID] = sess

	// Decrement active sessions counter
	if d.activeSessions > 0 {
		d.activeSessions--
	}
}

func (d *Dumper) getSessionBuff(sessionUUID string) (*bytes.Buffer, error) {
	d.sessMu.Lock()
	defer d.sessMu.Unlock()

	sess, ok := d.sessions[sessionUUID]
	if !ok {
		// Use RLock for reading file to avoid blocking other readers
		d.readDumpMu.RLock()
		currentVersion := d.dumpVersion
		dumpPath := d.currentDumpFilePath()
		d.readDumpMu.RUnlock()

		data, err := os.ReadFile(dumpPath)
		if err != nil {
			// If dump file doesn't exist, it's normal for first startup
			// Return empty buffer to indicate no dump available
			if errors.Is(err, os.ErrNotExist) {
				sess = readSession{
					buff:        bytes.NewBuffer(nil),
					dumpVersion: currentVersion,
					lastAccess:  time.Now(),
				}
				d.sessions[sessionUUID] = sess
				d.activeSessions++
				return sess.buff, nil
			}
			return nil, fmt.Errorf("failed to open dump file: %w", err)
		}

		sess = readSession{
			buff:        bytes.NewBuffer(data),
			dumpVersion: currentVersion,
			lastAccess:  time.Now(),
		}
		d.sessions[sessionUUID] = sess
		d.activeSessions++
	}

	if sess.closed {
		return nil, database.ErrDumpReadSessionClosed
	}

	return sess.buff, nil
}
