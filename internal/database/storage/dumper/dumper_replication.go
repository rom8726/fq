package dumper

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage/format"
)

type readSession struct {
	data          []byte
	offset        int
	closed        bool
	formatVersion uint16
	dumpVersion   uint64
	lastAccess    time.Time
}

func (d *Dumper) GetNextData(sessionUUID string) ([]database.DumpElem, bool, error) {
	d.sessMu.Lock()
	if d.activeSessions >= d.maxSessions {
		if _, exists := d.sessions[sessionUUID]; !exists {
			d.sessMu.Unlock()

			return nil, false, fmt.Errorf("maximum number of dump sessions (%d) reached", d.maxSessions)
		}
	}
	d.sessMu.Unlock()

	sess, err := d.getSession(sessionUUID)
	if err != nil {
		return nil, false, err
	}

	d.sessMu.Lock()
	if sess.dumpVersion != d.dumpVersion {
		d.sessMu.Unlock()
		d.CloseReadSession(sessionUUID)

		return nil, false, database.ErrDumpReadSessionClosed
	}

	sess.lastAccess = time.Now()

	if sess.offset >= len(sess.data) {
		d.sessMu.Unlock()
		d.CloseReadSession(sessionUUID)

		return nil, false, nil
	}

	payload, rest, err := format.NextFrame(sess.data[sess.offset:], dumpMaxFrameSize)
	if err != nil {
		frameOffset := format.HeaderSize + sess.offset
		d.sessMu.Unlock()
		d.CloseReadSession(sessionUUID)

		return nil, false, fmt.Errorf("dump batch at offset %d: %w", frameOffset, err)
	}

	batchData := append([]byte(nil), payload...)
	sessionVersion := sess.formatVersion
	sess.offset = len(sess.data) - len(rest)
	d.sessMu.Unlock()

	decoded, err := format.DecodePayload(nil, batchData, sessionVersion, dumpMaxFrameSize)
	if err != nil {
		d.CloseReadSession(sessionUUID)

		return nil, false, fmt.Errorf("decode dump payload: %w", err)
	}

	var batch []database.DumpElem
	if err := gob.NewDecoder(bytes.NewReader(decoded)).Decode(&batch); err != nil {
		d.CloseReadSession(sessionUUID)

		return nil, false, fmt.Errorf("decode batch: %w", err)
	}

	return batch, true, nil
}

func (d *Dumper) CloseReadSession(sessionUUID string) {
	d.sessMu.Lock()
	defer d.sessMu.Unlock()

	sess, ok := d.sessions[sessionUUID]
	if !ok || sess.closed {
		return
	}

	sess.data = nil
	sess.closed = true

	if d.activeSessions > 0 {
		d.activeSessions--
	}
}

func (d *Dumper) getSession(sessionUUID string) (*readSession, error) {
	d.sessMu.Lock()
	defer d.sessMu.Unlock()

	sess, ok := d.sessions[sessionUUID]
	if ok {
		if sess.closed {
			return nil, database.ErrDumpReadSessionClosed
		}

		return sess, nil
	}

	d.readDumpMu.RLock()
	currentVersion := d.dumpVersion
	dumpPath := d.currentDumpFilePath()
	d.readDumpMu.RUnlock()

	data, err := os.ReadFile(dumpPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("failed to open dump file: %w", err)
		}

		data = nil
	}

	var frames []byte
	version := dumpFormatVersionRaw
	if len(data) > 0 {
		frames, version, err = format.ParseHeaderVersions(
			data,
			format.MagicDump,
			dumpFormatVersionRaw,
			dumpFormatVersionCompressed,
		)
		if err != nil {
			return nil, fmt.Errorf("dump %s: %w", dumpPath, err)
		}
	}

	sess = &readSession{
		data:          frames,
		formatVersion: version,
		dumpVersion:   currentVersion,
		lastAccess:    time.Now(),
	}
	d.sessions[sessionUUID] = sess
	d.activeSessions++

	return sess, nil
}
