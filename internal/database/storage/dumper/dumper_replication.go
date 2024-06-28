package dumper

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"

	"fq/internal/database"
)

type readSession struct {
	buff   *bytes.Buffer
	closed bool
}

func (d *Dumper) GetNextData(sessionUUID string) ([]database.DumpElem, bool, error) {
	buff, err := d.getSessionBuff(sessionUUID)
	if err != nil {
		return nil, false, err
	}

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
}

func (d *Dumper) getSessionBuff(sessionUUID string) (*bytes.Buffer, error) {
	d.sessMu.Lock()
	defer d.sessMu.Unlock()

	sess, ok := d.sessions[sessionUUID]
	if !ok {
		d.readDumpMu.RLock()
		defer d.readDumpMu.RUnlock()

		data, err := os.ReadFile(d.currentDumpFilePath())
		if err != nil {
			return nil, fmt.Errorf("failed to open dump file: %w", err)
		}

		sess = readSession{buff: bytes.NewBuffer(data)}
		d.sessions[sessionUUID] = sess
	}

	if sess.closed {
		return nil, database.ErrDumpReadSessionClosed
	}

	return sess.buff, nil
}
