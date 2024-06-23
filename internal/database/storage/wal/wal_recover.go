package wal

import "context"

func (w *WAL) TryRecoverWALSegments(ctx context.Context, dumpLastLSN uint64) (lastLSN uint64, err error) {
	logs, err := w.fsReader.ReadLogs(ctx)
	if err != nil {
		return 0, err
	}

	if len(logs) == 0 {
		return 0, nil
	}

	logIdx := len(logs) // end of slice
	for i := range logs {
		if logs[i].LSN > dumpLastLSN {
			logIdx = i

			break
		}
	}

	if logIdx < len(logs) {
		w.stream <- logs[logIdx:]

		return logs[len(logs)-1].LSN, nil
	}

	return dumpLastLSN, nil
}
