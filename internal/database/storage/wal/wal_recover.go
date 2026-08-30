package wal

import (
	"context"
	"os"
)

func (w *WAL) TryRecoverWALSegments(ctx context.Context, dumpLastLSN uint64) (lastLSN uint64, err error) {
	lastFlushDBLSN, err := readLastFlushDBLSN(w.directory)
	if err != nil && !os.IsNotExist(err) {
		return 0, err
	}
	recoverAfterLSN := dumpLastLSN
	if lastFlushDBLSN > recoverAfterLSN {
		recoverAfterLSN = lastFlushDBLSN
	}

	var logs []*LogData
	if reader, ok := w.fsReader.(afterLSNFSReader); ok {
		logs, err = reader.ReadLogsAfter(ctx, recoverAfterLSN)
	} else {
		logs, err = w.fsReader.ReadLogs(ctx)
	}
	if err != nil {
		return 0, err
	}

	if len(logs) == 0 {
		return 0, nil
	}

	logIdx := len(logs) // end of slice
	for i := range logs {
		if logs[i].LSN > recoverAfterLSN {
			logIdx = i

			break
		}
	}

	if logIdx < len(logs) {
		w.stream <- Chunk{Logs: logs[logIdx:]}

		return logs[len(logs)-1].LSN, nil
	}

	return recoverAfterLSN, nil
}
