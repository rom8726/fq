package wal

import (
	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/tools"
)

type Log struct {
	data         *LogData
	writePromise tools.Promise[error]
	hasResult    bool
}

type Chunk struct {
	Logs    []*LogData
	Applied chan error
}

func NewLog(lsn uint64, commandID compute.CommandID, args []string) Log {
	logData := logDataPool.Get()
	logData.LSN = lsn
	logData.CommandId = uint32(commandID)
	logData.Arguments = args

	return Log{
		data:         logData,
		writePromise: tools.NewPromise[error](),
		hasResult:    true,
	}
}

func NewAsyncLog(lsn uint64, commandID compute.CommandID, args []string) Log {
	logData := logDataPool.Get()
	logData.LSN = lsn
	logData.CommandId = uint32(commandID)
	logData.Arguments = args

	return Log{data: logData}
}

func (l *Log) SetResult(err error) {
	if !l.hasResult {
		return
	}

	l.writePromise.Set(err)
}

func (l *Log) Result() tools.Future[error] {
	if !l.hasResult {
		panic("async WAL log has no result")
	}

	return l.writePromise.GetFuture()
}

func (l *Log) ReleaseLogData() {
	logDataPool.Put(l.data)
	l.data = nil
}
