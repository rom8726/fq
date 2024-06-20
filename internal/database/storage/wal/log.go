package wal

import (
	"fq/internal/database/compute"
	"fq/internal/tools"
)

type Log struct {
	data         *LogData
	writePromise tools.Promise[error]
}

func NewLog(lsn uint64, commandID compute.CommandID, args []string) Log {
	logData := logDataPool.Get()
	logData.LSN = lsn
	logData.CommandId = uint32(commandID)
	logData.Arguments = args

	return Log{
		data:         logData,
		writePromise: tools.NewPromise[error](),
	}
}

func (l *Log) SetResult(err error) {
	l.writePromise.Set(err)
}

func (l *Log) Result() tools.Future[error] {
	return l.writePromise.GetFuture()
}

func (l *Log) ReleaseLogData() {
	logDataPool.Put(l.data)
	l.data = nil
}
