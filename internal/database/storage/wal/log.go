package wal

import (
	"fq/internal/database/compute"
	"fq/internal/tools"
)

// type LogData struct {
//	LSN       int64
//	CommandID compute.CommandID
//	Arguments []string
//}

type Log struct {
	data         *LogData
	writePromise tools.Promise[error]
}

func NewLog(lsn uint64, commandID compute.CommandID, args []string) Log {
	return Log{
		data: &LogData{
			LSN:       lsn,
			CommandId: uint32(commandID),
			Arguments: args,
		},
		writePromise: tools.NewPromise[error](),
	}
}

func (l *Log) SetResult(err error) {
	l.writePromise.Set(err)
}

func (l *Log) Result() tools.Future[error] {
	return l.writePromise.GetFuture()
}
