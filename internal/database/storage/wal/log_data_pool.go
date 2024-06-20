package wal

import "sync"

var logDataPool = newLogDataPool()

type LogDataPool struct {
	pool sync.Pool
}

func newLogDataPool() *LogDataPool {
	return &LogDataPool{pool: sync.Pool{New: func() interface{} {
		return &LogData{}
	}}}
}

func (p *LogDataPool) Get() *LogData {
	return p.pool.Get().(*LogData)
}

func (p *LogDataPool) Put(logData *LogData) {
	logData.Reset()
	p.pool.Put(logData)
}
