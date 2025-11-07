package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

var now = time.Now

type FSWriter struct {
	segment   *os.File
	directory string

	segmentSize    int
	maxSegmentSize int

	logger *zerolog.Logger
}

func NewFSWriter(directory string, maxSegmentSize int, logger *zerolog.Logger) *FSWriter {
	return &FSWriter{
		directory:      directory,
		maxSegmentSize: maxSegmentSize,
		logger:         logger,
	}
}

func (w *FSWriter) WriteBatch(batch []Log) {
	if len(batch) == 0 {
		return
	}

	if w.segment == nil {
		if err := w.rotateSegment(); err != nil {
			w.acknowledgeWrite(batch, err)

			return
		}
	}

	if w.segmentSize > w.maxSegmentSize {
		if err := w.rotateSegment(); err != nil {
			w.acknowledgeWrite(batch, err)

			return
		}
	}

	// Pre-allocate slice with exact capacity to avoid reallocations
	logs := make([]*LogData, len(batch))
	for i, log := range batch {
		logs[i] = log.data
	}

	if err := w.writeLogs(logs); err != nil {
		w.acknowledgeWrite(batch, err)

		return
	}

	err := w.segment.Sync()
	if err != nil {
		w.logger.Error().Err(err).Msg("failed to sync segment file")
	}

	w.acknowledgeWrite(batch, err)
}

func (w *FSWriter) writeLogs(logs []*LogData) error {
	logDataArray := LogDataArray{Elems: logs}
	data, err := proto.Marshal(&logDataArray)
	if err != nil {
		w.logger.Warn().Err(err).Msg("failed to encode logs data")

		return err
	}

	sizeData := uint32ToBytes(uint32(len(data)))

	buff := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buff)

	buff.Grow(len(data) + len(sizeData))
	buff.Write(sizeData)
	buff.Write(data)

	writtenBytes, err := w.segment.Write(buff.Bytes())
	if err != nil {
		w.logger.Warn().Err(err).Msg("failed to write logs data")

		return err
	}

	w.segmentSize += writtenBytes

	return nil
}

func (w *FSWriter) acknowledgeWrite(batch []Log, err error) {
	for _, log := range batch {
		log.SetResult(err)
		log.ReleaseLogData()
	}
}

func (w *FSWriter) rotateSegment() error {
	segmentName := fmt.Sprintf("%s/wal_%d.log", w.directory, now().UnixMilli())

	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	segment, err := os.OpenFile(segmentName, flags, 0o644)
	if err != nil {
		w.logger.Error().Err(err).Msg("failed to create wal segment")

		return err
	}

	w.segment = segment
	w.segmentSize = 0

	return nil
}

func uint32ToBytes(num uint32) []byte {
	res := make([]byte, 4)
	binary.BigEndian.PutUint32(res, num)

	return res
}

func bytesToUint32(arr []byte) uint32 {
	return binary.BigEndian.Uint32(arr)
}
