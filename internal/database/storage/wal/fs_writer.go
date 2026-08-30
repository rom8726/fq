package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

var now = time.Now

var errFSWriterClosed = errors.New("wal writer is closed")

type FSWriter struct {
	mutex sync.Mutex

	segment     *os.File
	segmentPath string
	directory   string

	segmentSize       int
	syncedSegmentSize int
	maxSegmentSize    int
	segmentMaxLSN     uint64

	segmentTimestamp int64
	segmentSequence  int
	closed           bool

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

	w.mutex.Lock()
	err := w.writeBatch(batch)
	w.mutex.Unlock()

	w.acknowledgeWrite(batch, err)
}

func (w *FSWriter) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	return w.closeSegment()
}

func (w *FSWriter) Truncate() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return errFSWriterClosed
	}

	if err := w.closeSegment(); err != nil {
		return err
	}

	return removeWALFiles(w.directory)
}

func (w *FSWriter) writeBatch(batch []Log) error {
	if w.closed {
		return errFSWriterClosed
	}

	logs := make([]*LogData, len(batch))
	for i, log := range batch {
		logs[i] = log.data
	}

	data, err := encodeLogs(logs)
	if err != nil {
		w.logger.Warn().Err(err).Msg("failed to encode logs data")

		return err
	}

	if w.segment == nil {
		if err := w.rotateSegment(); err != nil {
			return err
		}
	}

	if w.shouldRotate(len(data)) {
		if err := w.rotateSegment(); err != nil {
			return err
		}
	}

	if err := w.writeLogs(data); err != nil {
		return err
	}

	if err := w.syncSegment(); err != nil {
		w.logger.Error().Err(err).Msg("failed to sync segment file")

		return err
	}

	w.recordSegmentMaxLSN(batch)

	return nil
}

func encodeLogs(logs []*LogData) ([]byte, error) {
	logDataArray := LogDataArray{Elems: logs}
	data, err := proto.Marshal(&logDataArray)
	if err != nil {
		return nil, err
	}

	sizeData := uint32ToBytes(uint32(len(data)))

	buff := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buff)

	buff.Grow(len(data) + len(sizeData))
	buff.Write(sizeData)
	buff.Write(data)

	result := make([]byte, buff.Len())
	copy(result, buff.Bytes())

	return result, nil
}

func (w *FSWriter) shouldRotate(nextBatchSize int) bool {
	return w.maxSegmentSize > 0 &&
		w.segmentSize > 0 &&
		w.segmentSize+nextBatchSize > w.maxSegmentSize
}

func (w *FSWriter) writeLogs(data []byte) error {
	writtenBytes, err := w.segment.Write(data)
	if err != nil {
		w.logger.Warn().Err(err).Msg("failed to write logs data")

		return err
	}

	if writtenBytes != len(data) {
		return io.ErrShortWrite
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
	if err := os.MkdirAll(w.directory, 0o750); err != nil {
		return fmt.Errorf("failed to create WAL directory: %w", err)
	}

	if err := w.closeSegment(); err != nil {
		return err
	}

	segmentName := w.nextSegmentName()
	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND | os.O_EXCL

	for {
		segment, err := os.OpenFile(segmentName, flags, 0o644)
		if err == nil {
			w.segment = segment
			w.segmentPath = segmentName
			w.segmentSize = 0
			w.syncedSegmentSize = 0
			w.segmentMaxLSN = 0

			return nil
		}

		if !errors.Is(err, os.ErrExist) {
			w.logger.Error().Err(err).Msg("failed to create wal segment")

			return err
		}

		w.segmentSequence++
		segmentName = w.segmentName(w.segmentTimestamp, w.segmentSequence)
	}
}

func (w *FSWriter) closeSegment() error {
	if w.segment == nil {
		return nil
	}

	syncErr := w.syncSegment()
	if syncErr != nil {
		w.logger.Error().Err(syncErr).Msg("failed to sync WAL segment before close")
	}

	if err := w.flushSegmentMetadata(); err != nil {
		w.logger.Warn().Err(err).Str("segment_path", w.segmentPath).Msg("failed to update WAL segment metadata")
	}

	segment := w.segment
	w.segment = nil
	w.segmentPath = ""
	w.segmentSize = 0
	w.syncedSegmentSize = 0
	w.segmentMaxLSN = 0

	closeErr := segment.Close()
	if closeErr != nil {
		w.logger.Error().Err(closeErr).Msg("failed to close WAL segment")
	}

	return errors.Join(syncErr, closeErr)
}

func (w *FSWriter) syncSegment() error {
	if w.segment == nil || w.syncedSegmentSize == w.segmentSize {
		return nil
	}

	if err := w.segment.Sync(); err != nil {
		return err
	}

	w.syncedSegmentSize = w.segmentSize

	return nil
}

func (w *FSWriter) nextSegmentName() string {
	timestamp := now().UnixMilli()
	if timestamp == w.segmentTimestamp {
		w.segmentSequence++
	} else {
		w.segmentTimestamp = timestamp
		w.segmentSequence = 0
	}

	return w.segmentName(w.segmentTimestamp, w.segmentSequence)
}

func (w *FSWriter) segmentName(timestamp int64, sequence int) string {
	if sequence == 0 {
		return filepath.Join(w.directory, fmt.Sprintf("wal_%d.log", timestamp))
	}

	return filepath.Join(w.directory, fmt.Sprintf("wal_%d_%d.log", timestamp, sequence))
}

func (w *FSWriter) recordSegmentMaxLSN(batch []Log) {
	for _, log := range batch {
		if log.data != nil && log.data.LSN > w.segmentMaxLSN {
			w.segmentMaxLSN = log.data.LSN
		}
	}
}

func (w *FSWriter) flushSegmentMetadata() error {
	if w.segmentPath == "" || w.segmentMaxLSN == 0 {
		return nil
	}

	return writeSegmentMetadata(w.segmentPath, segmentMetadata{MaxLSN: w.segmentMaxLSN})
}

func removeWALFiles(directory string) error {
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("failed to create WAL directory: %w", err)
	}

	files, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("failed to scan WAL directory: %w", err)
	}

	for _, file := range files {
		name := file.Name()
		if file.IsDir() || (!isWALSegmentFile(name) &&
			!isWALSegmentMetadataFile(name) &&
			name != lastFlushDBLSNFileName) {
			continue
		}

		if err := os.Remove(filepath.Join(directory, name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove WAL file %s: %w", name, err)
		}
	}

	return syncDirectory(directory)
}

func uint32ToBytes(num uint32) []byte {
	res := make([]byte, 4)
	binary.BigEndian.PutUint32(res, num)

	return res
}

func bytesToUint32(arr []byte) uint32 {
	return binary.BigEndian.Uint32(arr)
}
