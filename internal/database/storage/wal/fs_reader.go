package wal

import (
	"bytes"
	"fmt"
	"os"
	"sort"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

const batchMaxSize = 100 * 1024 * 1024

type FSReader struct {
	directory string
	logger    *zerolog.Logger
}

func NewFSReader(directory string, logger *zerolog.Logger) *FSReader {
	return &FSReader{
		directory: directory,
		logger:    logger,
	}
}

func (r *FSReader) ReadLogs() ([]*LogData, error) {
	files, err := os.ReadDir(r.directory)
	if err != nil {
		return nil, fmt.Errorf("failed to scan WAL directory: %w", err)
	}

	var logs []*LogData
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := fmt.Sprintf("%s/%s", r.directory, file.Name())
		segmentedLogs, err := r.readSegment(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to recove WAL segment: %w", err)
		}

		logs = append(logs, segmentedLogs...)
	}

	sort.Slice(logs, func(i, j int) bool {
		return logs[i].LSN < logs[j].LSN
	})

	return logs, nil
}

func (r *FSReader) readSegment(filename string) ([]*LogData, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var logs []*LogData
	buffer := bytes.NewBuffer(data)
	sizeBatchBytes := make([]byte, 4)

	for buffer.Len() > 0 {
		_, err = buffer.Read(sizeBatchBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read next batch size from WAL segment: %w", err)
		}

		batchSize := bytesToUint32(sizeBatchBytes)
		if batchSize > batchMaxSize {
			panic(fmt.Errorf("max batch size in WAL segment: %d", batchSize))
		}

		batchData := make([]byte, batchSize)
		_, err = buffer.Read(batchData)
		if err != nil {
			return nil, fmt.Errorf("failed to read next batch data from WAL segment: %w", err)
		}

		var batch LogDataArray
		if err := proto.Unmarshal(batchData, &batch); err != nil {
			return nil, fmt.Errorf("failed to unmarshal WAL segment: %w", err)
		}

		logs = append(logs, batch.Elems...)
	}

	return logs, nil
}
