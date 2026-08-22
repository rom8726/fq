package replication

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fq/internal/database/storage/wal"
	"fq/internal/observability"
)

const (
	walReplicationChunkSize = 4 << 20
	walBatchSizeHeaderSize  = 4
)

func (m *Master) processWAL(request WALRequest) []byte {
	response := m.synchronizeWAL(request)
	responseData, err := Encode(&response)
	if err != nil {
		m.logger.Error().Err(err).Msg("failed to encode WAL replication response")
	}

	return responseData
}

func (m *Master) synchronizeWAL(request WALRequest) WALResponse {
	if request.ReplicaID == "" {
		m.logger.Error().Msg("replica id is required")

		return WALResponse{}
	}

	cursor := ReplicaCursor{
		ReplicaID:       request.ReplicaID,
		LastSegmentName: request.LastSegmentName,
		SegmentOffset:   request.SegmentOffset,
		LastAppliedLSN:  request.LastAppliedLSN,
		UpdatedAt:       time.Now(),
	}
	if m.tracker != nil {
		m.tracker.Ack(cursor)
		observability.SetReplicationReplicaLastAppliedLSN(cursor.ReplicaID, cursor.LastAppliedLSN)
		observability.SetReplicationReplicaLastAckTimestamp(cursor.ReplicaID, cursor.UpdatedAt)
		observability.SetReplicationKnownReplicas(len(m.tracker.List()))
	}
	m.logger.Debug().
		Str("replica_id", cursor.ReplicaID).
		Str("last_segment_name", cursor.LastSegmentName).
		Int64("segment_offset", cursor.SegmentOffset).
		Uint64("last_applied_lsn", cursor.LastAppliedLSN).
		Time("updated_at", cursor.UpdatedAt).
		Msg("replica WAL ack received")

	if request.LastSegmentName != "" {
		response, ok, err := m.synchronizeWALSegment(request.LastSegmentName, request.SegmentOffset)
		if err != nil {
			m.logger.Error().Err(err).Str("segment_name", request.LastSegmentName).Msg("failed to read WAL segment chunk")

			return WALResponse{}
		}
		if ok {
			return response
		}
	}

	segmentName, err := wal.SegmentUpperBound(m.walDirectory, request.LastSegmentName)
	if err != nil {
		m.logger.Error().Err(err).Msg("failed to find WAL segment")

		return WALResponse{}
	}

	if segmentName == "" {
		m.logger.Debug().
			Str("last_segment_name", request.LastSegmentName).
			Int64("segment_offset", request.SegmentOffset).
			Msg("no new WAL segments to replicate")

		return WALResponse{Succeed: true}
	}

	response, ok, err := m.synchronizeWALSegment(segmentName, 0)
	if err != nil {
		m.logger.Error().Err(err).Str("segment_name", segmentName).Msg("failed to read WAL segment chunk")

		return WALResponse{}
	}
	if !ok {
		return WALResponse{Succeed: true}
	}

	return response
}

func (m *Master) synchronizeWALSegment(segmentName string, offset int64) (WALResponse, bool, error) {
	if !isSafeWALSegmentName(segmentName) {
		return WALResponse{}, false, fmt.Errorf("invalid wal segment name: %q", segmentName)
	}

	filename := filepath.Join(m.walDirectory, segmentName)
	data, nextOffset, err := readCompleteWALChunk(filename, offset, walReplicationChunkSize)
	if err != nil {
		if os.IsNotExist(err) {
			return WALResponse{}, false, nil
		}

		return WALResponse{}, false, err
	}
	if len(data) == 0 {
		return WALResponse{}, false, nil
	}

	m.logger.Info().
		Str("segment_name", segmentName).
		Int64("segment_offset", offset).
		Int64("next_segment_offset", nextOffset).
		Int("chunk_size", len(data)).
		Msg("sending WAL chunk to slave")

	return WALResponse{
		Succeed:           true,
		SegmentData:       data,
		SegmentName:       segmentName,
		SegmentOffset:     offset,
		NextSegmentOffset: nextOffset,
	}, true, nil
}

//nolint:gocritic // ok
func readCompleteWALChunk(filename string, offset, maxChunkSize int64) ([]byte, int64, error) {
	if offset < 0 {
		return nil, offset, fmt.Errorf("negative WAL segment offset: %d", offset)
	}
	if maxChunkSize <= 0 {
		maxChunkSize = walReplicationChunkSize
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, offset, err
	}
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		return nil, offset, err
	}
	if offset >= stat.Size() {
		return nil, offset, nil
	}

	remaining := stat.Size() - offset
	readSize := min64(maxChunkSize, remaining)
	data := make([]byte, readSize)
	if _, err := file.ReadAt(data, offset); err != nil && err != io.EOF {
		return nil, offset, err
	}

	completeSize := completeWALBatchesSize(data)
	if completeSize == 0 {
		firstBatchSize := firstWALBatchSize(data)
		if firstBatchSize <= 0 || int64(firstBatchSize) > remaining {
			return nil, offset, nil
		}

		data = make([]byte, firstBatchSize)
		if _, err := file.ReadAt(data, offset); err != nil && err != io.EOF {
			return nil, offset, err
		}

		return data, offset + int64(len(data)), nil
	}

	data = data[:completeSize]
	return data, offset + int64(len(data)), nil
}

func completeWALBatchesSize(data []byte) int {
	offset := 0
	completeSize := 0
	for offset+walBatchSizeHeaderSize <= len(data) {
		batchSize := int(binary.BigEndian.Uint32(data[offset : offset+walBatchSizeHeaderSize]))
		nextOffset := offset + walBatchSizeHeaderSize + batchSize
		if nextOffset > len(data) {
			break
		}

		completeSize = nextOffset
		offset = nextOffset
	}

	return completeSize
}

func firstWALBatchSize(data []byte) int {
	if len(data) < walBatchSizeHeaderSize {
		return 0
	}

	return walBatchSizeHeaderSize + int(binary.BigEndian.Uint32(data[:walBatchSizeHeaderSize]))
}

func isSafeWALSegmentName(segmentName string) bool {
	return segmentName != "" &&
		!strings.ContainsAny(segmentName, `/\`) &&
		segmentName == filepath.Clean(segmentName)
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}
