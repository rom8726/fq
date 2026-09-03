package replication

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fq-db/fq/internal/database/storage/format"
	"github.com/fq-db/fq/internal/database/storage/wal"
	"github.com/fq-db/fq/internal/observability"
	"github.com/fq-db/fq/internal/protocol"
)

const walReplicationChunkSize = 4 << 20

func (m *Master) processWAL(request WALRequest, codecs []uint8) []byte {
	response := m.synchronizeWAL(request, codecs)
	responseData, err := Encode(&response)
	if err != nil {
		m.logger.Error().Err(err).Msg("failed to encode WAL replication response")
	}

	return responseData
}

func (m *Master) synchronizeWAL(request WALRequest, codecs []uint8) WALResponse {
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
		response, ok, err := m.synchronizeWALSegment(request.LastSegmentName, request.SegmentOffset, codecs)
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

	response, ok, err := m.synchronizeWALSegment(segmentName, 0, codecs)
	if err != nil {
		m.logger.Error().Err(err).Str("segment_name", segmentName).Msg("failed to read WAL segment chunk")

		return WALResponse{}
	}
	if !ok {
		return WALResponse{Succeed: true}
	}

	return response
}

func (m *Master) synchronizeWALSegment(
	segmentName string,
	offset int64,
	codecs []uint8,
) (WALResponse, bool, error) {
	if !isSafeWALSegmentName(segmentName) {
		return WALResponse{}, false, fmt.Errorf("invalid wal segment name: %q", segmentName)
	}

	filename := filepath.Join(m.walDirectory, segmentName)

	version, err := wal.SegmentFormatVersion(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return WALResponse{}, false, nil
		}

		return WALResponse{}, false, err
	}

	if version > 1 && !SupportsCodec(codecs, m.compression.SegmentCodec) {
		m.logger.Warn().
			Str("segment_name", segmentName).
			Str("codec", m.compression.SegmentCodec.String()).
			Msg("replica does not support the segment compression codec")

		return WALResponse{Succeed: false, ErrorCode: protocol.CodeUnsupportedCompression}, true, nil
	}

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

	chunkCodec := format.CodecNone
	if version == 1 && m.compression.WireCodec != format.CodecNone &&
		SupportsCodec(codecs, m.compression.WireCodec) {
		encoded := format.EncodePayload(nil, data, format.Compression{
			Codec:        m.compression.WireCodec,
			MinFrameSize: m.compression.MinFrameSize,
		})

		if format.PayloadCodec(encoded) != format.CodecNone {
			data = encoded
			chunkCodec = m.compression.WireCodec
		}
	}

	m.logger.Info().
		Str("segment_name", segmentName).
		Int64("segment_offset", offset).
		Int64("next_segment_offset", nextOffset).
		Int("chunk_size", len(data)).
		Uint16("segment_format_version", version).
		Str("chunk_codec", chunkCodec.String()).
		Msg("sending WAL chunk to slave")

	return WALResponse{
		Succeed:              true,
		SegmentData:          data,
		SegmentName:          segmentName,
		SegmentOffset:        offset,
		NextSegmentOffset:    nextOffset,
		SegmentFormatVersion: version,
		SegmentCodec:         uint8(chunkCodec),
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

	hasHeader := offset == 0

	completeSize := format.CompleteFramesSize(data, hasHeader, wal.MaxBatchSize)
	if completeSize == 0 {
		firstFrameSize := format.FirstFrameSize(data, hasHeader, wal.MaxBatchSize)
		if firstFrameSize <= 0 || int64(firstFrameSize) > remaining {
			return nil, offset, nil
		}

		data = make([]byte, firstFrameSize)
		if _, err := file.ReadAt(data, offset); err != nil && err != io.EOF {
			return nil, offset, err
		}

		return data, offset + int64(len(data)), nil
	}

	data = data[:completeSize]

	return data, offset + int64(len(data)), nil
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
