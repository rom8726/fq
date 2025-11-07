package replication

import (
	"os"
	"path/filepath"

	"fq/internal/database/storage/wal"
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
	// First, try to find a new segment with name greater than lastSegmentName
	segmentName, err := wal.SegmentUpperBound(m.walDirectory, request.LastSegmentName)
	if err != nil {
		m.logger.Error().Err(err).Msg("failed to find WAL segment")

		return WALResponse{}
	}

	// If no new segment found, check if the last segment has been updated
	if segmentName == "" {
		// If we have a last segment name, check if it still exists and has grown
		if request.LastSegmentName != "" {
			lastSegmentPath := filepath.Join(m.walDirectory, request.LastSegmentName)
			_, err := os.Stat(lastSegmentPath)
			if err == nil {
				// File exists, read it to check if it has new data
				data, readErr := os.ReadFile(lastSegmentPath)
				if readErr == nil {
					// Check if file has grown (has more data than before)
					// We always send the full segment, slave will filter by LSN
					m.logger.Debug().
						Str("last_segment_name", request.LastSegmentName).
						Int("segment_size", len(data)).
						Msg("sending updated segment to slave")
					return WALResponse{
						Succeed:     true,
						SegmentData: data,
						SegmentName: request.LastSegmentName,
					}
				}
			}
		}

		// No updates found
		m.logger.Debug().
			Str("last_segment_name", request.LastSegmentName).
			Msg("no new WAL segments to replicate")
		return WALResponse{Succeed: true}
	}

	// New segment found
	filename := filepath.Join(m.walDirectory, segmentName)
	data, err := os.ReadFile(filename)
	if err != nil {
		m.logger.Error().Err(err).Str("segment_name", segmentName).Msg("failed to read WAL segment")

		return WALResponse{}
	}

	m.logger.Info().
		Str("segment_name", segmentName).
		Str("last_segment_name", request.LastSegmentName).
		Int("segment_size", len(data)).
		Msg("sending WAL segment to slave")

	return WALResponse{
		Succeed:     true,
		SegmentData: data,
		SegmentName: segmentName,
	}
}
