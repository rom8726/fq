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
	segmentName, err := wal.SegmentUpperBound(m.walDirectory, request.LastSegmentName)
	if err != nil {
		m.logger.Error().Err(err).Msg("failed to find WAL segment")

		return WALResponse{}
	}

	if segmentName == "" {
		return WALResponse{Succeed: true}
	}

	filename := filepath.Join(m.walDirectory, segmentName)
	data, err := os.ReadFile(filename)
	if err != nil {
		m.logger.Error().Err(err).Msg("failed to read WAL segment")

		return WALResponse{}
	}

	return WALResponse{
		Succeed:     true,
		SegmentData: data,
		SegmentName: segmentName,
	}
}
