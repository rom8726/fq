package replication

import "fq/internal/database"

type DumpProvider interface {
	GetNextData(sessionUUID string) ([]database.DumpElem, bool, error)
}

func (m *Master) processDump(request DumpRequest) []byte {
	response := m.synchronizeDump(request)
	responseData, err := Encode(&response)
	if err != nil {
		m.logger.Error().Err(err).Msg("failed to encode dump replication response")
	}

	return responseData
}

func (m *Master) synchronizeDump(request DumpRequest) DumpResponse {
	elems, ok, err := m.dumpProvider.GetNextData(request.SessionUUID)
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("session_uuid", request.SessionUUID).
			Uint64("last_segment_number", request.LastSegmentNumber).
			Msg("error getting next dump data")

		return DumpResponse{Succeed: false}
	}

	// If no more data and no elements, it means dump is empty (first startup)
	if !ok && len(elems) == 0 {
		m.logger.Info().
			Str("session_uuid", request.SessionUUID).
			Msg("dump is empty (first startup), ending dump synchronization")
		return DumpResponse{
			Succeed:     true,
			EndOfDump:   true,
			SegmentData: nil,
		}
	}

	return DumpResponse{
		Succeed:     true,
		EndOfDump:   !ok,
		SegmentData: elems,
	}
}
