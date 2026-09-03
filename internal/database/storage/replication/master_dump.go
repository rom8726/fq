package replication

import (
	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/protocol"
)

type DumpProvider interface {
	GetNextData(sessionUUID string) ([]database.DumpElem, bool, error)
}

func (m *Master) processDump(request DumpRequest, codecs []uint8) []byte {
	response := m.synchronizeDump(request, codecs)
	responseData, err := Encode(&response)
	if err != nil {
		m.logger.Error().Err(err).Msg("failed to encode dump replication response")
	}

	return responseData
}

func (m *Master) synchronizeDump(request DumpRequest, _ []uint8) DumpResponse {
	elems, ok, err := m.dumpProvider.GetNextData(request.SessionUUID)
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("session_uuid", request.SessionUUID).
			Uint64("last_segment_number", request.LastSegmentNumber).
			Msg("error getting next dump data")

		return DumpResponse{Succeed: false, ErrorCode: protocol.CodeInternal}
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
