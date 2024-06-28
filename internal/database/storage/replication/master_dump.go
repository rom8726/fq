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
		m.logger.Error().Err(err).Msg("error getting next dump data")

		return DumpResponse{Succeed: false}
	}

	return DumpResponse{
		Succeed:     true,
		EndOfDump:   !ok,
		SegmentData: elems,
	}
}
