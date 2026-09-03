package replication

import (
	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage/format"
	"github.com/fq-db/fq/internal/protocol"
)

type DumpProvider interface {
	GetNextData(sessionUUID string) ([]database.DumpElem, bool, error)
	GetNextRawBatch(sessionUUID string, want format.CodecID) (format.CodecID, []byte, bool, error)
}

func (m *Master) processDump(request DumpRequest, codecs []uint8) []byte {
	response := m.synchronizeDump(request, codecs)
	responseData, err := Encode(&response)
	if err != nil {
		m.logger.Error().Err(err).Msg("failed to encode dump replication response")
	}

	return responseData
}

func (m *Master) synchronizeDump(request DumpRequest, codecs []uint8) DumpResponse {
	if len(codecs) == 0 {
		return m.synchronizeDumpLegacy(request)
	}

	codec, data, ok, err := m.dumpProvider.GetNextRawBatch(request.SessionUUID, m.dumpWireCodec(codecs))
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("session_uuid", request.SessionUUID).
			Uint64("last_segment_number", request.LastSegmentNumber).
			Msg("error getting next dump batch")

		return DumpResponse{Succeed: false, ErrorCode: protocol.CodeInternal}
	}

	if !ok && len(data) == 0 {
		m.logger.Info().
			Str("session_uuid", request.SessionUUID).
			Msg("dump is empty (first startup), ending dump synchronization")

		return DumpResponse{Succeed: true, EndOfDump: true}
	}

	return DumpResponse{
		Succeed:    true,
		EndOfDump:  !ok,
		BatchCodec: uint8(codec),
		BatchData:  data,
	}
}

func (m *Master) dumpWireCodec(codecs []uint8) format.CodecID {
	if m.compression.WireCodec != format.CodecNone && SupportsCodec(codecs, m.compression.WireCodec) {
		return m.compression.WireCodec
	}

	return format.CodecNone
}

func (m *Master) synchronizeDumpLegacy(request DumpRequest) DumpResponse {
	elems, ok, err := m.dumpProvider.GetNextData(request.SessionUUID)
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("session_uuid", request.SessionUUID).
			Uint64("last_segment_number", request.LastSegmentNumber).
			Msg("error getting next dump data")

		return DumpResponse{Succeed: false, ErrorCode: protocol.CodeInternal}
	}

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
